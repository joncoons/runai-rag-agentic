"""PostgreSQL publication authority. All collection locks use the same order.

Callers are trusted infrastructure services, never agent-facing DB tools.
VDB effects are outside these transactions; attempt handles isolate late writes.
"""
import json
import re
from uuid import UUID, uuid4


class Conflict(RuntimeError):
    """Intent or publication cannot satisfy the current collection state."""


class Fenced(Conflict):
    """The attempt no longer owns a live mutation lease."""


def digest(value):
    if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{64}', value):
        raise Conflict('A full lowercase SHA-256 is required')
    return value


def decoded(value):
    return json.loads(value) if isinstance(value, str) else value


class ControlRepository:
    def __init__(self, pool, *, handle_prefix="rag", incremental=False):
        if handle_prefix not in ("rag", "rag_agentic", "rag_analysis"):raise Conflict("Invalid physical generation prefix")
        self.pool = pool
        self.handle_prefix = handle_prefix
        self.incremental = incremental

    async def create_collection(self, collection_id, contract_sha256):
        digest(contract_sha256)
        if not collection_id or len(collection_id)>255:
            raise Conflict('Invalid logical collection identity')
        async with self.pool.acquire() as c, c.transaction():
            await c.execute('INSERT INTO collection_control.collections(collection_id,contract_sha256) VALUES($1,$2) ON CONFLICT DO NOTHING',collection_id,contract_sha256)
            row=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',collection_id)
            if row['deleted'] or row['contract_sha256']!=contract_sha256:
                raise Conflict('Existing collection has another contract or is deleted')

    async def prepare(self, collection_id, key, kind, base_revision, artifact_sha256, contract_sha256):
        digest(artifact_sha256);digest(contract_sha256)
        if kind not in ('snapshot','enroll_milvus','remove_milvus') or not key or len(key)>255:
            raise Conflict('Invalid mutation kind or idempotency key')
        async with self.pool.acquire() as c, c.transaction():
            col=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',collection_id)
            if col is None or col['deleted'] or col['contract_sha256']!=contract_sha256:
                raise Conflict('Collection unavailable or embedding contract differs')
            old=await c.fetchrow('SELECT * FROM collection_control.operations WHERE collection_id=$1 AND idempotency_key=$2',collection_id,key)
            if old:
                if (old['kind'],old['base_revision'],old['artifact_sha256'])!=(kind,base_revision,artifact_sha256):
                    raise Conflict('Idempotency key reused for another immutable intent')
                return dict(old)
            if col['revision']!=base_revision:
                raise Conflict('Stale base revision')
            if await c.fetchval("SELECT EXISTS(SELECT 1 FROM collection_control.operations WHERE collection_id=$1 AND status='pending')",collection_id):
                raise Conflict('Another collection mutation is pending')
            if kind!='snapshot' and col['current_operation']:
                previous_sha=await c.fetchval('SELECT artifact_sha256 FROM collection_control.operations WHERE operation_id=$1',col['current_operation'])
                if previous_sha!=artifact_sha256:raise Conflict('Membership changes must preserve the published artifact')
            if kind=='enroll_milvus' and col['milvus_enabled']:
                raise Conflict('Milvus already enrolled')
            if kind=='remove_milvus' and not col['milvus_enabled']:
                raise Conflict('Milvus is not enrolled')
            targets=['elasticsearch']
            if (col['milvus_enabled'] and kind!='remove_milvus') or kind=='enroll_milvus':targets.append('milvus')
            op=uuid4()
            row=await c.fetchrow('INSERT INTO collection_control.operations(operation_id,collection_id,idempotency_key,kind,base_revision,base_epoch,artifact_sha256,targets) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *',op,collection_id,key,kind,base_revision,col['target_epoch'],artifact_sha256,targets)
            await self._event(c,collection_id,op,'generation_requested',{'artifact_sha256':artifact_sha256,'targets':targets})
            return dict(row)

    async def _event(self,c,collection_id,op,kind,payload):
        await c.execute('INSERT INTO collection_control.outbox(event_id,collection_id,operation_id,event_type,payload) VALUES($1,$2,$3,$4,$5::jsonb)',uuid4(),collection_id,op,kind,json.dumps(payload))

    async def _locked(self,c,operation_id):
        identity=await c.fetchval('SELECT collection_id FROM collection_control.operations WHERE operation_id=$1',UUID(str(operation_id)))
        if identity is None:raise Conflict('Unknown mutation')
        col=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',identity)
        op=await c.fetchrow('SELECT *,lease_until>clock_timestamp() AS live FROM collection_control.operations WHERE operation_id=$1 FOR UPDATE',UUID(str(operation_id)))
        return col,op

    def _owned(self,col,op,lease):
        if col['deleted'] or op['status']!='pending' or not op['live'] or str(op['lease_token'])!=str(lease['lease_token']) or op['fence']!=lease['fence']:
            raise Fenced('Mutation lease expired, superseded, deleted or already published')
        if col['revision']!=op['base_revision'] or col['target_epoch']!=op['base_epoch']:
            raise Fenced('Collection revision or membership changed')

    async def claim(self, operation_id, *, ttl_seconds):
        if not 1<=ttl_seconds<=3600:raise Conflict('Lease TTL must be 1..3600 seconds')
        async with self.pool.acquire() as c, c.transaction():
            col,op=await self._locked(c,operation_id)
            if col['deleted'] or op['status']!='pending' or op['live']:return None
            fence=op['fence']+1
            stem=self.handle_prefix+'_'+op['operation_id'].hex+'_'+str(fence)
            handles={t:stem+('_es' if t=='elasticsearch' else '_mv') for t in op['targets']}
            if self.incremental or op.get('storage_mode')=='record_versions':
                from .incremental import version_handle
                handles={t:version_handle(op['collection_id'],t) for t in op['targets']}
                await c.execute("UPDATE collection_control.operations SET storage_mode='record_versions' WHERE operation_id=$1",op['operation_id'])
            row=await c.fetchrow("UPDATE collection_control.operations SET fence=$2,lease_token=$3,lease_until=clock_timestamp()+$4*interval '1 second',handles=$5::jsonb WHERE operation_id=$1 RETURNING *",op['operation_id'],fence,uuid4(),ttl_seconds,json.dumps(handles))
            await c.execute('INSERT INTO collection_control.attempts(operation_id,fence,lease_token,handles) VALUES($1,$2,$3,$4::jsonb)',op['operation_id'],fence,row['lease_token'],json.dumps(handles))
            result=dict(row);result['handles']=handles;result['contract_sha256']=col['contract_sha256']
            return result

    async def receipt(self,lease,backend,content_sha256,row_count):
        digest(content_sha256)
        if not isinstance(row_count,int) or isinstance(row_count,bool) or row_count<0:raise Conflict('Invalid visible row count')
        async with self.pool.acquire() as c, c.transaction():
            col,op=await self._locked(c,lease['operation_id']);self._owned(col,op,lease)
            if backend not in op['targets']:raise Conflict('Backend is not required by this intent')
            handle=decoded(op['handles'])[backend]
            old=await c.fetchrow('SELECT * FROM collection_control.receipts WHERE operation_id=$1 AND fence=$2 AND backend=$3',op['operation_id'],op['fence'],backend)
            if old and (old['content_sha256'],old['row_count'],old['handle'])!=(content_sha256,row_count,handle):raise Conflict('Conflicting replay receipt')
            await c.execute('INSERT INTO collection_control.receipts(operation_id,fence,backend,content_sha256,row_count,handle) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING',op['operation_id'],op['fence'],backend,content_sha256,row_count,handle)

    async def publish(self,lease):
        async with self.pool.acquire() as c, c.transaction():
            col,op=await self._locked(c,lease['operation_id'])
            # A committed, matching replay has no further effects; deletion still wins.
            if not col['deleted'] and op['status']=='published' and str(op['lease_token'])==str(lease['lease_token']):return
            self._owned(col,op,lease)
            rows=await c.fetch('SELECT * FROM collection_control.receipts WHERE operation_id=$1 AND fence=$2',op['operation_id'],op['fence'])
            if {r['backend'] for r in rows}!=set(op['targets']) or len({(r['content_sha256'],r['row_count']) for r in rows})!=1:
                raise Conflict('All required stores must verify the same search-visible generation')
            await c.execute("UPDATE collection_control.operations SET status='published',published_at=clock_timestamp() WHERE operation_id=$1",op['operation_id'])
            enabled='milvus' in op['targets']
            await c.execute('UPDATE collection_control.collections SET revision=revision+1,target_epoch=target_epoch+CASE WHEN milvus_enabled<>$3 THEN 1 ELSE 0 END,milvus_enabled=$3,current_operation=$2 WHERE collection_id=$1',op['collection_id'],op['operation_id'],enabled)
            await self._event(c,op['collection_id'],op['operation_id'],'generation_published',{'handles':decoded(op['handles']),'revision':op['base_revision']+1})

    async def resolve(self,collection_id):
        async with self.pool.acquire() as c, c.transaction():
            col=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR SHARE',collection_id)
            if not col or col['deleted'] or not col['current_operation']:raise Conflict('No authorized publication')
            op=await c.fetchrow("SELECT * FROM collection_control.operations WHERE operation_id=$1 AND status='published'",col['current_operation'])
            if not op:raise Conflict('Publication invariant failed')
            return {'collection_id':collection_id,'revision':col['revision'],'target_epoch':col['target_epoch'],'operation_id':op['operation_id'],'handles':decoded(op['handles']),'contract_sha256':col['contract_sha256'],**({'storage_mode':'record_versions'} if op.get('storage_mode')=='record_versions' else {})}

    async def revalidate(self,manifest):
        if await self.resolve(manifest['collection_id'])!=manifest:raise Fenced('Publication changed before evidence release')

    async def delete_collection(self,collection_id):
        async with self.pool.acquire() as c, c.transaction():
            col=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',collection_id)
            if not col:raise Conflict('Unknown collection')
            if col['deleted']:return
            await c.execute('UPDATE collection_control.collections SET deleted=true,target_epoch=target_epoch+1 WHERE collection_id=$1',collection_id)
            await c.execute("UPDATE collection_control.operations SET status='cancelled',lease_until=clock_timestamp() WHERE collection_id=$1 AND status='pending'",collection_id)
            # Preserve every old/abandoned physical handle for subsequent fenced cleanup.
            handles=await c.fetch('SELECT a.operation_id,a.fence,a.handles FROM collection_control.attempts a JOIN collection_control.operations o USING(operation_id) WHERE o.collection_id=$1',collection_id)
            await self._event(c,collection_id,None,'collection_deleted',{'retained_generations':[{'operation_id':str(r['operation_id']),'fence':r['fence'],'handles':decoded(r['handles'])} for r in handles],'physical_purge_complete':False})

    async def renew(self,lease,*,ttl_seconds):
        if not 1<=ttl_seconds<=3600:raise Conflict('Lease TTL must be 1..3600 seconds')
        async with self.pool.acquire() as c,c.transaction():
            col,op=await self._locked(c,lease['operation_id'])
            if not col['deleted'] and op['status']=='published' and str(op['lease_token'])==str(lease['lease_token']):return False
            self._owned(col,op,lease)
            await c.execute("UPDATE collection_control.operations SET lease_until=clock_timestamp()+$2*interval '1 second' WHERE operation_id=$1",op['operation_id'],ttl_seconds)
            return True

    async def cancel(self,lease,reason):
        if reason not in ('artifact_admission_rejected','operator_cancelled','backend_contract_rejected'):raise Conflict('Unsupported cancellation reason')
        async with self.pool.acquire() as c,c.transaction():
            col,op=await self._locked(c,lease['operation_id']);self._owned(col,op,lease)
            await c.execute("UPDATE collection_control.operations SET status='cancelled',lease_until=clock_timestamp() WHERE operation_id=$1",op['operation_id'])
            await self._event(c,op['collection_id'],op['operation_id'],'generation_cancelled',{'reason':reason,'handles':decoded(op['handles']),'fence':op['fence']})

    async def release_retry(self,lease):
        """Expire only this owned attempt; the next retry gets fresh handles.

        A native backend thread may finish late after cancellation. Its physical
        generation remains isolated by the old fence and cannot be published.
        """
        async with self.pool.acquire() as c,c.transaction():
            col,op=await self._locked(c,lease['operation_id'])
            if op['status']!='pending':return
            self._owned(col,op,lease)
            await c.execute("UPDATE collection_control.operations SET lease_until=clock_timestamp() WHERE operation_id=$1",op['operation_id'])
