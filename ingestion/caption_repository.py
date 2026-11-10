"""Caption scheduling policy over the existing durable stages and outbox."""
from .streams import stream_name
from dataclasses import replace
from datetime import timedelta
import json
import random
from uuid import UUID,uuid4,uuid5,NAMESPACE_URL

from .contracts import Stage,StageEnvelope,OutboxEvent
from .postgres import PostgresManifestRepository,LeaseLostError
from .pinned_connection import PinnedPool
from .deferred_caption import verified_file,canonical,digest


def _json(value):
    return json.loads(value) if isinstance(value,str) else dict(value or {})


class PermanentImageError(ValueError):pass


def processing_identity(envelope):
    """Immutable computation identity; stream retry/message fields are transport."""
    value=envelope.to_dict() if hasattr(envelope,'to_dict') else _json(envelope)
    keys=('schema_version','job_id','document_id','stage','pipeline_version','config_digest','model_digest','idempotency_key','artifact_uri')
    if any(not isinstance(value.get(key),str) or not value[key] for key in keys):
        raise PermanentImageError('Caption persisted processing identity is incomplete')
    return {key:value[key] for key in keys}


def retryable(error):
    if isinstance(error,PermanentImageError):return False
    current=error
    while current is not None:
        response=getattr(current,'response',None)
        code=getattr(current,'status_code',None) or getattr(current,'status',None) or getattr(response,'status_code',None)
        if isinstance(code,int):return code in (408,429) or code>=500
        current=current.__cause__ or current.__context__
    # Native HTTP wrappers can expose only a safe status string.
    import re
    match=re.search(r'\b(4\d\d|5\d\d)\b',str(error))
    if match:return int(match.group(1)) in (408,429) or int(match.group(1))>=500
    return True


def backoff(attempt,base=1.0,cap=60.0,jitter=.2):
    return min(cap,base*(2**max(0,attempt-1))*random.uniform(1-jitter,1+jitter))


class CaptionManifestRepository:
    def __init__(self,legacy,root,*,base_seconds=1.0,cap_seconds=60.0):
        self.legacy=legacy;self.pool=legacy.pool;self.root=root
        self.base_seconds=base_seconds;self.cap_seconds=cap_seconds
    def __getattr__(self,name):return getattr(self.legacy,name)

    async def commit_stage(self,envelope,*,lease_token,artifact,downstream):
        checkpoint_bytes=verified_file(self.root,artifact.uri,artifact.digest.removeprefix('sha256:'))
        if len(checkpoint_bytes)!=artifact.size_bytes:raise ValueError('Caption handoff artifact size differs')
        checkpoint=json.loads(checkpoint_bytes)
        jobs=checkpoint.get('caption_jobs',[])
        async with self.pool.acquire() as c,c.transaction():
            if envelope.stage==Stage.CAPTION:
                await self._require_current(c,envelope,lease_token)
            existing=await c.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1 FOR UPDATE',envelope.idempotency_key)
            if existing and (str(existing['job_id'])!=envelope.job_id or existing['document_id']!=envelope.document_id or existing['stage']!=envelope.stage.value):raise ValueError('Parent stage scope differs from handoff')
            if existing and existing['input_envelope'] is not None and processing_identity(existing['input_envelope'])!=processing_identity(envelope):raise ValueError('Parent persisted input differs from handoff')
            if existing and existing['status']=='committed' and (existing['artifact_digest']!=artifact.digest or existing['artifact_uri']!=artifact.uri or existing['artifact_size_bytes']!=artifact.size_bytes):
                raise ValueError('Committed stage cannot enqueue a different artifact')
            legacy=PostgresManifestRepository(PinnedPool(c))
            if not existing or existing['status']!='committed':
                await legacy.commit_stage(envelope,lease_token=lease_token,artifact=artifact,downstream=downstream)
            for job in jobs:
                child=StageEnvelope.from_stream_fields(job['envelope'])
                if child.stage!=Stage.CAPTION or child.job_id!=envelope.job_id or child.document_id!=envelope.document_id or child.pipeline_version!=envelope.pipeline_version or job.get('stage_key',child.idempotency_key)!=child.idempotency_key or child.artifact_uri!=job['manifest_uri'] or not any(stream==stream_name('caption.pending') and e==child for stream,e in downstream):
                    raise ValueError('Caption association does not match stage handoff')
                parent=await c.fetchrow('SELECT d.source_digest,d.source_uri,j.corpus_namespace,j.metadata AS job_metadata FROM public.documents d JOIN public.jobs j ON j.job_id=d.job_id WHERE d.document_id=$1 AND d.job_id=$2',envelope.document_id,UUID(envelope.job_id))
                if parent is None:raise ValueError('Caption source parent missing')
                if _json(parent['job_metadata']).get('ingestion_options',{}).get('captions_enabled') is not True:
                    raise ValueError('Caption jobs require explicit user opt-in')
                raw=verified_file(self.root,job['manifest_uri'],job['manifest_sha256'])
                manifest=json.loads(raw)
                if envelope.stage!=Stage.EXTRACTION or manifest['parent_stage_key']!=envelope.idempotency_key or manifest['document_id']!=envelope.document_id or manifest['parent_job_id']!=envelope.job_id or manifest['pipeline_version']!=envelope.pipeline_version or manifest['parent_config_digest']!=envelope.config_digest or manifest['parent_model_digest']!=envelope.model_digest or manifest['model_digest']!=child.model_digest or manifest['asset_sha256']!=job['asset_sha256'] or child.config_digest!='sha256:'+digest(canonical(manifest['caption_config'])):
                    raise ValueError('Caption retained manifest processing identity differs')
                verified_file(self.root,manifest['asset_uri'],job['asset_sha256'])
                await c.execute('''INSERT INTO caption_control.assets(stage_key,parent_job_id,document_id,source_digest,source_uri,collection_id,manifest_uri,manifest_sha256,asset_sha256,source_metadata)
                    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb) ON CONFLICT(stage_key) DO NOTHING''',child.idempotency_key,UUID(child.job_id),child.document_id,parent['source_digest'],parent['source_uri'],parent['corpus_namespace'],job['manifest_uri'],job['manifest_sha256'],job['asset_sha256'],json.dumps({'acl':_json(parent['job_metadata']).get('acl',[]),'ingestion_run_id':_json(parent['job_metadata']).get('ingestion_run_id')}))
                prior=await c.fetchrow('SELECT * FROM caption_control.assets WHERE stage_key=$1',child.idempotency_key)
                expected={'parent_job_id':UUID(child.job_id),'document_id':child.document_id,'source_digest':parent['source_digest'],'source_uri':parent['source_uri'],'collection_id':parent['corpus_namespace'],'manifest_uri':job['manifest_uri'],'manifest_sha256':job['manifest_sha256'],'asset_sha256':job['asset_sha256']}
                scope={'acl':_json(parent['job_metadata']).get('acl',[]),'ingestion_run_id':_json(parent['job_metadata']).get('ingestion_run_id')}
                if any(prior[key]!=value for key,value in expected.items()) or _json(prior['source_metadata'])!=scope:raise ValueError('Caption replay asset/source identity differs')
                # Register before the outbox transaction becomes visible. Legacy
                # acquire_lease intentionally does not persist input envelopes.
                await c.execute("INSERT INTO public.stage_units(idempotency_key,job_id,document_id,stage,status,input_envelope) VALUES($1,$2,$3,'caption','pending',$4::jsonb) ON CONFLICT DO NOTHING",child.idempotency_key,UUID(child.job_id),child.document_id,json.dumps(child.to_dict()))
                persisted=await c.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1 FOR UPDATE',child.idempotency_key)
                if persisted['job_id']!=UUID(child.job_id) or persisted['document_id']!=child.document_id or persisted['stage']!='caption':raise ValueError('Caption child stage scope differs')
                if persisted['input_envelope'] is None:
                    # Matching committed-parent replay is the only recovery
                    # source; do not reset leases, artifacts or attempt history.
                    await c.execute('UPDATE public.stage_units SET input_envelope=$2::jsonb WHERE idempotency_key=$1 AND input_envelope IS NULL',child.idempotency_key,json.dumps(child.to_dict()))
                elif processing_identity(persisted['input_envelope'])!=processing_identity(child):raise ValueError('Caption child persisted computation differs')

    async def _require_current(self,c,envelope,lease_token=None):
        row=await c.fetchrow('SELECT * FROM caption_control.assets WHERE stage_key=$1 FOR UPDATE',envelope.idempotency_key)
        parent=await c.fetchrow('SELECT d.source_digest,d.source_uri,d.status AS document_status,j.status AS job_status,j.corpus_namespace,j.metadata FROM public.documents d JOIN public.jobs j ON j.job_id=d.job_id WHERE d.document_id=$1 AND d.job_id=$2 FOR SHARE OF d,j',envelope.document_id,UUID(envelope.job_id))
        if not row or not parent or _json(parent['metadata']).get('ingestion_options',{}).get('captions_enabled') is not True or parent['source_digest']!=row['source_digest'] or parent['source_uri']!=row['source_uri'] or parent['corpus_namespace']!=row['collection_id'] or _json(parent['metadata']).get('acl',[])!=_json(row['source_metadata']).get('acl',[]) or parent['job_status'] in ('cancelled','canceled','deleted','failed','superseded') or parent['document_status'] in ('cancelled','canceled','deleted','failed','superseded'):
            raise PermanentImageError('Caption source revision or scope is no longer current')
        stage=await c.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1 FOR UPDATE',envelope.idempotency_key)
        if not stage or stage['job_id']!=UUID(envelope.job_id) or stage['document_id']!=envelope.document_id or stage['stage']!='caption' or stage['input_envelope'] is None or processing_identity(stage['input_envelope'])!=processing_identity(envelope):
            raise PermanentImageError('Caption notification differs from persisted processing identity')
        if lease_token is not None:
            active=await c.fetchval('SELECT lease_token=$2::uuid AND lease_expires_at>clock_timestamp() FROM public.stage_units WHERE idempotency_key=$1 FOR UPDATE',envelope.idempotency_key,lease_token)
            if not active:raise LeaseLostError('Caption lease expired before commit')
        return row

    async def source_context(self,envelope):
        async with self.pool.acquire() as c,c.transaction():
            row=await self._require_current(c,envelope)
            return {'source_uri':row['source_uri'],'source_digest':row['source_digest'],'collection_id':row['collection_id'],**_json(row['source_metadata'])}

    async def acquire_lease(self,envelope,*,owner,ttl):
        if envelope.stage!=Stage.CAPTION:return await self.legacy.acquire_lease(envelope,owner=owner,ttl=ttl)
        async with self.pool.acquire() as c,c.transaction():
            row=await self._require_current(c,envelope)
            stage=await c.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1 FOR UPDATE',envelope.idempotency_key)
            if row['terminal_reason'] or row['next_attempt_at']>await c.fetchval('SELECT clock_timestamp()') or (stage and (stage['attempt_count']>=row['max_attempts'] or stage['status'] in ('deadlettered','committed'))):return None
            return await PostgresManifestRepository(PinnedPool(c)).acquire_lease(envelope,owner=owner,ttl=ttl)

    async def record_failure(self,envelope,*,lease_token,error):
        if envelope.stage!=Stage.CAPTION:return await self.legacy.record_failure(envelope,lease_token=lease_token,error=error)
        async with self.pool.acquire() as c,c.transaction():
            await c.fetchrow('SELECT stage_key FROM caption_control.assets WHERE stage_key=$1 FOR UPDATE',envelope.idempotency_key)
            count=await PostgresManifestRepository(PinnedPool(c)).record_failure(envelope,lease_token=lease_token,error=error)
            if not count:raise LeaseLostError('Stale caption worker cannot schedule a retry')
            terminal=count>=4 or not retryable(error)
            delay=backoff(count,self.base_seconds,self.cap_seconds)
            await c.execute('UPDATE caption_control.assets SET next_attempt_at=clock_timestamp()+$2::interval,terminal_reason=$3,updated_at=clock_timestamp() WHERE stage_key=$1',envelope.idempotency_key,timedelta(seconds=delay),type(error).__name__ if terminal else None)
            if not terminal:
                child=replace(envelope,message_id=str(uuid5(NAMESPACE_URL,envelope.idempotency_key+':retry:'+str(count))))
                await c.execute('''INSERT INTO public.outbox_events(event_id,idempotency_key,target_stream,envelope) VALUES($1,$2,$3,$4::jsonb)
                    ON CONFLICT(idempotency_key,target_stream,(envelope->>'message_id')) DO NOTHING''',uuid4(),envelope.idempotency_key,stream_name('caption.pending'),json.dumps(child.to_dict()))
            return count

    async def fetch_unpublished_outbox(self,*,limit):
        rows=await self.pool.fetch('''SELECT o.event_id::text,o.target_stream,o.envelope FROM public.outbox_events o
            LEFT JOIN caption_control.assets a ON a.stage_key=(o.envelope->>'idempotency_key')
            WHERE o.published_at IS NULL AND o.target_stream=ANY($2::text[])
              AND (o.target_stream<>$3 OR (a.terminal_reason IS NULL AND a.next_attempt_at<=clock_timestamp()))
            ORDER BY o.created_at LIMIT $1''',limit,[stream_name('caption.pending'),stream_name('caption.deadletter'),stream_name('caption.results')],stream_name('caption.pending'))
        return [OutboxEvent(event_id=r['event_id'],stream=r['target_stream'],envelope=StageEnvelope.from_stream_fields(json.loads(r['envelope']) if isinstance(r['envelope'],str) else r['envelope'])) for r in rows]

    async def _dispose(self,message,error=None,target_stream=stream_name('caption.deadletter')):
        envelope=message.envelope
        async with self.pool.acquire() as c,c.transaction():
            policy=await c.fetchrow('SELECT * FROM caption_control.assets WHERE stage_key=$1 FOR UPDATE',envelope.idempotency_key)
            stage=await c.fetchrow('SELECT *,lease_expires_at>clock_timestamp() AS leased FROM public.stage_units WHERE idempotency_key=$1 FOR UPDATE',envelope.idempotency_key)
            if stage and (stage['status']=='committed' or stage['leased']):return False
            exhausted=stage and policy and stage['attempt_count']>=policy['max_attempts']
            reason=policy['terminal_reason'] if policy else None
            if error is None and not reason and not exhausted and not (stage and stage['status']=='deadlettered'):return False
            error=error or PermanentImageError(reason or 'Caption retry cap reached')
            await PostgresManifestRepository(PinnedPool(c)).deadletter(message,error=error,target_stream=target_stream)
            if policy:
                await c.execute('UPDATE caption_control.assets SET terminal_reason=COALESCE(terminal_reason,$2),updated_at=clock_timestamp() WHERE stage_key=$1',envelope.idempotency_key,type(error).__name__)
            return True

    async def terminal(self,message):return await self._dispose(message)

    async def deadletter(self,message,*,error,target_stream):
        if message.envelope.stage!=Stage.CAPTION:
            return await self.legacy.deadletter(message,error=error,target_stream=target_stream)
        return await self._dispose(message,error,target_stream)
