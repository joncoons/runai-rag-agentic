"""PostgreSQL proof gate for retained-source generation publication."""
import hashlib
import asyncio

from .artifacts import canonical,content_digest,record_digest
from .pinned_connection import PinnedPool
from .provenance import full_hash,envelope_digest,verified_file
from .repository import ControlRepository,Conflict,decoded


class ProvenanceRepository(ControlRepository):
    async def prepare(self,collection_id,key,kind,base_revision,artifact_sha256,contract_sha256):
        async with self.pool.acquire() as c,c.transaction():
            col=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',collection_id)
            op=await ControlRepository(PinnedPool(c)).prepare(collection_id,key,kind,base_revision,artifact_sha256,contract_sha256)
            if col and kind!='snapshot' and col['current_operation']:
                parent=await c.fetchrow('SELECT * FROM collection_control.generation_admissions WHERE operation_id=$1',col['current_operation'])
                if parent and parent['artifact_sha256']==artifact_sha256:
                    await c.execute("INSERT INTO collection_control.generation_admissions(operation_id,artifact_sha256,content_sha256,row_count,inherited_from,policy_version) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING",op['operation_id'],artifact_sha256,parent['content_sha256'],parent['row_count'],col['current_operation'],parent['policy_version'])
                    await c.execute('INSERT INTO collection_control.record_lineage(operation_id,record_id,record_sha256,stage_key) SELECT $1,record_id,record_sha256,stage_key FROM collection_control.record_lineage WHERE operation_id=$2 ON CONFLICT DO NOTHING',op['operation_id'],col['current_operation'])
                    if await c.fetchval("SELECT to_regclass('collection_control.caption_record_lineage')"):
                        await c.execute('INSERT INTO collection_control.caption_record_lineage(operation_id,record_id,record_sha256,stage_key) SELECT $1,record_id,record_sha256,stage_key FROM collection_control.caption_record_lineage WHERE operation_id=$2 ON CONFLICT DO NOTHING',op['operation_id'],col['current_operation'])
            if col and col['current_operation'] and await c.fetchval("SELECT to_regclass('collection_control.web_generation_manifests')"):
                await c.execute('INSERT INTO collection_control.web_generation_manifests(operation_id,manifest_uri,manifest_sha256,raw_root,stage_root,contract,inherited_from) SELECT $1,manifest_uri,manifest_sha256,raw_root,stage_root,contract,operation_id FROM collection_control.web_generation_manifests WHERE operation_id=$2 ON CONFLICT DO NOTHING',op['operation_id'],col['current_operation'])
                if await c.fetchval('SELECT EXISTS(SELECT 1 FROM collection_control.web_generation_manifests WHERE operation_id=$1)',op['operation_id']):
                    await c.execute('UPDATE collection_control.operations SET web_graph_required=true WHERE operation_id=$1',op['operation_id'])
                    op['web_graph_required']=True
            return op

    async def reproject(self,collection_id,key,*,expected_operation_id=None):
        """New physical projection of the exact current admitted snapshot."""
        async with self.pool.acquire() as c,c.transaction():
            col=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',collection_id)
            if not col or col['deleted'] or not col['current_operation']:raise Conflict('No live publication to reproject')
            old=await c.fetchrow('SELECT * FROM collection_control.operations WHERE collection_id=$1 AND idempotency_key=$2',collection_id,key)
            if old and old['status']=='published' and old['operation_id']==col['current_operation']:
                if old['kind']!='snapshot':raise Conflict('Reprojection key reused')
                await self._proof(c,old)
                return dict(old)
            if expected_operation_id is not None and str(col['current_operation'])!=str(expected_operation_id):raise Conflict('Publication changed before reprojection')
            parent=await c.fetchrow("SELECT * FROM collection_control.operations WHERE operation_id=$1 AND collection_id=$2 AND status='published'",col['current_operation'],collection_id)
            if not parent:raise Conflict('Selected publication is unavailable')
            await self._proof(c,parent)
            pinned=ProvenanceRepository(PinnedPool(c),handle_prefix=self.handle_prefix,incremental=self.incremental)
            op=await pinned.prepare(collection_id,key,'snapshot',col['revision'],parent['artifact_sha256'],col['contract_sha256'])
            await c.execute("INSERT INTO collection_control.generation_admissions(operation_id,artifact_sha256,content_sha256,row_count,inherited_from,policy_version) SELECT $1,artifact_sha256,content_sha256,row_count,operation_id,policy_version FROM collection_control.generation_admissions WHERE operation_id=$2 ON CONFLICT DO NOTHING",op['operation_id'],parent['operation_id'])
            for table in ('record_lineage','caption_record_lineage'):
                if await c.fetchval('SELECT to_regclass($1)','collection_control.'+table):
                    await c.execute('INSERT INTO collection_control.'+table+'(operation_id,record_id,record_sha256,stage_key) SELECT $1,record_id,record_sha256,stage_key FROM collection_control.'+table+' WHERE operation_id=$2 ON CONFLICT DO NOTHING',op['operation_id'],parent['operation_id'])
            await pinned._proof(c,op)
            return op

    async def _proof(self,c,op):
        proof=await c.fetchrow('SELECT * FROM collection_control.generation_admissions WHERE operation_id=$1',op['operation_id'])
        if not proof or proof['artifact_sha256']!=op['artifact_sha256'] or proof['policy_version']!='retained-source-stage.v1':
            raise Conflict('Generation has no matching admitted-stage proof')
        rows=await c.fetch("SELECT l.record_id,l.record_sha256,a.stage_key,a.stage_artifact_sha256,a.raw_sha256,a.collection_id,a.proof,a.envelope_sha256,s.input_envelope,s.artifact_digest,s.artifact_uri,s.artifact_size_bytes,s.status,s.document_id,s.job_id,a.document_id AS admitted_document,d.source_digest,d.source_uri,d.job_id AS document_job,j.corpus_namespace,j.pipeline_version,j.status AS job_status,d.status AS document_status,j.metadata AS job_metadata FROM collection_control.record_lineage l JOIN collection_control.stage_admissions a USING(stage_key) JOIN public.stage_units s ON s.idempotency_key=a.stage_key JOIN public.documents d ON d.document_id=a.document_id JOIN public.jobs j ON j.job_id=s.job_id WHERE l.operation_id=$1 FOR SHARE OF s,d,j",op['operation_id'])
        captions=await self._caption_rows(c,op)
        if len(rows)+len(captions)!=proof['row_count'] or hashlib.sha256(canonical(sorted((r['record_id'],r['record_sha256']) for r in list(rows)+list(captions)))).hexdigest()!=proof['content_sha256']:
            raise Conflict('Incomplete or changed per-record admission coverage')
        verified_stages=set()
        for row in rows:
            try:
                source_proof=decoded(row['proof']);job_meta=decoded(row['job_metadata'])
                if row['job_status'] in ('cancelled','canceled','deleted','failed','superseded') or row['document_status'] in ('cancelled','canceled','deleted','failed','superseded') or row['status']!='committed' or full_hash(row['artifact_digest'])!=row['stage_artifact_sha256'] or full_hash(row['source_digest'])!=row['raw_sha256'] or row['admitted_document']!=row['document_id'] or row['job_id']!=row['document_job'] or row['collection_id']!=op['collection_id'] or row['corpus_namespace']!=op['collection_id'] or envelope_digest(decoded(row['input_envelope']))!=row['envelope_sha256'] or source_proof['pipeline_version']!=row['pipeline_version'] or source_proof['source_uri']!=row['source_uri'] or source_proof['acl']!=job_meta['acl'] or source_proof['ingestion_run_id']!=job_meta['ingestion_run_id'] or source_proof['source_manifest']!=job_meta['source_manifests'][row['document_id']]:
                    raise Conflict('Legacy stage/source/provenance join no longer matches')
                if row['stage_key'] not in verified_stages:
                    if row['artifact_uri']!=source_proof['stage_artifact_uri'] or row['artifact_size_bytes']!=source_proof['stage_artifact_size_bytes']:
                        raise Conflict('Retained source stage location/size differs from admission')
                    # Retention is rechecked for inherited records as well as
                    # new source stages. Read each distinct source once rather
                    # than rereading a full PDF for every admitted chunk.
                    manifest=source_proof['source_manifest']
                    await asyncio.to_thread(verified_file,source_proof['raw_root'],source_proof['raw_artifact_uri'],row['raw_sha256'])
                    await asyncio.to_thread(verified_file,source_proof['raw_root'],manifest['uri'],full_hash(manifest['sha256']))
                    await asyncio.to_thread(verified_file,source_proof['stage_root'],source_proof['stage_artifact_uri'],row['stage_artifact_sha256'],source_proof['stage_artifact_size_bytes'])
                    verified_stages.add(row['stage_key'])
            except (KeyError,TypeError,AttributeError) as e:
                raise Conflict('Incomplete legacy admission join') from e
        from .web_groups import verify_group
        await verify_group(c,op)
        return proof,list(rows)+list(captions)

    async def has_admission(self,operation_id):
        async with self.pool.acquire() as c:
            return bool(await c.fetchval('SELECT EXISTS(SELECT 1 FROM collection_control.generation_admissions WHERE operation_id=$1)',operation_id))

    async def verify_admission(self,lease,artifact):
        async with self.pool.acquire() as c,c.transaction():
            col,op=await self._locked(c,lease['operation_id']);self._owned(col,op,lease)
            proof,rows=await self._proof(c,op)
            expected={r['record_id']:record_digest(r) for r in artifact['records']}
            if hashlib.sha256(canonical(artifact)).hexdigest()!=proof['artifact_sha256'] or proof['content_sha256']!=content_digest(artifact['records']) or len(expected)!=proof['row_count'] or {r['record_id']:r['record_sha256'] for r in rows}!=expected:
                raise Conflict('Artifact records differ from admitted legacy stages')
            return True

    async def publish(self,lease):
        async with self.pool.acquire() as c,c.transaction():
            col,op=await self._locked(c,lease['operation_id'])
            if op['status']!='published':
                self._owned(col,op,lease)
                proof,_=await self._proof(c,op)
                receipts=await c.fetch('SELECT content_sha256,row_count FROM collection_control.receipts WHERE operation_id=$1 AND fence=$2',op['operation_id'],op['fence'])
                if any((r['content_sha256'],r['row_count'])!=(proof['content_sha256'],proof['row_count']) for r in receipts):raise Conflict('Store receipts differ from admitted generation')
            # Source/stage rows remain share-locked until this outer commit.
            await ControlRepository(PinnedPool(c)).publish(lease)

    async def _caption_rows(self,c,op):
        """Revalidate inherited caption derivations at every publication gate."""
        if not await c.fetchval("SELECT to_regclass('collection_control.caption_record_lineage')"):
            return []
        from .provenance import caption_record,verified_file
        from .artifacts import _unique_object
        import json
        rows=await c.fetch('SELECT l.record_id,l.record_sha256,a.stage_key,a.collection_id,a.document_id,a.parent_source_stage_key,a.source_revision_id,a.stage_artifact_sha256,a.envelope_sha256,a.proof,a.record_id AS admitted_record_id,a.record_sha256 AS admitted_record_sha256 FROM collection_control.caption_record_lineage l JOIN collection_control.caption_admissions a USING(stage_key) WHERE l.operation_id=$1',op['operation_id'])
        for row in rows:
            if row['record_id']!=row['admitted_record_id'] or row['record_sha256']!=row['admitted_record_sha256']:raise Conflict('Caption per-record lineage differs from immutable admission')
            proof=decoded(row['proof'])
            stage=await c.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1 FOR SHARE',row['stage_key'])
            asset=await c.fetchrow('SELECT * FROM caption_control.assets WHERE stage_key=$1 FOR SHARE',row['stage_key'])
            parent=await c.fetchrow('SELECT * FROM collection_control.stage_admissions WHERE stage_key=$1',row['parent_source_stage_key'])
            supersession=proof.get('supersession')
            if supersession and str(op['operation_id'])==proof['admission_operation_id'] and op['status']!='published':
                base=await c.fetchrow('SELECT current_operation FROM collection_control.collections WHERE collection_id=$1',op['collection_id'])
                if str(base['current_operation'])!=supersession['operation_id']:raise Conflict('Empty source supersession base publication changed')
                source_revisions=await c.fetch('SELECT DISTINCT a.source_revision_id FROM collection_control.record_lineage l JOIN collection_control.stage_admissions a USING(stage_key) JOIN collection_control.source_revisions r ON r.collection_id=a.collection_id AND r.source_revision_id=a.source_revision_id WHERE l.operation_id=$1 AND r.canonical_uri=$2',base['current_operation'],supersession['canonical_uri'])
                caption_revisions=await c.fetch('SELECT DISTINCT a.source_revision_id FROM collection_control.caption_record_lineage l JOIN collection_control.caption_admissions a USING(stage_key) JOIN caption_control.assets x ON x.stage_key=a.stage_key WHERE l.operation_id=$1 AND x.source_uri=$2',base['current_operation'],supersession['canonical_uri'])
                if {r['source_revision_id'] for r in list(source_revisions)+list(caption_revisions)}!={supersession['source_revision_id']}:raise Conflict('Empty source supersession previous revision differs from published base')
            capture_only=proof.get('capture_only')
            if capture_only:
                from .provenance import empty_source_capture
                capture_stage=await c.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1 FOR SHARE',capture_only['stage_key'])
                if not capture_stage or capture_stage['status']!='committed' or capture_stage['stage']!='embedding' or capture_stage['job_id']!=asset['parent_job_id'] or capture_stage['document_id']!=asset['document_id'] or full_hash(capture_stage['artifact_digest'])!=capture_only['artifact_sha256'] or envelope_digest(decoded(capture_stage['input_envelope']))!=capture_only['envelope_sha256']:
                    raise Conflict('Empty source capture stage changed')
                capture_checkpoint=json.loads(verified_file(proof['stage_root'],capture_stage['artifact_uri'],capture_only['artifact_sha256']),object_pairs_hook=_unique_object)
                capture_document=await c.fetchrow('SELECT * FROM public.documents WHERE document_id=$1 FOR SHARE',asset['document_id'])
                capture_job=await c.fetchrow('SELECT * FROM public.jobs WHERE job_id=$1 FOR SHARE',asset['parent_job_id'])
                parent=empty_source_capture(capture_checkpoint,capture_stage,capture_document,capture_job,capture_only['raw_root'])
                if parent['source_revision_id']!=row['source_revision_id']:raise Conflict('Empty source revision changed')
            current=await c.fetchrow('SELECT d.*,j.status AS job_status,j.metadata AS job_metadata,j.corpus_namespace FROM public.documents d JOIN public.jobs j ON j.job_id=d.job_id WHERE d.document_id=$1 FOR SHARE OF d,j',parent['document_id'] if parent else '')
            if not stage or not asset or not parent or not current or stage['status']!='committed' or stage['stage']!='caption' or str(stage['job_id'])!=str(asset['parent_job_id']) or stage['document_id']!=asset['document_id'] or row['document_id']!=asset['document_id'] or row['collection_id']!=op['collection_id'] or parent['collection_id']!=op['collection_id'] or full_hash(stage['artifact_digest'])!=row['stage_artifact_sha256'] or envelope_digest(decoded(stage['input_envelope']))!=row['envelope_sha256']:
                raise Conflict('Caption stage/source admission no longer matches')
            metadata=decoded(current['job_metadata']);scope=decoded(asset['source_metadata'])
            if current['status'] in ('cancelled','canceled','deleted','failed','superseded') or current['job_status'] in ('cancelled','canceled','deleted','failed','superseded') or current['source_uri']!=asset['source_uri'] or current['source_digest']!=asset['source_digest'] or full_hash(current['source_digest'])!=parent['raw_sha256'] or current['job_id']!=asset['parent_job_id'] or current['corpus_namespace']!=op['collection_id'] or metadata.get('acl')!=scope.get('acl') or metadata.get('ingestion_run_id')!=scope.get('ingestion_run_id') or metadata.get('ingestion_options',{}).get('captions_enabled') is not True:
                raise Conflict('Caption current source/ACL/opt-in changed')
            # A caption may only accompany its admitted source revision in this
            # generation, including when inherited by membership operations.
            if not capture_only and not await c.fetchval('SELECT EXISTS(SELECT 1 FROM collection_control.record_lineage WHERE operation_id=$1 AND stage_key=$2)',op['operation_id'],parent['stage_key']):
                raise Conflict('Caption parent source revision absent from generation')
            stage_root=proof['stage_root']
            checkpoint=json.loads(verified_file(stage_root,stage['artifact_uri'],row['stage_artifact_sha256']),object_pairs_hook=_unique_object)
            manifest=json.loads(verified_file(stage_root,asset['manifest_uri'],asset['manifest_sha256']),object_pairs_hook=_unique_object)
            extraction=await c.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1 FOR SHARE',manifest['parent_stage_key'])
            if not extraction or extraction['status']!='committed' or extraction['stage']!='extraction' or extraction['job_id']!=asset['parent_job_id'] or extraction['document_id']!=asset['document_id']:
                raise Conflict('Caption extraction parent no longer committed')
            extraction_result=json.loads(verified_file(stage_root,extraction['artifact_uri'],full_hash(extraction['artifact_digest'])),object_pairs_hook=_unique_object)
            associated=[j for j in extraction_result.get('caption_jobs',[]) if j.get('envelope',{}).get('idempotency_key')==row['stage_key']]
            if len(associated)!=1 or associated[0].get('manifest_uri')!=asset['manifest_uri'] or associated[0].get('manifest_sha256')!=asset['manifest_sha256'] or associated[0].get('asset_sha256')!=asset['asset_sha256']:
                raise Conflict('Committed extraction does not bind this caption asset')
            record=caption_record(checkpoint,stage,asset,manifest,parent,proof['contract'],stage_root,identity_policy=proof.get('record_identity_policy','caption-stage.v1'))
            if record_digest(record)!=row['record_sha256'] or record['record_id']!=row['record_id']:
                raise Conflict('Caption retained vector/text derivation differs')
        return rows
