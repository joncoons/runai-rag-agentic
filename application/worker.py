"""Application-only outbox, source admission and independent publication roles."""
import asyncio
from dataclasses import replace
from datetime import timedelta
import hashlib
import json
import logging
import os
from pathlib import Path
import signal
import socket
from uuid import UUID
from collection_control.artifacts import canonical
from collection_control.provenance import verified_file,full_hash
from collection_control.repository import Conflict,decoded
from collection_control.stage_bridge import StageCollectionRepository,CaptionCollectionRepository
from nv_ingest_durable.artifacts import FileArtifactStore,JsonResultStager
from nv_ingest_durable.contracts import Stage,StageEnvelope
from nv_ingest_durable.postgres import PostgresManifestRepository
from nv_ingest_durable.redis_streams import RedisStreamsTransport
from .context import Context
from .settings import Settings
from .intake import embedding_config
from .provenance import enrich_checkpoint

log=logging.getLogger(__name__)

def revision_dict(row):
    value=dict(row);value['job_id']=str(value['job_id']);return value

class ApplicationWorker:
    def __init__(self,context):
        self.context=context;self.pool=context.pool;self.settings=context.settings
        self.legacy=PostgresManifestRepository(self.pool)
        self.transport=RedisStreamsTransport.from_url(self.settings.redis_url)

    async def relay_once(self):
        # Never consume the historical application's outbox. Captions have their
        # own due-time aware relay, with the same application stream prefix.
        rows=await self.pool.fetch("""SELECT e.* FROM public.outbox_events e JOIN public.stage_units s USING(idempotency_key)
            JOIN application.jobs a ON a.job_id=s.job_id WHERE e.published_at IS NULL
            AND e.envelope->>'job_id'=s.job_id::text AND e.envelope->>'document_id'=s.document_id
            AND e.target_stream=ANY($1::text[]) AND NOT EXISTS(SELECT 1 FROM application.source_revisions p WHERE p.document_id=s.document_id AND p.web_role='page' AND e.target_stream='rag.agentic.extraction.pending' AND EXISTS(SELECT 1 FROM collection_control.web_group_members m WHERE m.group_id=p.web_group_id AND m.role='image' AND NOT EXISTS(SELECT 1 FROM public.stage_units i WHERE i.document_id=m.document_id AND i.stage='extraction' AND i.status='committed'))) ORDER BY e.created_at LIMIT 32""",['rag.agentic.extraction.pending','rag.agentic.pages.extraction.pending','rag.agentic.images.extraction.pending','rag.agentic.binaries.extraction.pending','rag.agentic.pages.index','rag.agentic.assets.index','rag.agentic.index','rag.agentic.admission.deadletter'])
        for row in rows:
            message=StageEnvelope.from_stream_fields(decoded(row['envelope']))
            identifier=await self.transport.publish(row['target_stream'],message)
            await self.legacy.mark_outbox_published(str(row['event_id']),identifier)
        return len(rows)

    async def source(self,message):
        e=message.envelope
        if e.stage!=Stage.INDEX:raise Conflict('Source admission requires INDEX notification')
        revision=await self.pool.fetchrow('SELECT * FROM application.source_revisions WHERE document_id=$1 AND job_id=$2',e.document_id,UUID(e.job_id))
        if not revision:raise Conflict('Notification outside application scope')
        if revision['status']=='cancelled':return
        if revision['status'] in ('published','captured'):
            if revision.get('web_asset_download_id'):await self._admit_deferred_asset(revision,revision['records'])
            return
        col=await self.pool.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1',revision['collection_id'])
        if not col or col['deleted']:return
        extraction=await self.pool.fetchrow("SELECT * FROM public.stage_units WHERE job_id=$1 AND document_id=$2 AND stage='extraction' AND status='committed'",UUID(e.job_id),e.document_id)
        if not extraction or extraction['artifact_uri']!=e.artifact_uri:raise Conflict('Source notification lacks matching extraction commit')
        raw=await asyncio.to_thread(verified_file,self.settings.stages,e.artifact_uri,full_hash(extraction['artifact_digest']))
        checkpoint=json.loads(raw)
        if revision.get('web_role')=='image':
            if checkpoint.get('schema_version')!='native-web-image-evidence.v1' or checkpoint.get('status')!='complete' or checkpoint.get('source_revision_id')!=revision['source_revision_id']:raise Conflict('Image evidence source revision mismatch')
            if revision.get('web_asset_download_id'):
                await self._admit_deferred_asset(revision,0);return
            await self.pool.execute("UPDATE application.source_revisions SET status='captured',records=0 WHERE source_revision_id=$1",revision['source_revision_id'])
            await self.refresh_jobs();return
        committed=None
        async with self.pool.acquire() as c,c.transaction():
            locked=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',revision['collection_id'])
            revision=await c.fetchrow('SELECT * FROM application.source_revisions WHERE source_revision_id=$1 FOR UPDATE',revision['source_revision_id'])
            if not locked or locked['deleted'] or revision['status'] in ('published','captured','failed','cancelled'):return
            earlier=await c.fetchval("""SELECT EXISTS(SELECT 1 FROM application.source_revisions old WHERE old.source_id=$1 AND (old.observed_at,old.source_revision_id)<($2,$3) AND old.status NOT IN ('published','captured','failed','cancelled'))""",revision['source_id'],revision['observed_at'],revision['source_revision_id'])
            if earlier:raise Conflict('Earlier revision of this source is still being admitted')
            existing=await c.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1',revision['admission_stage_key']) if revision['admission_stage_key'] else None
            if existing and existing['status']=='committed':
                committed=existing
            else:
                if not (revision.get('web_group_id') and revision.get('web_role')=='page' and getattr(self.context.control,'incremental',False)) and await c.fetchval("SELECT EXISTS(SELECT 1 FROM collection_control.operations WHERE collection_id=$1 AND status='pending')",revision['collection_id']):raise Conflict('Collection publication pending')
                if not revision['admission_stage_key']:
                    # The published snapshot is authoritative even if the
                    # application pointer has not yet been reconciled.
                    previous=await c.fetch("""SELECT DISTINCT source_revision_id FROM (
                      SELECT a.source_revision_id FROM collection_control.record_lineage l JOIN collection_control.stage_admissions a USING(stage_key) JOIN collection_control.source_revisions r ON r.collection_id=a.collection_id AND r.source_revision_id=a.source_revision_id WHERE l.operation_id=$1 AND r.canonical_uri=$2
                      UNION SELECT a.source_revision_id FROM collection_control.caption_record_lineage l JOIN collection_control.caption_admissions a USING(stage_key) JOIN caption_control.assets x ON x.stage_key=a.stage_key WHERE l.operation_id=$1 AND x.source_uri=$2
                    ) revisions""",locked['current_operation'],revision['canonical_uri'])
                    if len(previous)>1:raise Conflict('Published source has conflicting revisions')
                    await c.execute("UPDATE application.source_revisions SET status='admitting',previous_source_revision_id=$2 WHERE source_revision_id=$1",revision['source_revision_id'],previous[0]['source_revision_id'] if previous else None)
                    revision=await c.fetchrow('SELECT * FROM application.source_revisions WHERE source_revision_id=$1',revision['source_revision_id'])
        if committed:
            # Recover the DB/filesystem commit boundary without regenerating a
            # checkpoint using changed runtime configuration or source pointers.
            admission=StageEnvelope.from_stream_fields(decoded(committed['input_envelope']))
            artifact=await self.legacy.get_committed(committed['idempotency_key'])
            await self._commit_source(revision,admission,None,artifact)
            retained=json.loads(await asyncio.to_thread(verified_file,self.settings.stages,artifact.uri,full_hash(artifact.digest)))
            await self._record_source_admission(revision,len(retained['documents']))
            return
        config=embedding_config(self.settings);model=self.settings.contract['embedding_checkpoint_sha256']
        processing=decoded(extraction['input_envelope']) or {}
        if processing.get('stage')!='extraction' or processing.get('job_id')!=e.job_id or processing.get('document_id')!=e.document_id or processing.get('artifact_uri')!=revision['raw_uri'] or processing.get('pipeline_version')!=e.pipeline_version or full_hash(processing.get('model_digest'))!=model or full_hash(processing.get('config_digest'))!=config:raise Conflict('Native extraction vector processing contract differs from admission')
        enriched,manifest=await asyncio.to_thread(enrich_checkpoint,checkpoint,revision_dict(revision),raw_root=self.settings.raw,model_digest=model,config_digest=config,ocr_model_digest=os.environ['OCR_MODEL_DIGEST'])
        manifest_raw=canonical(manifest);manifest_sha=hashlib.sha256(manifest_raw).hexdigest()
        # Retained manifest is content-addressed, verified before source commit.
        from nv_ingest_durable.deferred_caption import retain
        uri,_=retain(self.settings.raw,manifest_raw,'.json')
        enriched['retained_source_capture']['source_manifest']['uri']=uri
        key='sha256:'+hashlib.sha256(canonical({'extraction':extraction['idempotency_key'],'revision':revision['source_revision_id'],'previous':revision['previous_source_revision_id'],'artifact':full_hash(extraction['artifact_digest']),'producer':'source-admission-v1'})).hexdigest()
        admission=replace(e,stage=Stage.EMBEDDING,idempotency_key=key,model_digest='sha256:'+model,config_digest='sha256:'+config)
        if revision['admission_stage_key'] and revision['admission_stage_key']!=key:raise Conflict('Source admission processing identity changed during retry')
        async with self.pool.acquire() as c,c.transaction():
            await c.execute("UPDATE public.jobs SET metadata=jsonb_set(metadata,'{source_manifests}',COALESCE(metadata->'source_manifests','{}'::jsonb)||$2::jsonb) WHERE job_id=$1",UUID(e.job_id),json.dumps({e.document_id:{'uri':uri,'sha256':manifest_sha}}))
            await c.execute("INSERT INTO public.stage_units(idempotency_key,job_id,document_id,stage,status,input_envelope) VALUES($1,$2,$3,'embedding','pending',$4::jsonb) ON CONFLICT DO NOTHING",key,UUID(e.job_id),e.document_id,json.dumps(admission.to_dict()))
            await c.execute('UPDATE application.source_revisions SET admission_stage_key=$2 WHERE source_revision_id=$1',revision['source_revision_id'],key)
        lease=await self.legacy.acquire_lease(admission,owner=socket.gethostname(),ttl=timedelta(seconds=180))
        if lease:
            try:
                pending,_=await JsonResultStager(self.settings.stages).stage(admission,enriched)
                pending=replace(pending,metadata={**pending.metadata,'payload_format':'es_docs'})
                artifact=await FileArtifactStore(self.settings.stages).finalize(pending)
                await self._commit_source(revision,admission,lease,artifact)
            except Exception as error:
                await self._admission_failure(message,admission,lease,error,revision['collection_id'],revision['source_revision_id'])
                raise
        elif not await self.pool.fetchval("SELECT status='committed' FROM public.stage_units WHERE idempotency_key=$1",key):raise Conflict('Admission lease busy')
        await self._record_source_admission(revision,len(enriched['documents']),key)

    async def _admission_failure(self,message,envelope,lease,error,collection_id,revision_id):
        from collection_control.pinned_connection import PinnedPool
        # Keep failure + terminal transition under the same exact lease and row
        # lock. A stale attempt must never deadletter its replacement owner.
        async with self.pool.acquire() as c,c.transaction():
            await c.fetchrow('SELECT collection_id FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',collection_id)
            legacy=type(self.legacy)(PinnedPool(c))
            attempts=await legacy.record_failure(envelope,lease_token=lease,error=error)
            contention=isinstance(error,Conflict) and str(error) in ('Collection publication pending','Another collection mutation is pending')
            permanent=isinstance(error,Conflict) and str(error)=='Payload exceeds supported Milvus JSON field size'
            if attempts and (permanent or (attempts>=5 and not contention)):
                await legacy.deadletter(replace(message,envelope=envelope),error=error,target_stream='rag.agentic.admission.deadletter')
                await c.execute("UPDATE application.source_revisions SET status='failed' WHERE source_revision_id=$1 AND status NOT IN ('published','captured','cancelled')",revision_id)

    async def _commit_source(self,revision,envelope,lease,artifact):
        if revision.get('web_asset_download_id'):
            bridge,reference=await self._deferred_bridge(revision)
            await bridge.commit_child_candidate(envelope,lease_token=lease,artifact=artifact,acquisition=reference)
        elif revision.get('web_group_id'):
            from collection_control.web_groups import GroupRepository
            bridge=GroupRepository(self.legacy,self.context.control,revision['collection_id'],self.settings.contract,self.settings.stages,self.settings.raw,self.settings.snapshots)
            await bridge.commit_candidate(envelope,lease_token=lease,artifact=artifact,group_id=revision['web_group_id'])
        else:
            bridge=StageCollectionRepository(self.legacy,self.context.control,revision['collection_id'],self.settings.contract,self.settings.stages,self.settings.raw,self.settings.snapshots)
            await bridge.commit_stage(envelope,lease_token=lease,artifact=artifact,downstream=())

    async def _record_source_admission(self,revision,count,key=None):
        key=key or revision['admission_stage_key']
        if revision.get('web_asset_download_id'):
            await self._admit_deferred_asset(revision,count);return
        if revision.get('web_group_id'):
            await self.pool.execute("UPDATE application.source_revisions SET status='admitted',records=$2 WHERE source_revision_id=$1 AND status NOT IN ('published','cancelled')",revision['source_revision_id'],count)
            if getattr(self.context.control,'incremental',False) and revision.get('web_role')=='page':return
            from collection_control.web_groups import GroupRepository
            bridge=GroupRepository(self.legacy,self.context.control,revision['collection_id'],self.settings.contract,self.settings.stages,self.settings.raw,self.settings.snapshots)
            op=await bridge.admit_group(revision['web_group_id'])
            await self.pool.execute("UPDATE application.source_revisions SET operation_id=$2,status=CASE WHEN web_role='image' THEN 'captured' ELSE 'publishing' END WHERE web_group_id=$1 AND status NOT IN ('published','cancelled')",revision['web_group_id'],op['operation_id'])
            await self.reconcile_publications();await self.refresh_jobs();return
        prior=await self.pool.fetchrow('SELECT operation_id FROM collection_control.stage_admissions WHERE stage_key=$1',key)
        await self.pool.execute("UPDATE application.source_revisions SET status=$2,operation_id=$3,records=$4 WHERE source_revision_id=$1 AND status NOT IN ('published','cancelled')",revision['source_revision_id'],'publishing' if prior else 'captured',prior['operation_id'] if prior else None,count)
        if not prior and count==0 and revision.get('previous_source_revision_id'):
            eligible=await self.pool.fetchval('SELECT count(*) FROM caption_control.assets WHERE document_id=$1 AND terminal_reason IS NULL',revision['document_id'])
            if not eligible:
                # Preserve last good publication rather than report a successful
                # replacement while leaving its previous records searchable.
                async with self.pool.acquire() as c,c.transaction():
                    await c.execute("UPDATE application.source_revisions SET status='failed' WHERE source_revision_id=$1",revision['source_revision_id'])
                    await c.execute("UPDATE application.jobs SET status='failed',stage='publication',errors=errors||$2::jsonb WHERE job_id=$1",revision['job_id'],json.dumps([{'code':'empty_replacement_not_published'}]))
        await self.reconcile_publications()
        await self.refresh_jobs()

    async def _deferred_bridge(self,revision):
        from collection_control.web_groups import GroupRepository
        task=await self.pool.fetchrow('SELECT * FROM application.web_asset_downloads WHERE download_id=$1',revision['web_asset_download_id'])
        if not task or not task['publication_manifest_uri']:raise Conflict('Deferred asset lacks committed acquisition association')
        bridge=GroupRepository(self.legacy,self.context.control,revision['collection_id'],self.settings.contract,self.settings.stages,self.settings.raw,self.settings.snapshots)
        return bridge,{'uri':task['publication_manifest_uri'],'sha256':task['publication_manifest_sha256']}

    async def _yield_to_ready_pages(self,collection_id):
        ready=await self.pool.fetchval("""SELECT EXISTS(SELECT 1 FROM application.source_revisions r
          JOIN public.stage_units s ON s.document_id=r.document_id AND s.job_id=r.job_id
          WHERE r.collection_id=$1 AND r.web_role='page' AND s.stage='extraction' AND s.status='committed'
          AND (r.status IN ('queued','admitting','admitted') OR (r.status='publishing' AND EXISTS(
            SELECT 1 FROM collection_control.operations o WHERE o.operation_id=r.operation_id AND o.status='pending'))))""",collection_id)
        if ready:raise Conflict('Ready HTML publication has priority over background enrichment')

    async def _admit_deferred_asset(self,revision,count):
        await self._yield_to_ready_pages(revision['collection_id'])
        bridge,reference=await self._deferred_bridge(revision)
        from .web_captions import ParentEmbedding
        if not hasattr(self,'_asset_parent_embedding'):self._asset_parent_embedding=ParentEmbedding()
        op=await bridge.admit_child(reference,embed=self._asset_parent_embedding,embedding_config_digest=embedding_config(self.settings))
        await self.pool.execute("UPDATE application.source_revisions SET status=$2,operation_id=$3,records=$4 WHERE source_revision_id=$1 AND status NOT IN ('published','cancelled')",revision['source_revision_id'],'publishing' if op else 'captured',op['operation_id'] if op else None,count)
        await self.reconcile_publications();await self.refresh_jobs()

    async def acquire_asset_once(self):
        from .deferred_web_assets import DeferredWebAssets
        from .deferred_asset_native import NativeAssetSubmitter
        if not hasattr(self,'_deferred_acquisition'):self._deferred_acquisition=DeferredWebAssets(self.pool,self.settings.raw)
        if not hasattr(self,'_native_asset_submitter'):self._native_asset_submitter=NativeAssetSubmitter(self.settings)
        await self._deferred_acquisition.relay_once(self.transport.client)
        return sum(await asyncio.gather(*(self._deferred_acquisition.consume_once(self.transport.client,socket.gethostname()+'-'+str(slot),self._native_asset_submitter) for slot in range(4))))

    async def caption(self,message):
        e=message.envelope
        asset=await self.pool.fetchrow('SELECT a.* FROM caption_control.assets a JOIN application.jobs j ON j.job_id=a.parent_job_id WHERE a.stage_key=$1',e.idempotency_key)
        if not asset:raise Conflict('Caption notification outside application scope')
        stage=await self.pool.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1',e.idempotency_key)
        if e.stage!=Stage.INDEX or str(asset['parent_job_id'])!=e.job_id or asset['document_id']!=e.document_id or not stage or stage['status']!='committed' or stage['stage']!='caption' or stage['artifact_uri']!=e.artifact_uri:raise Conflict('Caption notification differs from committed result')
        col=await self.pool.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1',asset['collection_id'])
        if not col or col['deleted']:return
        await self._yield_to_ready_pages(asset['collection_id'])
        web=await self.pool.fetchval("SELECT web_role FROM application.source_revisions WHERE document_id=$1",e.document_id)
        if web=='image':
            from collection_control.web_captions import WebCaptionRepository
            from .web_captions import ParentEmbedding
            if not hasattr(self,'_web_caption_embedding'):self._web_caption_embedding=ParentEmbedding()
            bridge=WebCaptionRepository(self.context.control,asset['collection_id'],self.settings.contract,self.settings.stages,self.settings.raw,self.settings.snapshots)
            op=await bridge.admit(e.idempotency_key,embed=self._web_caption_embedding,embedding_config_digest=embedding_config(self.settings),expected_model_digest=os.environ['CAPTION_MODEL_DIGEST'])
            await self.pool.execute("INSERT INTO application.caption_publications(caption_stage_key,operation_id,status,last_error_type) VALUES($1,$2,$3,$4) ON CONFLICT(caption_stage_key) DO UPDATE SET operation_id=EXCLUDED.operation_id,status=EXCLUDED.status,last_error_type=EXCLUDED.last_error_type",e.idempotency_key,op['operation_id'] if op else None,'publishing' if op else 'failed',None if op else 'StaleWebImage')
            return
        obsolete=await self.pool.fetchval("SELECT status='superseded' FROM public.documents WHERE document_id=$1",e.document_id)
        if obsolete:
            await self.pool.execute("INSERT INTO application.caption_publications(caption_stage_key,status,last_error_type) VALUES($1,'failed','SupersededSource') ON CONFLICT(caption_stage_key) DO UPDATE SET status='failed',last_error_type='SupersededSource'",e.idempotency_key)
            return
        bridge=CaptionCollectionRepository(self.context.control,asset['collection_id'],self.settings.contract,self.settings.stages,self.settings.snapshots,expected_embedding_config_digest=embedding_config(self.settings),raw_root=self.settings.raw)
        op=await bridge.admit(e.idempotency_key,expected_model_digest=os.environ['CAPTION_MODEL_DIGEST'])
        await self.pool.execute("INSERT INTO application.caption_publications(caption_stage_key,operation_id) VALUES($1,$2) ON CONFLICT(caption_stage_key) DO UPDATE SET operation_id=EXCLUDED.operation_id",e.idempotency_key,op['operation_id'])

    async def consume_once(self,stream,handler,*,batch=1):
        group='application-admission-v1';consumer=socket.gethostname()
        await self.transport.ensure_group(stream,group)
        if not hasattr(self,'_reclaim_cursors'):self._reclaim_cursors={}
        cursor,retried=await self.transport.reclaim(stream,group,consumer,min_idle_ms=5000,start_id=self._reclaim_cursors.get(stream,'0-0'),count=batch)
        self._reclaim_cursors[stream]=cursor
        fresh=await self.transport.read(stream,group,consumer,count=batch,block_ms=100)
        messages=[*retried,*fresh]
        for message in messages:
            try:
                await handler(message)
                await self.transport.acknowledge(message)
            except Exception as error:
                # Pending stream entry is durable retry; no source/model replay.
                log.warning('admission_retry stream=%s error=%s detail=%s',stream,type(error).__name__,str(error) if isinstance(error,Conflict) else type(error).__name__)
        return len(messages)

    async def run_once(self,role):
        # Each admission role runs in its own process/Context/connection pool.
        # Do not consume both streams serially, even when one is currently idle.
        if role=='admission':return await self.consume_once('rag.agentic.index',self.source)
        if role=='caption-admission':return await self.consume_once('rag.agentic.caption.results',self.caption)
        if role=='page-admission':
            count=await self.consume_once('rag.agentic.pages.index',self.source,batch=32)
            return count+await self.admit_ready_pages()
        if role=='asset-admission':return await self.consume_once('rag.agentic.assets.index',self.source)
        if role=='asset-download':return await self.acquire_asset_once()
        if role=='relay':return await self.relay_once()
        if role=='writer':return await self.publish_once()
        raise ValueError('Unknown application worker role')

    async def admit_ready_pages(self):
        from collection_control.web_groups import GroupRepository
        rows=await self.pool.fetch("SELECT DISTINCT ON (r.collection_id) r.collection_id,r.web_group_id FROM application.source_revisions r JOIN collection_control.web_groups g ON g.group_id=r.web_group_id JOIN collection_control.collections c ON c.collection_id=r.collection_id WHERE r.web_role='page' AND r.status='admitted' AND g.state='pending' AND NOT c.deleted AND NOT EXISTS(SELECT 1 FROM collection_control.operations o WHERE o.collection_id=r.collection_id AND o.status='pending') ORDER BY r.collection_id,r.observed_at LIMIT 8")
        async def admit(row):
            try:
                bridge=GroupRepository(self.legacy,self.context.control,row['collection_id'],self.settings.contract,self.settings.stages,self.settings.raw,self.settings.snapshots)
                op=await bridge.admit_group(row['web_group_id'],batch=True)
                await self.pool.execute("UPDATE application.source_revisions r SET operation_id=$2,status='publishing' FROM collection_control.web_groups g WHERE r.web_group_id=g.group_id AND g.operation_id=$2 AND r.collection_id=$1 AND r.status='admitted'",row['collection_id'],op['operation_id'])
                return 1
            except Conflict:
                return 0
        return sum(await asyncio.gather(*(admit(r) for r in rows)))

    async def publish_once(self):
        rows=await self.pool.fetch("""SELECT o.* FROM collection_control.operations o JOIN application.collections a USING(collection_id)
          JOIN collection_control.collections c USING(collection_id) WHERE o.status='pending' AND NOT c.deleted AND (o.lease_until IS NULL OR o.lease_until<=clock_timestamp()) ORDER BY o.operation_id LIMIT 8""")
        try:
            await asyncio.gather(*(self.context.writer.run(row['operation_id'],ttl_seconds=180) for row in rows))
        finally:
            await self.reconcile_publications()
            await self.refresh_jobs()
        return len(rows)

    async def reconcile_publications(self):
        rows=await self.pool.fetch('SELECT collection_id FROM application.collections ORDER BY collection_id')
        for row in rows:await self._reconcile_collection(row['collection_id'])

    async def _reconcile_collection(self,collection_id):
        async with self.pool.acquire() as c,c.transaction():
            # Follow the same collection-first lock order as strict admission
            # and publication. Historical completion never rolls pointers back.
            await c.fetchrow('SELECT collection_id FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',collection_id)
            await c.execute("""UPDATE application.source_revisions r SET status='published' FROM collection_control.operations o WHERE o.operation_id=r.operation_id AND o.status='published' AND r.status<>'published' AND r.collection_id=$1""",collection_id)
            await c.execute("""UPDATE application.caption_publications p SET status='published',updated_at=clock_timestamp() FROM collection_control.operations o WHERE o.operation_id=p.operation_id AND o.status='published' AND p.status<>'published' AND o.collection_id=$1""",collection_id)
            await c.execute("UPDATE collection_control.web_groups g SET state='published' FROM collection_control.operations o WHERE o.operation_id=g.operation_id AND o.status='published' AND g.state<>'published' AND g.collection_id=$1",collection_id)
            await c.execute("UPDATE application.sources a SET current_revision_id=NULL FROM collection_control.collections col WHERE col.collection_id=a.collection_id AND a.collection_id=$1 AND EXISTS(SELECT 1 FROM application.source_revisions r WHERE r.source_id=a.source_id AND r.web_role='binary') AND NOT EXISTS(SELECT 1 FROM collection_control.record_lineage l JOIN collection_control.stage_admissions s USING(stage_key) WHERE l.operation_id=col.current_operation AND s.source_revision_id=a.current_revision_id)",collection_id)
            current=await c.fetch("""SELECT DISTINCT r.source_id,r.source_revision_id,r.document_id,COALESCE((SELECT a.operation_id FROM collection_control.stage_admissions a WHERE a.stage_key=r.admission_stage_key),c.current_operation) AS published_operation_id,COALESCE((SELECT a.record_count FROM collection_control.stage_admissions a WHERE a.stage_key=r.admission_stage_key),r.records) AS published_records FROM application.source_revisions r JOIN collection_control.collections c ON c.collection_id=r.collection_id
              WHERE NOT c.deleted AND c.collection_id=$1 AND (
                EXISTS(SELECT 1 FROM collection_control.record_lineage l JOIN collection_control.stage_admissions a USING(stage_key) WHERE l.operation_id=c.current_operation AND a.source_revision_id=r.source_revision_id AND a.collection_id=r.collection_id)
                OR EXISTS(SELECT 1 FROM collection_control.caption_record_lineage l JOIN collection_control.caption_admissions a USING(stage_key) WHERE l.operation_id=c.current_operation AND a.source_revision_id=r.source_revision_id AND a.collection_id=r.collection_id))""",collection_id)
            for r in current:
                await c.execute('UPDATE application.sources SET current_revision_id=$2 WHERE source_id=$1',r['source_id'],r['source_revision_id'])
                await c.execute("UPDATE application.source_revisions SET status='published',operation_id=COALESCE(operation_id,$2),records=$3 WHERE source_revision_id=$1",r['source_revision_id'],r['published_operation_id'],r['published_records'])
                await c.execute("UPDATE public.documents d SET status='superseded' FROM application.source_revisions old WHERE old.document_id=d.document_id AND old.source_id=$1 AND old.source_revision_id<>$2 AND old.status IN ('published','captured')",r['source_id'],r['source_revision_id'])
            await c.execute("""UPDATE collection_control.outbox e SET delivered_at=clock_timestamp() FROM collection_control.operations o,application.collections a WHERE e.operation_id=o.operation_id AND a.collection_id=e.collection_id AND e.event_type='generation_requested' AND o.status='published' AND e.delivered_at IS NULL AND o.collection_id=$1""",collection_id)

    async def refresh_jobs(self):
        await self.pool.execute("""UPDATE application.source_revisions r SET status='failed' WHERE r.status NOT IN ('published','captured','failed','cancelled') AND (EXISTS(SELECT 1 FROM public.stage_units s WHERE s.document_id=r.document_id AND s.stage='extraction' AND s.status='deadlettered') OR EXISTS(SELECT 1 FROM collection_control.operations o WHERE o.operation_id=r.operation_id AND o.status='cancelled'))""")
        await self.pool.execute("""UPDATE application.jobs a SET status=CASE
           WHEN EXISTS(SELECT 1 FROM collection_control.collections c WHERE c.collection_id=a.collection_id AND c.deleted) THEN 'cancelled'
           WHEN EXISTS(SELECT 1 FROM application.source_revisions r WHERE r.job_id=a.job_id AND r.status='failed') THEN 'failed'
           WHEN NOT EXISTS(SELECT 1 FROM application.source_revisions r WHERE r.job_id=a.job_id AND r.status NOT IN ('published','captured')) THEN 'completed'
           ELSE 'processing' END,
           stage=CASE WHEN NOT EXISTS(SELECT 1 FROM application.source_revisions r WHERE r.job_id=a.job_id AND r.status NOT IN ('published','captured')) THEN 'complete' WHEN EXISTS(SELECT 1 FROM application.source_revisions r WHERE r.job_id=a.job_id AND r.status='publishing') THEN 'publication' ELSE 'extraction' END,updated_at=clock_timestamp() WHERE a.status NOT IN ('cancelled','failed')""")
        # The independent publication path replaces the legacy INDEX worker.
        # Reflect completed source work in its original manifest, without making
        # async captions a document/job completion dependency or touching history.
        await self.pool.execute("""UPDATE public.documents d SET status='completed',updated_at=clock_timestamp() FROM application.source_revisions r WHERE r.document_id=d.document_id AND r.status IN ('published','captured') AND d.status NOT IN ('completed','cancelled','canceled','deleted','failed','superseded')""")
        await self.pool.execute("""UPDATE public.jobs j SET status='completed',updated_at=clock_timestamp() FROM application.jobs a WHERE j.job_id=a.job_id AND a.status='completed' AND j.status NOT IN ('completed','cancelled','canceled','deleted','failed')""")

async def run(role):
    context=await Context(Settings.load()).open();worker=ApplicationWorker(context);stop=asyncio.Event()
    loop=asyncio.get_running_loop()
    for signum in (signal.SIGTERM,signal.SIGINT):loop.add_signal_handler(signum,stop.set)
    from .source_refresh import RefreshWorker
    refresh=RefreshWorker(context) if role=='refresh' else None
    # A genuine DB/broker readiness endpoint, shared with the source worker.
    from nv_ingest_durable.pipeline_worker import HealthServer
    health=HealthServer(context.pool,worker.transport.client,int(os.environ.get('HEALTH_PORT','8085')))
    await health.start()
    try:
        while not stop.is_set():
            try:
                if role=='refresh':await refresh.tick()
                else:await worker.run_once(role)
            except Exception as error:log.warning('%s_retry error=%s',role,type(error).__name__)
            try:await asyncio.wait_for(stop.wait(),timeout=.5)
            except TimeoutError:pass
    finally:
        await health.stop();await worker.transport.client.aclose();await context.close()

if __name__=='__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run(os.environ.get('APPLICATION_ROLE','admission')))
