"""Atomically hand a captured deferred asset to its independent native worker."""
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID
from collection_control.artifacts import canonical
from collection_control.pinned_connection import PinnedPool
from collection_control.repository import Conflict,decoded
from nv_ingest_durable.deferred_caption import retain

async def submit_native_asset(connection,task,capture,acquisition,*,settings):
    from .intake import submit
    c=connection
    reuse=bool(task.get('child_source_revision_id'))
    if reuse and task['captions_enabled']:
        metadata=await c.fetchval('SELECT metadata FROM public.jobs WHERE job_id=$1',UUID(str(task['child_job_id'])))
        reuse=decoded(metadata).get('ingestion_options',{}).get('captions_enabled') is True
    if reuse:
        revision=await c.fetchrow('SELECT * FROM application.source_revisions WHERE source_revision_id=$1',task['child_source_revision_id'])
        if not revision or revision['raw_sha256']!=capture.raw_sha256.removeprefix('sha256:'):raise Conflict('Retained native child identity changed')
        job=str(revision['job_id'])
    else:
        job=await submit(SimpleNamespace(pool=PinnedPool(c),settings=settings),task['collection_id'],[(capture.filename,capture.raw_bytes)],
            captions_enabled=task['captions_enabled'],_web_captures={capture.filename:{'canonical_uri':capture.requested_uri,'role':capture.kind,'media_type':capture.media_type}},_streamed=True,_inspections=task.get('_native_prepared'))
        revision=await c.fetchrow('SELECT * FROM application.source_revisions WHERE job_id=$1',UUID(job))
    stage=await c.fetchrow("SELECT * FROM public.stage_units WHERE document_id=$1 AND stage='extraction'",revision['document_id'])
    envelope=decoded(stage['input_envelope'])
    parents={}
    for ref in task['references']:
        parent={'group_id':str(ref['parent_group_id']),'document_id':ref['parent_document_id'],
            'source_revision_id':ref['parent_source_revision_id'],'canonical_uri':ref['parent_canonical_uri']}
        parents[parent['source_revision_id']]=parent
    asset={k:revision[k] for k in ('document_id','source_revision_id','canonical_uri','raw_uri','raw_sha256','media_type')}
    asset.update(kind=capture.kind,model_digest=envelope['model_digest'],config_digest=envelope['config_digest'],
        requested_uri=capture.requested_uri,final_uri=capture.final_uri,http=capture.evidence)
    if capture.kind=='image':asset['extraction_stage_key']=stage['idempotency_key']
    manifest={'schema_version':'web-asset-publication.v2','collection_id':task['collection_id'],'request_id':str(task['download_id']),
        'acquisition':acquisition,'parent_pages':[parents[k] for k in sorted(parents)],'asset':asset}
    uri,sha=retain(settings.raw,canonical(manifest),'.web-asset-publication.json')
    if reuse and stage['status']=='committed':
        # A later parent attachment needs fresh admission, not another extraction.
        from uuid import uuid5,NAMESPACE_URL
        from nv_ingest_durable.contracts import Stage,StageEnvelope
        from nv_ingest_durable.stage_processing import downstream_envelope
        original=StageEnvelope.from_stream_fields(envelope)
        notification=downstream_envelope(original,stage=Stage.INDEX,artifact_uri=stage['artifact_uri'],config_digest=original.config_digest,model_digest=original.model_digest)
        await c.execute('INSERT INTO public.outbox_events(event_id,idempotency_key,target_stream,envelope) VALUES($1,$2,$3,$4::jsonb) ON CONFLICT(event_id) DO NOTHING',uuid5(NAMESPACE_URL,'web-association:'+sha),original.idempotency_key,'rag.agentic.assets.index',json.dumps(notification.to_dict()))

    return {'job_id':job,'source_revision_id':revision['source_revision_id'],'publication_manifest':{'uri':uri,'sha256':sha}}


class NativeAssetSubmitter:
    """CPU preflight occurs before the acquisition repository locks a collection."""
    def __init__(self,settings):self.settings=settings
    async def prepare(self,task,capture):
        import asyncio,hashlib
        from .intake import inspect_upload,inspect_linked_image
        if capture.kind=='image':inspection=await asyncio.to_thread(inspect_linked_image,capture.raw_bytes,capture.media_type)
        else:inspection=await asyncio.to_thread(inspect_upload,capture.filename,capture.raw_bytes,captions_enabled=False)
        return {capture.filename:{'raw_sha256':hashlib.sha256(capture.raw_bytes).hexdigest(),'inspection':inspection}}
    async def __call__(self,c,task,capture,acquisition):
        return await submit_native_asset(c,task,capture,acquisition,settings=self.settings)
