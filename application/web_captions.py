"""Optional web-image CAPTION handoff and same-page embedding adapter."""
import asyncio
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
from urllib.parse import unquote,urlparse
from uuid import uuid5,NAMESPACE_URL

from collection_control.artifacts import canonical
from collection_control.provenance import verified_file
from nv_ingest_durable.contracts import Stage,StageEnvelope,derive_idempotency_key
from nv_ingest_durable.deferred_caption import retain
from nv_ingest_durable.streams import stream_name


async def stage_web_caption_handoff(envelope,artifact,downstream,*,stages,raw_root,caption_params,model_digest):
    """Attach a standard per-image job to the native extraction's atomic commit.

    Caller validates the persisted job opt-in and operator availability. The
    existing CaptionManifestRepository revalidates opt-in during the commit.
    """
    path=Path(unquote(urlparse(artifact.temporary_uri).path))
    body=json.loads(path.read_text())
    if body['schema_version']!='native-web-image-evidence.v1' or body['document_id']!=envelope.document_id or body['status']!='complete':raise ValueError('Web caption handoff requires complete native image evidence')
    pixels=await asyncio.to_thread(verified_file,raw_root,body['normalized']['uri'],body['normalized']['sha256'])
    # Match the installed caption library; acquisition and native extraction
    # retain small images, but they must never enter the caption retry queue.
    import base64
    from io import BytesIO
    from PIL import Image
    from nemo_retriever.operators.extract.caption.caption import _image_meets_min_size, _MIN_IMAGE_DIMENSION
    with Image.open(BytesIO(pixels)) as image:
        width,height=image.size
        image.verify()
    if not _image_meets_min_size(base64.b64encode(pixels).decode('ascii')):
        body['caption_jobs']=[]
        body['caption_status']='skipped'
        body['caption_skip']={'reason':'below_caption_library_minimum','width':width,'height':height,
            'minimum_width':_MIN_IMAGE_DIMENSION,'minimum_height':_MIN_IMAGE_DIMENSION,
            'policy':'nemo_retriever.operators.extract.caption.caption._image_meets_min_size',
            'normalized_sha256':body['normalized']['sha256']}
        encoded=canonical(body);await asyncio.to_thread(path.write_bytes,encoded)
        return replace(artifact,digest='sha256:'+hashlib.sha256(encoded).hexdigest(),size_bytes=len(encoded),
            metadata={**artifact.metadata,'pending_caption_assets':0,'caption_jobs_manifest':True,'caption_status':'skipped'}),tuple(downstream)
    uri,asset_sha=await asyncio.to_thread(retain,stages,pixels,'.image')
    config={key:getattr(caption_params,key,None) for key in ('temperature','max_tokens','top_p','prompt','system_prompt','extra_body','context_text_max_chars')}
    association={'asset_uri':uri,'asset_sha256':asset_sha,'asset_size_bytes':len(pixels),'page_number':1,'region_kind':'image','region_index':0,'bbox':[0.0,0.0,1.0,1.0],'model_name':caption_params.model_name,'model_digest':model_digest,'caption_config':config,'asset_validation_error':None,
        'source_metadata':{'web_image_source_revision_id':body['source_revision_id'],'original_raw_sha256':body['raw_sha256'],'normalized_sha256':body['normalized']['sha256']},
        'parent_job_id':envelope.job_id,'document_id':envelope.document_id,'parent_stage_key':envelope.idempotency_key,'pipeline_version':envelope.pipeline_version,'parent_config_digest':envelope.config_digest,'parent_model_digest':envelope.model_digest}
    key=derive_idempotency_key(source_content_hash=envelope.document_id,logical_unit_id=hashlib.sha256(canonical(association)).hexdigest(),stage=Stage.CAPTION,pipeline_version=envelope.pipeline_version,stage_config=config,model_digest=model_digest)
    manifest_uri,manifest_sha=await asyncio.to_thread(retain,stages,canonical(association),'.manifest.json')
    child=StageEnvelope(created_at=envelope.created_at,priority=envelope.priority,trace_id=envelope.trace_id,message_id=str(uuid5(NAMESPACE_URL,key)),job_id=envelope.job_id,document_id=envelope.document_id,stage=Stage.CAPTION,artifact_uri=manifest_uri,idempotency_key=key,pipeline_version=envelope.pipeline_version,config_digest='sha256:'+hashlib.sha256(canonical(config)).hexdigest(),model_digest=model_digest)
    body['caption_jobs']=[{'stage_key':key,'manifest_uri':manifest_uri,'manifest_sha256':manifest_sha,'asset_sha256':asset_sha,'envelope':child.to_dict()}];body['caption_status']='pending'
    encoded=canonical(body);await asyncio.to_thread(path.write_bytes,encoded)
    return replace(artifact,digest='sha256:'+hashlib.sha256(encoded).hexdigest(),size_bytes=len(encoded),metadata={**artifact.metadata,'pending_caption_assets':1,'caption_jobs_manifest':True}),tuple(downstream)+((stream_name('caption.pending'),child),)


class ParentEmbedding:
    def __init__(self):self.embedding=None
    def __call__(self,text):
        if self.embedding is None:
            from nv_ingest_durable.pipeline_worker import _model_params
            from nv_ingest_durable.embedding import RemoteEmbedding
            import nv_ingest_durable.embedding as module
            _,params=_model_params()
            self.embedding=RemoteEmbedding(params,checkpoint=os.environ['EMBED_CHECKPOINT_DIR'],expected_files=json.loads(Path(module.__file__).with_name('embedding-input-contract.json').read_text()))
        import pandas as pd
        rows=self.embedding.process(pd.DataFrame([{'text':text,'_content_type':'text','_embed_modality':'text','metadata':{'content_metadata':{'type':'text'}}}]))
        row=rows.to_dict(orient='records')[0]
        # Reuse the exact native vector extraction used by foreground checkpoints.
        from nv_ingest_durable.elasticsearch_writer import compact_checkpoint
        return compact_checkpoint([row],'web-caption-parent')['documents'][0]['vector']
