"""Retain image assets and stage caption envelopes without invoking a model."""
from .streams import stream_name
import base64
import copy
from dataclasses import replace
import hashlib
import json
import math
from numbers import Integral
import os
from pathlib import Path
from urllib.parse import urlparse,unquote
from uuid import uuid4,uuid5,NAMESPACE_URL


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def retain(root, data, suffix):
    root=Path(root).resolve();identity=digest(data)
    folder=root/'caption-assets'/identity[:2];folder.mkdir(parents=True,exist_ok=True)
    target=folder/(identity+suffix)
    if target.exists():
        if target.is_symlink() or digest(target.read_bytes())!=identity:raise ValueError('Retained asset identity conflict')
        return target.as_uri(),identity
    temporary=folder/('.'+uuid4().hex)
    try:
        with temporary.open('xb') as handle:handle.write(data);handle.flush();os.fsync(handle.fileno())
        # Link without overwriting an immutable asset created concurrently.
        try:os.link(temporary,target)
        except FileExistsError:
            if target.is_symlink() or digest(target.read_bytes())!=identity:raise ValueError('Retained asset identity conflict')
        fd=os.open(folder,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)
    finally:temporary.unlink(missing_ok=True)
    return target.as_uri(),identity


def verified_file(root, uri, expected):
    parsed=urlparse(uri)
    if parsed.scheme!='file' or parsed.netloc not in ('','localhost'):raise ValueError('Retained caption asset must be a local file')
    path=Path(unquote(parsed.path))
    if path.is_symlink() or not path.resolve().is_relative_to(Path(root).resolve()):raise ValueError('Caption asset escapes trusted root')
    data=path.read_bytes()
    if digest(data)!=expected:raise ValueError('Caption asset checksum mismatch')
    return data


class DeferredCaptionAssets:
    def __init__(self,root,params,model_digest):
        self.root=Path(root);self.params=params;self.model_digest=model_digest

    def process(self, frame):
        from .admission import require_successful_upstream
        from nemo_retriever.operators.extract.caption.caption import _image_meets_min_size
        from nemo_retriever.operators.extract.ocr.ocr import _crop_b64_image_by_norm_bbox
        require_successful_upstream(frame)
        output=frame.copy(deep=True);all_assets=[]
        for index,row in frame.iterrows():
            candidates=[]
            images=row.get('images')
            if isinstance(images,list):
                for position,item in enumerate(images):
                    if isinstance(item,dict) and not item.get('text') and item.get('image_b64'):
                        candidates.append(('image',position,item.get('bbox_xyxy_norm'),item['image_b64'],None))
            page=row.get('page_image');page_b64=page.get('image_b64') if isinstance(page,dict) else None
            infographics=row.get('infographic')
            if self.params.caption_infographics and page_b64 and isinstance(infographics,list):
                for position,item in enumerate(infographics):
                    if isinstance(item,dict) and not item.get('caption') and item.get('bbox_xyxy_norm'):
                        try:
                            crop,_=_crop_b64_image_by_norm_bbox(page_b64,bbox_xyxy_norm=item['bbox_xyxy_norm'])
                            if crop:candidates.append(('infographic',position,item['bbox_xyxy_norm'],crop,None))
                        except Exception:
                            candidates.append(('infographic',position,item['bbox_xyxy_norm'],page_b64,'invalid_crop'))
            for kind,position,bbox,encoded,validation_error in candidates:
                try:raw=base64.b64decode(encoded,validate=True)
                except Exception:
                    raw=str(encoded).encode();validation_error='invalid_encoding'
                # Native size exclusions are retained; malformed images are retained
                # as jobs so the worker can terminally classify them without looping.
                try:
                    from PIL import Image
                    from io import BytesIO
                    with Image.open(BytesIO(raw)) as image:image.verify()
                    if not validation_error and not _image_meets_min_size(encoded):continue
                except Exception:pass
                uri,sha=retain(self.root,raw,'.image')
                metadata=row.get('metadata') if isinstance(row.get('metadata'),dict) else {}
                page_number=row.get('_page_number',row.get('page_number',metadata.get('page_number',1)))
                asset={'asset_uri':uri,'asset_sha256':sha,'asset_size_bytes':len(raw),'page_number':int(page_number or 1),'region_kind':kind,'region_index':position,'bbox':list(bbox) if bbox is not None else None,'model_name':self.params.model_name,'model_digest':self.model_digest,'caption_config':{key:getattr(self.params,key,None) for key in ['temperature','max_tokens','top_p','prompt','system_prompt','extra_body','context_text_max_chars']},'asset_validation_error':validation_error,'source_metadata':copy.deepcopy(metadata)}
                all_assets.append(asset)
        output.attrs['caption_assets']=all_assets
        return output


def reshape_source_content(frame, *, caption_enabled):
    from nemo_retriever.common.modality.content_transforms import explode_content_to_rows,_CONTENT_COLUMNS
    from nemo_retriever.common.schemas.embedding import embedding_text_input
    from .admission import require_successful_upstream
    require_successful_upstream(frame)
    assets=frame.attrs.get('caption_assets',[])
    observed=[]
    for _,row in frame.iterrows():
        if isinstance(row.get('error'),(dict,str)) and row.get('error'):raise ValueError('Native page failed before reshape')
        native=row.get('metadata') if isinstance(row.get('metadata'),dict) else {}
        content=native.get('content_metadata') if isinstance(native.get('content_metadata'),dict) else {}
        values=[mapping.get(key) for mapping,keys in ((row,('_page_number','page_number')),(native,('page_number',)),(content,('page_number',))) for key in keys]
        values=[value for value in values if value is not None and not (isinstance(value,float) and math.isnan(value))]
        if not values or any(isinstance(value,bool) or not isinstance(value,Integral) or value<1 for value in values) or len(set(values))!=1:
            raise ValueError('Native extraction requires one explicit positive page identity')
        page=int(values[0])
        if page in observed:raise ValueError('Native extraction emitted duplicate page identities')
        observed.append(page)
    completions={'schema_version':'native-page-completions.v1','pages':[{'page_number':page,'status':'complete'} for page in sorted(observed)]}
    result=explode_content_to_rows(frame,content_columns=_CONTENT_COLUMNS+(("images",) if caption_enabled else ()),modality='text')
    # Preserve the native extraction flags and exact page/region anchors on
    # every exploded text row, including OCR, tables and visual annotations.
    metadata=[]
    for _,row in result.iterrows():
        retained=copy.deepcopy(row.get('metadata')) if isinstance(row.get('metadata'),dict) else {}
        content=dict(retained.get('content_metadata') or {})
        for field in ('has_text','needs_ocr_for_text'):
            if field in retained:content.setdefault(field,retained[field])
            elif field in row:content.setdefault(field,row[field])
        page=row.get('_page_number',row.get('page_number',retained.get('page_number')))
        if page is not None:content.setdefault('page_number',page)
        bbox=row.get('_bbox_xyxy_norm')
        if isinstance(bbox,(list,tuple)):content['bbox']=list(bbox)
        retained['content_metadata']=content;metadata.append(retained)
    result['metadata']=metadata
    keep=[isinstance(embedding_text_input(row),str) and bool(embedding_text_input(row).strip()) for _,row in result.iterrows()]
    result=result.loc[keep].copy()
    result.attrs['native_page_completions']=completions
    result.attrs['caption_assets']=assets
    return result


class CaptionHandoffStager:
    def __init__(self,delegate,root):self.delegate=delegate;self.root=Path(root)

    async def stage(self,envelope,result):
        from .contracts import Stage,StageEnvelope,derive_idempotency_key
        assets=result.attrs.get('caption_assets',[]) if hasattr(result,'attrs') else []
        page_completions=copy.deepcopy(result.attrs.get('native_page_completions')) if hasattr(result,'attrs') else None
        if os.environ.get('INGEST_STREAM_PREFIX')=='rag.agentic' and not page_completions:raise ValueError('Application source checkpoint requires observed native page completion evidence')
        pending,downstream=await self.delegate.stage(envelope,result)
        path=Path(unquote(urlparse(pending.temporary_uri).path))
        checkpoint=json.loads(path.read_text());jobs=[]
        if page_completions is not None:checkpoint['native_page_completions']=page_completions
        for asset in assets:
            association={**asset,'parent_job_id':envelope.job_id,'document_id':envelope.document_id,'parent_stage_key':envelope.idempotency_key,'pipeline_version':envelope.pipeline_version,'parent_config_digest':envelope.config_digest,'parent_model_digest':envelope.model_digest}
            key=derive_idempotency_key(source_content_hash=envelope.document_id,logical_unit_id=digest(canonical(association)),stage=Stage.CAPTION,pipeline_version=envelope.pipeline_version,stage_config=asset['caption_config'],model_digest=asset['model_digest'])
            uri,sha=retain(self.root,canonical(association),'.manifest.json')
            caption=StageEnvelope(created_at=envelope.created_at,priority=envelope.priority,trace_id=envelope.trace_id,message_id=str(uuid5(NAMESPACE_URL,key)),job_id=envelope.job_id,document_id=envelope.document_id,stage=Stage.CAPTION,artifact_uri=uri,idempotency_key=key,pipeline_version=envelope.pipeline_version,config_digest='sha256:'+digest(canonical(asset['caption_config'])),model_digest=asset['model_digest'])
            jobs.append({'stage_key':key,'manifest_uri':uri,'manifest_sha256':sha,'asset_sha256':asset['asset_sha256'],'envelope':caption.to_dict()})
            downstream+= ((stream_name('caption.pending'),caption),)
        checkpoint['caption_jobs']=jobs;checkpoint['caption_status']='pending' if jobs else 'not_required'
        data=canonical(checkpoint);path.write_bytes(data)
        return replace(pending,digest='sha256:'+digest(data),size_bytes=len(data),metadata={**pending.metadata,'pending_caption_assets':len(jobs),'caption_jobs_manifest':True}),downstream
