"""Typed text extraction inside the existing durable worker commit boundary.

The native NeMo embedding actor remains an internal representation detail. No
VDB or queue writes occur here; the outer worker commits its checkpoint/outbox.
"""
import asyncio
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
from uuid import UUID
from nv_ingest_durable.contracts import StageResult
from collection_control.provenance import verified_file


class TypedExtractionProcessor:
    def __init__(self,pool,pdf,*,root,raw_root=None,embedding=None,stager=None):
        self.pool=pool;self.pdf=pdf;self.root=Path(root);self.raw_root=Path(raw_root) if raw_root is not None else self.root.parent/'raw';self.embedding=embedding;self.stager=stager

    def _dependencies(self):
        if self.embedding is None:
            from nv_ingest_durable.pipeline_worker import _model_params
            from nv_ingest_durable.embedding import RemoteEmbedding
            _,params=_model_params()
            import nv_ingest_durable.embedding as module
            manifest=json.loads(Path(os.environ.get('EMBED_INPUT_CONTRACT', str(Path(module.__file__).with_name('embedding-input-contract.json')))).read_text())
            self.embedding=RemoteEmbedding(params,checkpoint=os.environ['EMBED_CHECKPOINT_DIR'],expected_files=manifest)
        if self.stager is None:
            from nv_ingest_durable.stage_processing import FusedCheckpointStager
            from nv_ingest_durable.elasticsearch_writer import EsReadyCheckpointStager
            from nv_ingest_durable.streams import stream_name
            self.stager=FusedCheckpointStager(EsReadyCheckpointStager(self.root,os.environ['INDEX_CONFIG_DIGEST'],os.environ['INDEX_MODEL_DIGEST'],2048,index_stream=os.environ.get('INDEX_RESULT_STREAM',stream_name('index'))),os.environ['EMBED_CONFIG_DIGEST'],os.environ['EMBED_MODEL_DIGEST'])

    async def process(self,envelope):
        row=await self.pool.fetchrow('SELECT * FROM application.source_revisions WHERE document_id=$1 AND job_id=$2',envelope.document_id,UUID(envelope.job_id))
        if row is None or row['source_kind']=='pdf':return await self.pdf.process(envelope)
        revision=dict(row)
        if revision['source_kind']=='web_image':
            from .image_extraction import NativeImageProcessor
            result=await NativeImageProcessor(root=self.root,raw_root=self.raw_root).process(envelope,revision)
            if revision.get("web_asset_download_id"):
                result=replace(result,downstream=tuple((os.environ.get("INDEX_RESULT_STREAM","rag.agentic.assets.index"),e) for _,e in result.downstream))
            enabled=await self.pool.fetchval("SELECT metadata @> '{\"ingestion_options\":{\"captions_enabled\":true}}'::jsonb FROM public.jobs WHERE job_id=$1",UUID(envelope.job_id))
            if enabled is True and os.environ.get('CAPTION_ENABLED','false').lower()=='true':
                from nv_ingest_durable.pipeline_worker import _model_params,_digest
                from .web_captions import stage_web_caption_handoff
                params,_=_model_params()
                pending,downstream=await stage_web_caption_handoff(envelope,result.artifact,result.downstream,stages=self.root,raw_root=self.raw_root,caption_params=params,model_digest=_digest('CAPTION_MODEL_DIGEST'))
                result=replace(result,artifact=pending,downstream=downstream)
            return result
        units=json.loads(revision['source_units']) if isinstance(revision['source_units'],str) else revision['source_units']
        raw=await asyncio.to_thread(verified_file,self.raw_root,envelope.artifact_uri,revision['raw_sha256'])
        from ingestion.document_formats import inspect_document
        actual=await asyncio.to_thread(inspect_document,revision['filename'],raw,html_version=units['transform']['version'] if revision['source_kind']=='html' else '2',html_input_contract=units['transform'].get('input_contract_sha256'))
        observed={'schema_version':'native-source-units.v1','source_kind':actual['source_kind'],'transform':actual['transform'],'units':actual['units']}
        if observed!=units:raise ValueError('Typed extraction differs from persisted intake source units')
        office=None;embedding_units=units
        if revision['source_kind'] in ('docx','pptx') and revision.get('office_visuals_required'):
            from .office_visuals import OfficeVisualProcessor
            office,visual_units=await OfficeVisualProcessor(root=self.root,raw_root=self.raw_root).extract(raw,revision['filename'],units,raw_uri=revision['raw_uri'])
            import copy
            embedding_units=copy.deepcopy(units);embedding_units['units'].extend(visual_units)
        materialization=None
        if revision.get('web_group_id') and revision['source_kind']=='html':
            group=await self.pool.fetchrow('SELECT * FROM collection_control.web_groups WHERE group_id=$1',revision['web_group_id'])
            if not group:raise ValueError('Page discovery group unavailable')
            group_manifest=json.loads(await asyncio.to_thread(verified_file,self.raw_root,group['manifest_uri'],group['manifest_sha256']))
            if group_manifest.get('schema_version')!='web-page-group.v2':
                from .web_materializer import page_materialization
                materialization,text=await page_materialization(self.pool,revision,self.raw_root,units)
                import copy
                embedding_units=copy.deepcopy(units);embedding_units['units'][0]['text']=text
        self._dependencies()
        frame=(await self._embed_with_reuse(embedding_units,envelope) if revision['source_kind']=='html' and units['transform']['version']=='4' else await asyncio.to_thread(self._embed,embedding_units,envelope))
        artifact,downstream=await self.stager.stage(envelope,frame.to_dict(orient='records'))
        path=Path(__import__('urllib.parse',fromlist=['urlparse']).urlparse(artifact.temporary_uri).path)
        body=json.loads(path.read_text())
        body['native_source_units']=observed
        if office is not None:body['office_visuals']=office
        if materialization is not None:body['web_materialization']=materialization
        encoded=json.dumps(body,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
        await asyncio.to_thread(path.write_bytes,encoded)
        artifact=replace(artifact,digest='sha256:'+hashlib.sha256(encoded).hexdigest(),size_bytes=len(encoded))
        return StageResult(artifact=artifact,downstream=downstream)

    def _embed(self,manifest,envelope):
        import pandas as pd
        rows=[{'text':u['text'],'_content_type':'text','_embed_modality':'text',
               'metadata':{'source':{'source_uri':envelope.artifact_uri},'content_metadata':{'type':'text','source_unit_id':u['unit_id']}}}
              for u in manifest['units'] if u['status']=='complete' and u.get('text','').strip()]
        return self.embedding.process(pd.DataFrame(rows,columns=['text','_content_type','_embed_modality','metadata']))

    async def _embed_with_reuse(self,manifest,envelope):
        import pandas as pd
        from .legal_embedding_cache import LegalEmbeddingCache
        from collection_control.artifacts import canonical
        selected=[u for u in manifest['units'] if u['status']=='complete' and u.get('text','').strip()]
        legal={u['anchors']['legal_content_sha256']:u['text'] for u in selected if u['anchors'].get('section')=='legal_disclosure'}
        contract=hashlib.sha256(canonical({'model_digest':envelope.model_digest,'config_digest':envelope.config_digest,
            'input_contracts':sorted({u['anchors']['input_contract_sha256'] for u in selected if u['anchors'].get('section')=='legal_disclosure'})})).hexdigest()
        cache=LegalEmbeddingCache(self.pool);hits=await cache.get(contract,legal)
        missing=[u for u in selected if u['anchors'].get('legal_content_sha256') not in hits]
        partial={**manifest,'units':missing}
        frame=await asyncio.to_thread(self._embed,partial,envelope)
        output={r['metadata']['content_metadata']['source_unit_id']:r for r in frame.to_dict(orient='records')}
        result=[]
        for unit in selected:
            sha=unit['anchors'].get('legal_content_sha256');key=unit['unit_id']
            if sha is None:result.append(output[key]);continue
            if sha in hits:vector=hits[sha]
            else:
                row=output[key];column=getattr(getattr(self.embedding,'params',None),'output_column','metadata')
                payload=row.get(column);vector=payload.get('embedding') if isinstance(payload,dict) else None
                if vector is None:vector=row.get('metadata',{}).get('embedding')
                vector=await cache.put(contract,unit['text'],vector)
            # Only the vector is shared. Source/ACL/citation metadata is never
            # copied from another row or an earlier capture.
            result.append({'text':unit['text'],'_content_type':'text','_embed_modality':'text',
                'metadata':{'source':{'source_uri':envelope.artifact_uri},
                    'content_metadata':{'type':'text','source_unit_id':key},'embedding':vector,
                    'legal_embedding_reuse':{'content_sha256':sha,'embedding_contract_sha256':contract,'cache_hit':sha in hits}}})
        return pd.DataFrame(result,columns=['text','_content_type','_embed_modality','metadata'])

    async def process_batch(self,envelopes):
        return tuple([await self.process(e) for e in envelopes])
