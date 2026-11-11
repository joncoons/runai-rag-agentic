"""Extraction routes keep outer commit and notification artifact identities."""
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock
from uuid import uuid4
from application.typed_extraction import TypedExtractionProcessor
from nv_ingest_durable.contracts import StageEnvelope,Stage
from nv_ingest_durable.artifacts import JsonResultStager
from ingestion.document_formats import inspect_document

class TypedExtractionTests(unittest.IsolatedAsyncioTestCase):
    async def test_text_uses_actual_units_and_preserves_pending_final_identity(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);(root/'raw').mkdir();raw=b'first line\nlast line\n';p=root/'raw'/'source.txt';p.write_bytes(raw)
            inspected=inspect_document('source.txt',raw)
            units={'schema_version':'native-source-units.v1','source_kind':inspected['source_kind'],'transform':inspected['transform'],'units':inspected['units']}
            row={'source_kind':'txt','source_units':units,'raw_sha256':hashlib.sha256(raw).hexdigest(),'filename':'source.txt'}
            pool=SimpleNamespace(fetchrow=AsyncMock(return_value=row))
            envelope=StageEnvelope(job_id=str(uuid4()),document_id='source',stage=Stage.EXTRACTION,artifact_uri=p.as_uri(),idempotency_key='sha256:'+'a'*64,pipeline_version='test',config_digest='sha256:'+'b'*64,model_digest='sha256:'+'c'*64)
            class Embedding:
                def process(self,frame):return frame
            class Stager:
                async def stage(self,envelope,rows):
                    a,_=await JsonResultStager(root).stage(envelope,{'schema_version':'1.0','payload_format':'es_docs','documents':rows})
                    return a,(('index',SimpleNamespace(artifact_uri=a.final_uri)),)
            processor=TypedExtractionProcessor(pool,SimpleNamespace(process=AsyncMock()),root=root/'stages',embedding=Embedding(),stager=Stager())
            result=await processor.process(envelope)
            body=json.loads(Path(__import__('urllib.parse',fromlist=['urlparse']).urlparse(result.artifact.temporary_uri).path).read_text())
            self.assertEqual(body['native_source_units'],units)
            self.assertEqual(result.downstream[0][1].artifact_uri,result.artifact.final_uri)
            self.assertEqual(result.artifact.digest,'sha256:'+hashlib.sha256(Path(__import__('urllib.parse',fromlist=['urlparse']).urlparse(result.artifact.temporary_uri).path).read_bytes()).hexdigest())
            self.assertTrue(all(r['metadata']['content_metadata']['source_unit_id'] for r in body['documents']))
            self.assertTrue(all('page_number' not in r['metadata']['content_metadata'] for r in body['documents']))

    async def test_pdf_remains_existing_pipeline(self):
        pdf=SimpleNamespace(process=AsyncMock(return_value='pdf'))
        pool=SimpleNamespace(fetchrow=AsyncMock(return_value={'source_kind':'pdf'}))
        env=SimpleNamespace(document_id='pdf',job_id=str(uuid4()))
        self.assertEqual(await TypedExtractionProcessor(pool,pdf,root='/tmp').process(env),'pdf')
        pdf.process.assert_awaited_once_with(env)

    async def test_empty_typed_source_still_notifies_admission_under_application_prefix(self):
        from unittest.mock import patch
        from nv_ingest_durable.stage_processing import FusedCheckpointStager
        from nv_ingest_durable.elasticsearch_writer import EsReadyCheckpointStager
        with tempfile.TemporaryDirectory() as folder:
            e=StageEnvelope(job_id=str(uuid4()),document_id='empty',stage=Stage.EXTRACTION,artifact_uri='file:///empty.txt',idempotency_key='sha256:'+'a'*64,pipeline_version='test',config_digest='sha256:'+'b'*64,model_digest='sha256:'+'c'*64)
            stager=FusedCheckpointStager(EsReadyCheckpointStager(Path(folder),'sha256:'+'b'*64,'sha256:'+'c'*64,index_stream='rag.agentic.index'),'sha256:'+'b'*64,'sha256:'+'c'*64)
            with patch.dict('os.environ',{'INGEST_STREAM_PREFIX':'rag.agentic'}):
                artifact,downstream=await stager.stage(e,[])
            self.assertEqual(downstream[0][0],'rag.agentic.index')
            self.assertEqual(downstream[0][1].artifact_uri,artifact.final_uri)

    async def test_private_html_never_uses_unscoped_legal_text_cache(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);(root/'raw').mkdir();raw=b'<main><h1>Private policy</h1><p>Private facts.</p><section id="disclaimer">Private legal terms.</section></main>'
            path=root/'raw'/'source.html';path.write_bytes(raw);inspected=inspect_document('source.html',raw)
            units={'schema_version':'native-source-units.v1','source_kind':inspected['source_kind'],'transform':inspected['transform'],'units':inspected['units']}
            row={'source_kind':'html','source_units':units,'raw_sha256':hashlib.sha256(raw).hexdigest(),'filename':'source.html'}
            pool=SimpleNamespace(fetchrow=AsyncMock(return_value=row))
            envelope=StageEnvelope(job_id=str(uuid4()),document_id='private-html',stage=Stage.EXTRACTION,artifact_uri=path.as_uri(),idempotency_key='sha256:'+'a'*64,pipeline_version='test',config_digest='sha256:'+'b'*64,model_digest='sha256:'+'c'*64)
            class Embedding:
                def process(self,frame):return frame
            class Stager:
                async def stage(self,envelope,rows):return (await JsonResultStager(root).stage(envelope,{'schema_version':'1.0','payload_format':'es_docs','documents':rows}))[0],()
            processor=TypedExtractionProcessor(pool,None,root=root/'stages',embedding=Embedding(),stager=Stager(),reuse_legal=False)
            processor._embed_with_reuse=AsyncMock(side_effect=AssertionError('Private content entered unscoped cache'))
            await processor.process(envelope);processor._embed_with_reuse.assert_not_awaited()
