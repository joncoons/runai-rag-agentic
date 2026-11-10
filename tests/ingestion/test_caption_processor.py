import base64
from io import BytesIO
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from PIL import Image
from nemo_retriever.common.params import CaptionParams
from nv_ingest_durable.caption_worker import CaptionProcessor
from nv_ingest_durable.caption_repository import PermanentImageError
from nv_ingest_durable.contracts import Stage,StageEnvelope
from nv_ingest_durable.deferred_caption import retain,canonical,digest


class FakeEmbedding:
    def process(self,frame):
        result=frame.copy();result['text_embeddings_1b_v2']=[{'embedding':[1.0]+[0.0]*2047} for _ in range(len(frame))]
        return result


class CaptionProcessorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.model='sha256:'+'a'*64
        self.params=CaptionParams(endpoint_url='http://127.0.0.1:1/v1/chat/completions',model_name='test-model',temperature=.9,max_tokens=2048)
        self.processor=CaptionProcessor(self.root,self.params,FakeEmbedding(),self.model)

    def envelope(self,*,corrupt=False,config_override=None,bbox=None):
        stream=BytesIO();Image.new('RGB',(64,64),'blue').save(stream,format='PNG')
        raw=b'corrupt' if corrupt else stream.getvalue()
        uri,sha=retain(self.root,raw,'.image')
        config={'temperature':0,'max_tokens':512}
        manifest={'asset_uri':uri,'asset_sha256':sha,'model_name':'test-model','model_digest':self.model,'document_id':'document','parent_job_id':'job','parent_stage_key':'parent-stage','page_number':1,'bbox':bbox,'source_metadata':{'fractional_geometry':bbox} if bbox is not None else {},'caption_config':config}
        manifest_uri,_=retain(self.root,canonical(manifest),'.manifest.json')
        return StageEnvelope(job_id='job',document_id='document',stage=Stage.CAPTION,artifact_uri=manifest_uri,idempotency_key='sha256:'+'b'*64,pipeline_version='async-v1',config_digest=config_override or 'sha256:'+digest(canonical(config)),model_digest=self.model)

    async def test_retained_settings_override_new_worker_generation_settings(self):
        seen=[]
        def caption(actor,frame):
            seen.append((actor._params.temperature,actor._params.max_tokens))
            result=frame.copy();result.iloc[0]['images'][0]['text']='Generated description';return result
        with patch('nemo_retriever.operators.extract.caption.caption.CaptionCPUActor.process',caption):
            result=await self.processor.process(self.envelope())
        self.assertEqual(seen,[(0,512)])
        self.assertEqual(result.downstream[0][0],'ingest.caption.results')

    async def test_corrupt_image_is_terminal_before_any_model_call(self):
        with patch('nemo_retriever.operators.extract.caption.caption.CaptionCPUActor.process') as model:
            with self.assertRaises(PermanentImageError):await self.processor.process(self.envelope(corrupt=True))
            model.assert_not_called()

    async def test_configuration_tampering_is_terminal_before_inference(self):
        with patch('nemo_retriever.operators.extract.caption.caption.CaptionCPUActor.process') as model:
            with self.assertRaises(PermanentImageError):await self.processor.process(self.envelope(config_override='sha256:'+'c'*64))
            model.assert_not_called()

    async def test_empty_response_does_not_produce_caption_artifact(self):
        with patch('nemo_retriever.operators.extract.caption.caption.CaptionCPUActor.process',side_effect=lambda frame:frame):
            with self.assertRaises(RuntimeError):await self.processor.process(self.envelope())
        self.assertFalse((self.root/'caption').exists())

    async def test_native_fractional_bbox_and_vector_precision_survive_caption_checkpoint(self):
        import json
        import math
        bbox=[0.0,0.0,0.9947954416275024,0.9737284779548645]
        vector=[1/math.sqrt(2),1/math.sqrt(2)]+[0.0]*2046
        class PreciseEmbedding:
            def process(self,frame):
                result=frame.copy();result['text_embeddings_1b_v2']=[{'embedding':vector} for _ in range(len(frame))];return result
        self.processor.embedding=PreciseEmbedding()
        def caption(actor,frame):
            result=frame.copy();result.iloc[0]['images'][0]['text']='Generated precise geometry fixture';return result
        with patch('nemo_retriever.operators.extract.caption.caption.CaptionCPUActor.process',caption):
            result=await self.processor.process(self.envelope(bbox=bbox))
        checkpoint=json.loads(Path(result.artifact.temporary_uri.removeprefix('file://')).read_text())
        document=checkpoint['documents'][0]
        self.assertEqual(document['metadata']['content_metadata']['bbox'],bbox)
        self.assertEqual(document['metadata']['caption_provenance']['source_metadata']['fractional_geometry'],bbox)
        self.assertEqual(document['vector'],vector)
