"""Native caption eligibility applied before a durable web-caption job exists."""
import hashlib
from io import BytesIO
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from uuid import uuid4
from urllib.parse import urlparse
from PIL import Image
from nv_ingest_durable.artifacts import JsonResultStager
from nv_ingest_durable.contracts import Stage, StageEnvelope
from application.web_captions import stage_web_caption_handoff

class WebCaptionMinimumTests(unittest.IsolatedAsyncioTestCase):
    async def handoff(self, size):
        root=Path(self.folder.name);raw_root=root/'raw';raw_root.mkdir(exist_ok=True)
        buffer=BytesIO();Image.new('RGB',size).save(buffer,format='PNG');pixels=buffer.getvalue()
        image=raw_root/'image.png';image.write_bytes(pixels)
        envelope=StageEnvelope(job_id=str(uuid4()),document_id='image-document',stage=Stage.EXTRACTION,
            artifact_uri=image.as_uri(),idempotency_key='sha256:'+'a'*64,pipeline_version='test',config_digest='b'*64,model_digest='c'*64)
        evidence={'schema_version':'native-web-image-evidence.v1','status':'complete','document_id':envelope.document_id,
            'source_revision_id':'image-revision','raw_uri':image.as_uri(),'raw_sha256':hashlib.sha256(pixels).hexdigest(),
            'normalized':{'uri':image.as_uri(),'sha256':hashlib.sha256(pixels).hexdigest()},'text':'Retained native OCR'}
        artifact,downstream=await JsonResultStager(root/'stages').stage(envelope,evidence)
        original_downstream=tuple(downstream)
        result,events=await stage_web_caption_handoff(envelope,artifact,downstream,stages=root/'stages',raw_root=raw_root,
            caption_params=SimpleNamespace(model_name='caption-model'),model_digest='d'*64)
        body=json.loads(Path(urlparse(result.temporary_uri).path).read_bytes())
        self.assertEqual(result.digest,'sha256:'+hashlib.sha256(Path(urlparse(result.temporary_uri).path).read_bytes()).hexdigest())
        self.assertEqual(body['text'],'Retained native OCR');self.assertEqual(image.read_bytes(),pixels)
        return body,events,original_downstream,result

    async def asyncSetUp(self):
        self.folder=tempfile.TemporaryDirectory();self.addCleanup(self.folder.cleanup)

    async def test_either_dimension_below_native_minimum_is_a_recorded_skip(self):
        for size in ((31,64),(64,31),(1,1)):
            with self.subTest(size=size):
                body,events,original,result=await self.handoff(size)
                self.assertEqual(body['caption_status'],'skipped')
                self.assertEqual(body['caption_jobs'],[])
                self.assertEqual(events,original)
                self.assertEqual(body['caption_skip']['reason'],'below_caption_library_minimum')
                self.assertEqual(body['caption_skip']['width'],size[0]);self.assertEqual(body['caption_skip']['height'],size[1])
                self.assertEqual(body['caption_skip']['minimum_width'],32);self.assertEqual(body['caption_skip']['minimum_height'],32)
                self.assertEqual(result.metadata['pending_caption_assets'],0)

    async def test_exact_native_boundary_queues_one_caption(self):
        body,events,original,result=await self.handoff((32,32))
        self.assertEqual(body['caption_status'],'pending');self.assertEqual(len(body['caption_jobs']),1)
        self.assertEqual(len(events),len(original)+1);self.assertEqual(result.metadata['pending_caption_assets'],1)
