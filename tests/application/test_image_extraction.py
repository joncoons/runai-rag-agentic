"""Inline image native evidence has no embedding or VDB side effects."""
import hashlib
from io import BytesIO
import json
from pathlib import Path
import tempfile
import unittest
from uuid import uuid4
from urllib.parse import urlparse
from PIL import Image
from application.image_extraction import NativeImageProcessor, image_contract
from nv_ingest_durable.contracts import Stage, StageEnvelope

class NativeImageTests(unittest.IsolatedAsyncioTestCase):
    def raw(self,mode="RGB",size=(32,24)):
        buf=BytesIO();Image.new(mode,size).save(buf,format="PNG");return buf.getvalue()

    def fixture(self,folder,raw):
        root=Path(folder);(root/"raw").mkdir(exist_ok=True)
        p=root/"raw"/"original.image";p.write_bytes(raw)
        model,config=image_contract()
        env=StageEnvelope(job_id=str(uuid4()),document_id="image-document",stage=Stage.EXTRACTION,artifact_uri=p.as_uri(),idempotency_key="sha256:"+"a"*64,pipeline_version="test",config_digest=config,model_digest=model)
        rev={"document_id":env.document_id,"source_revision_id":"image-revision","raw_uri":p.as_uri(),"raw_sha256":hashlib.sha256(raw).hexdigest(),"canonical_uri":"https://example.test/asset.png"}
        return root,env,rev

    def read(self,result):return json.loads(Path(urlparse(result.artifact.temporary_uri).path).read_text())

    async def test_retains_evidence_and_only_extraction_checkpoint(self):
        with tempfile.TemporaryDirectory() as folder:
            root,env,rev=self.fixture(folder,self.raw("RGBA"));paths=[]
            def runner(path):
                paths.append(path)
                return [{"page_number":1,"text":"grounded OCR","metadata":{"error":None},"page_image":{"image_b64":"do not persist"},"page_elements_v3":{"detections":[{"label":0,"label_name":"table","score":0.95,"bbox_xyxy_norm":[0,0,1,1]}]},"table":[{"text":"| A | B |","bbox_xyxy_norm":[0,0,1,1]}],"ocr":{"error":None}}]
            result=await NativeImageProcessor(root=root/"stages",runner=runner).process(env,rev);body=self.read(result)
            self.assertEqual(body["status"],"complete")
            self.assertEqual(body["raw_sha256"],rev["raw_sha256"])
            self.assertEqual(body["source_revision_id"],rev["source_revision_id"])
            self.assertEqual(body["text"],"grounded OCR\n\n| A | B |")
            self.assertEqual(body["normalized"]["width"],32)
            self.assertEqual(body["normalized"]["transform"]["original_mode"],"RGBA")
            self.assertTrue(paths[0].endswith(".png"))
            self.assertEqual(body["native"]["detections"][0]["score"],0.95)
            self.assertNotIn("image_b64",json.dumps(body));self.assertNotIn("page_number",body)
            self.assertEqual(body["native"]["coordinate_frame"],"normalized_image")
            self.assertEqual(result.downstream[0][1].stage,Stage.INDEX)
            self.assertEqual(result.downstream[0][1].artifact_uri,result.artifact.final_uri)
            self.assertIn("/extraction/",result.artifact.final_uri);self.assertNotIn("documents",body)

    async def test_native_legacy_error_is_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            root,env,rev=self.fixture(folder,self.raw())
            with self.assertRaisesRegex(ValueError,"Native image extraction failed"):
                await NativeImageProcessor(root=root/"stages",runner=lambda p:[{"metadata":{"error":"unsupported"}}]).process(env,rev)

    async def test_multiframe_rejected_before_native(self):
        buf=BytesIO();Image.new("RGB",(8,8)).save(buf,format="GIF",save_all=True,append_images=[Image.new("RGB",(8,8),"red")])
        with tempfile.TemporaryDirectory() as folder:
            root,env,rev=self.fixture(folder,buf.getvalue())
            with self.assertRaisesRegex(ValueError,"[Mm]ulti"):
                await NativeImageProcessor(root=root/"stages",runner=lambda p:self.fail("native called")).process(env,rev)

    async def test_oversize_rejected_before_native(self):
        with tempfile.TemporaryDirectory() as folder:
            root,env,rev=self.fixture(folder,self.raw())
            with self.assertRaisesRegex(ValueError,"pixel"):
                await NativeImageProcessor(root=root/"stages",max_pixels=100,runner=lambda p:self.fail("native called")).process(env,rev)

    async def test_empty_detections_remain_complete_unclassified(self):
        with tempfile.TemporaryDirectory() as folder:
            root,env,rev=self.fixture(folder,self.raw())
            result=await NativeImageProcessor(root=root/"stages",runner=lambda p:[{"text":"","metadata":{"error":None},"page_elements_v3":{"detections":[]}}]).process(env,rev)
            body=self.read(result);self.assertEqual(body["native"]["classes"],[]);self.assertEqual(body["text"],"");self.assertNotIn("decorative",json.dumps(body))

    async def test_contract_and_checksum_verified(self):
        from dataclasses import replace
        with tempfile.TemporaryDirectory() as folder:
            root,env,rev=self.fixture(folder,self.raw());processor=NativeImageProcessor(root=root/"stages",runner=lambda p:self.fail("native called"))
            with self.assertRaisesRegex(ValueError,"contract"):
                await processor.process(replace(env,model_digest="sha256:"+"f"*64),rev)
            rev["raw_sha256"]="0"*64
            with self.assertRaises(Exception):await processor.process(env,rev)


    async def test_native_table_error_cannot_report_complete(self):
        with tempfile.TemporaryDirectory() as folder:
            root,env,rev=self.fixture(folder,self.raw())
            result=[{"metadata":{"error":None},"text":"readable", "table_structure_v1":{"regions":[],"error":{"message":"table service failed"}}}]
            with self.assertRaisesRegex(ValueError,"Native image extraction failed"):
                await NativeImageProcessor(root=root/"stages",runner=lambda p:result).process(env,rev)

    async def test_full_grounded_text_and_original_bytes_are_preserved(self):
        text="Long grounded OCR "*10000
        with tempfile.TemporaryDirectory() as folder:
            raw=self.raw("RGBA");root,env,rev=self.fixture(folder,raw)
            result=await NativeImageProcessor(root=root/"stages",runner=lambda p:[{"text":text,"metadata":{"error":None}}]).process(env,rev)
            body=self.read(result)
            self.assertEqual(body["text"],text)
            self.assertEqual(Path(urlparse(body["raw_uri"]).path).read_bytes(),raw)
            normalized=Path(urlparse(body["normalized"]["uri"]).path)
            self.assertEqual(hashlib.sha256(normalized.read_bytes()).hexdigest(),body["normalized"]["sha256"])
            with Image.open(normalized) as image:
                self.assertEqual(image.mode,"RGB");self.assertEqual(image.getpixel((0,0)),(255,255,255))

    def test_actual_native_loader_png_and_legacy_error(self):
        try:from nemo_retriever.common.modality.image.load import image_bytes_to_pages_df
        except ImportError:self.skipTest("Native library available in selected image only")
        frame=image_bytes_to_pages_df(self.raw(),"generated.png")
        self.assertEqual(frame.iloc[0]["page_number"],1);self.assertTrue(frame.iloc[0]["metadata"]["needs_ocr_for_text"])
        self.assertEqual(frame.iloc[0]["page_image"]["orig_shape_hw"],(24,32))
        self.assertTrue(image_bytes_to_pages_df(self.raw(),"generated.image").iloc[0]["metadata"]["error"])

class LinkedSVGTests(unittest.IsolatedAsyncioTestCase):
 async def test_original_svg_bytes_are_preserved_with_png_renderer_lineage(self):
  raw=b'<svg xmlns="http://www.w3.org/2000/svg" width="80" height="40"><rect width="80" height="40" fill="white"/><text x="2" y="25">Example fixture</text></svg>'
  with tempfile.TemporaryDirectory() as folder:
   helper=NativeImageTests();root,env,rev=helper.fixture(folder,raw)
   result=await NativeImageProcessor(root=root/'stages',runner=lambda p:[{'text':'Example fixture','metadata':{'error':None}}]).process(env,rev)
   body=helper.read(result);self.assertEqual(Path(urlparse(body['raw_uri']).path).read_bytes(),raw)
   self.assertEqual(body['normalized']['transform']['original_format'],'SVG')
   self.assertEqual(body['normalized']['transform']['svg']['renderer'],'cairosvg')
   with Image.open(Path(urlparse(body['normalized']['uri']).path)) as image:self.assertEqual(image.size,(80,40))
