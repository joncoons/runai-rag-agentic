"""Native worker assembly contracts using generated stage fixtures, never a corpus benchmark."""
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import pandas as pd
from nv_ingest_durable.pipeline_worker import PipelineWorkerSettings, build_processor


class NativeContentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        environment = {
            'PAGE_ELEMENTS_URL':'http://127.0.0.1:1/v1/page-elements',
            'OCR_URL':'http://127.0.0.1:1/v1/ocr',
            'TABLE_STRUCTURE_URL':'http://127.0.0.1:1/v1/table-structure',
            'CAPTION_URL':'http://127.0.0.1:1/v1/chat/completions',
            'CAPTION_MODEL':'nvidia/nemotron-3-nano-omni-30b-a3b-reasoning',
            'EMBED_URL':'http://127.0.0.1:1/v1/embeddings',
            'EMBED_MODEL':'nvidia/Nemotron-3.5-Embed-1B-NVFP4',
            'EMBED_CHECKPOINT_DIR':os.environ['TEST_EMBED_CHECKPOINT'],
            'EMBED_DIMENSIONS':'2048',
        }
        for name in ['CAPTION_MODEL_DIGEST','EMBED_CONFIG_DIGEST','EMBED_MODEL_DIGEST','INDEX_CONFIG_DIGEST','INDEX_MODEL_DIGEST']:
            environment[name]='sha256:'+'1'*64
        self.env = patch.dict(os.environ, environment)
        self.env.start()
        self.addCleanup(self.env.stop)
        settings = PipelineWorkerSettings(role='fused',postgres_dsn='not-connected',redis_url='not-connected',artifact_root=Path(self.temp.name),stream='test',consumer_group='test',consumer='test',concurrency=1,read_count=1,caption_enabled=True)
        processor = build_processor(settings)
        self.processor=processor
        graph = processor.runner.registry.build(processor.graph_name)
        operators=[]
        def visit(node):
            operators.append(node.operator)
            for child in node.children:visit(child)
        for root in graph.roots:visit(root)
        first=next(i for i,op in enumerate(operators) if getattr(op,'name','')=='RequireSuccessfulExtraction')
        self.operators=operators[first:]
        self.embedded=[]

    def run_content(self, content):
        def remote(actor, frame, **kwargs):
            self.embedded.extend(frame['text'].tolist())
            result=frame.copy()
            result[actor._params.output_column]=[{'embedding':[1.0]+[0.0]*2047} for _ in range(len(result))]
            return result
        with patch('nemo_retriever.operators.embed.cpu_operator._BatchEmbedCPUActor.process',remote):
            output=pd.DataFrame([{'page_number':1,**content}] if isinstance(content,dict) else content)
            for operator in self.operators:output=operator.run(output)
            return output

    def test_scanned_page_recognized_text_survives(self):
        result=self.run_content({'text':'Recognized scan text','metadata':{'has_text':False,'needs_ocr_for_text':True},'images':[]})
        self.assertIn('Recognized scan text',self.embedded)
        self.assertEqual(len(result),1)

    def test_table_chart_and_infographic_text_reaches_embedding(self):
        self.run_content({'text':'Page heading','metadata':{},'images':[],
            'table':[{'text':'Table revenue 42'}],
            'chart':[{'text':'Chart sales 84'}],
            'infographic':[{'text':'Infographic theme 126'}]})
        for expected in ['Page heading','Table revenue 42','Chart sales 84','Infographic theme 126']:
            self.assertIn(expected,self.embedded)

    def test_infographic_caption_and_ocr_are_separate_searchable_records(self):
        output=self.run_content({'text':'','metadata':{},'images':[],
            'infographic':[{'text':'Observed source label','caption':'Generated visual description'}]})
        self.assertIn('Observed source label',self.embedded)
        self.assertIn('Generated visual description',self.embedded)
        self.assertEqual(set(output['_content_type']),{'infographic','infographic_caption'})

    def test_observed_blank_pages_survive_as_explicit_completion_after_row_filtering(self):
        output=self.run_content([{'page_number':page,'text':text,'metadata':{'error':None},'images':[]} for page,text in [(1,'Observed page text'),(2,''),(3,'')]])
        self.assertEqual(len(output),1)
        self.assertEqual(self.embedded,['Observed page text'])
        self.assertEqual(output.attrs['native_page_completions'],{'schema_version':'native-page-completions.v1','pages':[{'page_number':page,'status':'complete'} for page in (1,2,3)]})

    def test_upstream_failure_cannot_reach_embedding(self):
        with self.assertRaises(ValueError):
            self.run_content({'text':'Partial output','metadata':{},'ocr':{'error':'failed OCR'}})
        self.assertEqual(self.embedded,[])

    def test_bad_base64_image_does_not_block_page_text(self):
        output=self.run_content({'text':'Independent page text','metadata':{},'images':[{'image_b64':'not-base64%%%','text':''}]})
        self.assertIn('Independent page text',self.embedded)
        self.assertEqual(output.attrs['caption_assets'][0]['asset_validation_error'],'invalid_encoding')

    def test_overlength_rejected_before_model_request(self):
        with self.assertRaises(ValueError):
            self.run_content({'text':'document '*40000,'metadata':{},'images':[]})
        self.assertEqual(self.embedded,[])

class DeferredAssetTests(unittest.TestCase):
    def setUp(self):
        import base64
        from io import BytesIO
        from PIL import Image
        from types import SimpleNamespace
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        stream=BytesIO();Image.new('RGB',(64,64),'blue').save(stream,format='PNG')
        self.image=base64.b64encode(stream.getvalue()).decode()
        self.params=SimpleNamespace(caption_infographics=True,model_name='test-model',temperature=0,max_tokens=512)

    def test_image_only_page_retains_pending_asset_without_searchable_rows(self):
        from nv_ingest_durable.deferred_caption import DeferredCaptionAssets,reshape_source_content
        source=pd.DataFrame([{'page_number':1,'text':'','images':[{'image_b64':self.image,'text':''}]}])
        deferred=DeferredCaptionAssets(self.temp.name,self.params,'sha256:'+'a'*64).process(source)
        output=reshape_source_content(deferred,caption_enabled=True)
        self.assertTrue(output.empty)
        self.assertEqual(len(output.attrs['caption_assets']),1)

    def test_duplicate_bytes_preserve_separate_page_associations(self):
        from nv_ingest_durable.deferred_caption import DeferredCaptionAssets
        source=pd.DataFrame([{'text':'page','_page_number':page,'images':[{'image_b64':self.image,'text':''}]} for page in [1,2]])
        result=DeferredCaptionAssets(self.temp.name,self.params,'sha256:'+'a'*64).process(source)
        assets=result.attrs['caption_assets']
        self.assertEqual([item['page_number'] for item in assets],[1,2])
        self.assertEqual(assets[0]['asset_sha256'],assets[1]['asset_sha256'])

    def test_corrupt_image_is_retained_for_terminal_worker_classification(self):
        import base64
        from nv_ingest_durable.deferred_caption import DeferredCaptionAssets
        source=pd.DataFrame([{'page_number':1,'text':'page','images':[{'image_b64':base64.b64encode(b'corrupt-image').decode(),'text':''}]}])
        result=DeferredCaptionAssets(self.temp.name,self.params,'sha256:'+'a'*64).process(source)
        self.assertEqual(len(result.attrs['caption_assets']),1)

if __name__=='__main__':unittest.main()
