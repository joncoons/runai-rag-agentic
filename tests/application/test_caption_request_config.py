import importlib.util,sys,os,unittest
from pathlib import Path
from unittest.mock import patch
class CaptionRequestConfigTests(unittest.TestCase):
 def module(self):
  name='nv_ingest_durable.qualified_caption_params';spec=importlib.util.spec_from_file_location(name,Path(__file__).resolve().parents[2]/'ingestion/qualified_pipeline_worker.py');module=importlib.util.module_from_spec(spec);sys.modules[name]=module;spec.loader.exec_module(module);return module
 def env(self):return {'CAPTION_URL':'http://caption.test/v1/chat/completions','CAPTION_MODEL':'selected-model','EMBED_URL':'http://embed.test/v1','EMBED_MODEL':'selected-embed','CAPTION_EXTRA_BODY':'{"chat_template_kwargs":{"enable_thinking":false}}'}
 def test_explicit_nonreasoning_request_is_retained_in_caption_params(self):
  with patch.dict(os.environ,self.env()):params,embed=self.module()._model_params()
  self.assertIs(params.extra_body['chat_template_kwargs']['enable_thinking'],False);self.assertEqual(embed.dimensions,2048)
 def test_invalid_extra_body_json_fails_closed(self):
  with patch.dict(os.environ,{**self.env(),'CAPTION_EXTRA_BODY':'invalid'}),self.assertRaises(ValueError):self.module()._model_params()
 def test_missing_setting_preserves_legacy_configuration(self):
  env=self.env();env.pop('CAPTION_EXTRA_BODY')
  with patch.dict(os.environ,env,clear=True):params,_=self.module()._model_params()
  self.assertEqual(params.extra_body,{})
