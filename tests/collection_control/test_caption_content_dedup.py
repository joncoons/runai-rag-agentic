import unittest
from collection_control.web_captions import captioned_text
class CaptionContentTests(unittest.TestCase):
 def test_repeated_hash_has_one_text_contribution_but_input_proofs_are_not_removed(self):
  def dep(key,sha,text='same caption'):
   return dict(stage_key=key,asset_sha256=sha*64,model_digest='c'*64,config_digest='d'*64,stage_artifact_sha256='e'*64,canonical_uri='https://fixture.invalid/'+key,text=text)
  deps=[dep('a','a'),dep('b','a'),dep('c','b','different image')]
  text=captioned_text('parent',deps)
  self.assertEqual(text.count('same caption'),1)
  self.assertIn('different image',text)
  self.assertEqual(len(deps),3)
 def test_changed_caption_contract_is_not_suppressed(self):
  deps=[dict(stage_key=k,asset_sha256='a'*64,model_digest=m*64,config_digest='d'*64,stage_artifact_sha256='e'*64,canonical_uri='https://fixture.invalid/'+k,text='caption '+k) for k,m in [('a','b'),('b','c')]]
  text=captioned_text('parent',deps)
  self.assertIn('caption a',text);self.assertIn('caption b',text)

 def test_historical_composition_can_be_reconstructed_without_deduplication(self):
  d=dict(stage_key='a',asset_sha256='a'*64,model_digest='b'*64,config_digest='c'*64,stage_artifact_sha256='d'*64,canonical_uri='https://fixture.invalid/a',text='historical caption')
  deps=[d,{**d,'stage_key':'b'}]
  self.assertEqual(captioned_text('parent',deps,deduplicate=False).count('historical caption'),2)
