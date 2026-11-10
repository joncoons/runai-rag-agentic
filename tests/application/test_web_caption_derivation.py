import copy
import hashlib
import unittest

from collection_control.artifacts import admit
from collection_control.repository import Conflict
from collection_control.web_captions import derived_page, captioned_text


class WebCaptionDerivationTests(unittest.TestCase):
    def fixture(self):
        contract={'dimensions':2048,'dtype':'float32','normalization':'unit','metric':'IP','schema_version':1,'embedding_checkpoint_sha256':'e'*64}
        record={'record_id':'original-page-record','text':'Original grounded page', 'vector':[1.0]+[0.0]*2047,'metadata':{'origin':'grounded','source_id':'source','source_revision_id':'revision','content_sha256':hashlib.sha256(b'Original grounded page').hexdigest(),'acl':['reader'],'provenance':{'canonical_uri':'https://fixture.invalid/page'}}}
        captions=[{'stage_key':'caption-b','document_id':'image','canonical_uri':'https://fixture.invalid/image','text':'A machine description','asset_sha256':'a'*64,'model_digest':'b'*64,'config_digest':'c'*64,'stage_artifact_sha256':'d'*64}]
        return contract,record,captions

    def test_same_parent_record_with_explicit_generated_lineage(self):
        contract,parent,captions=self.fixture();original=copy.deepcopy(parent)
        text=captioned_text(parent['text'],captions)
        derived=derived_page(parent,captions,[0.0,1.0]+[0.0]*2046,contract,'f'*64,'parent-stage')
        self.assertEqual(derived['record_id'],parent['record_id'])
        self.assertEqual(derived['metadata']['source_revision_id'],'revision')
        self.assertEqual(derived['metadata']['origin'],'derived')
        self.assertEqual(derived['text'],text)
        self.assertIn('[Machine-generated image caption:',text)
        self.assertEqual(parent,original)
        admit({'contract':contract,'records':[derived]})

    def test_empty_duplicate_or_nondeterministic_caption_coverage_rejected(self):
        contract,parent,captions=self.fixture()
        for invalid in ([],captions*2,[{**captions[0],'text':'   '}], [{**captions[0],'model_digest':'invalid'}]):
            with self.subTest(invalid=invalid),self.assertRaises(Conflict):
                derived_page(parent,invalid,parent['vector'],contract,'f'*64,'parent-stage')

    def test_proof_or_text_tamper_rejected(self):
        contract,parent,captions=self.fixture()
        result=derived_page(parent,captions,parent['vector'],contract,'f'*64,'parent-stage')
        for field,value in [('text','tampered'),('record_id','independent-image')]:
            tampered=copy.deepcopy(result);tampered[field]=value
            with self.assertRaises(Conflict):admit({'contract':contract,'records':[tampered]})
