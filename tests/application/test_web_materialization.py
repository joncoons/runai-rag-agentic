import hashlib,json,tempfile,unittest
from pathlib import Path
from collection_control.artifacts import canonical
from collection_control.web_materialization import verify_materialization
from collection_control.repository import Conflict

class MaterializationTests(unittest.TestCase):
 def test_original_html_and_exact_child_evidence_are_bound_without_extra_vectors(self):
  with tempfile.TemporaryDirectory() as tmp:
   raw=Path(tmp)/'raw';stages=Path(tmp)/'stages';raw.mkdir();stages.mkdir()
   image=b'original pixels fixture';(raw/'image').write_bytes(image);sha=hashlib.sha256(image).hexdigest()
   normalized=b'normalized pixels fixture';(raw/'normalized').write_bytes(normalized);norm=hashlib.sha256(normalized).hexdigest()
   imageproof={'document_id':'image','source_revision_id':'image-rev','canonical_uri':'https://fixture.invalid/image.png','raw_uri':(raw/'image').as_uri(),'raw_sha256':sha,'model_digest':'sha256:'+'a'*64,'config_digest':'sha256:'+'b'*64}
   evidence={'schema_version':'native-web-image-evidence.v1','status':'complete','document_id':'image','source_revision_id':'image-rev','raw_uri':imageproof['raw_uri'],'raw_sha256':sha,'normalized':{'uri':(raw/'normalized').as_uri(),'sha256':norm},'native':{'coordinate_frame':'normalized_image','internal_frame_number':1,'model_digest':imageproof['model_digest'],'config_digest':imageproof['config_digest']},'text':'Grounded chart value 45'}
   encoded=canonical(evidence);(stages/'evidence.json').write_bytes(encoded)
   proof={**imageproof,'artifact_uri':(stages/'evidence.json').as_uri(),'sha256':hashlib.sha256(encoded).hexdigest()}
   manifest={'schema_version':'web-asset-group.v1','coverage':'complete','group_id':'group','collection_id':'collection','pages':[{'document_id':'page-doc','canonical_uri':'https://fixture.invalid/page','source_revision_id':'page-rev','assets':[{**imageproof,'kind':'image'}]}]}
   encoded=canonical(manifest);(raw/'group.json').write_bytes(encoded)
   base='Original webpage';text=base+'\n\n[Grounded image evidence: https://fixture.invalid/image.png]\nGrounded chart value 45'
   materialization={'schema_version':'web-page-materialization.v1','group_id':'group','collection_id':'collection','document_id':'page-doc','canonical_uri':'https://fixture.invalid/page','source_revision_id':'page-rev','html_text_sha256':hashlib.sha256(base.encode()).hexdigest(),'group_manifest_uri':(raw/'group.json').as_uri(),'group_manifest_sha256':hashlib.sha256(encoded).hexdigest(),'images':[proof],'text_sha256':hashlib.sha256(text.encode()).hexdigest()}
   self.assertEqual(verify_materialization(raw,materialization,'page-rev',base),text)
   with self.assertRaises(Conflict):verify_materialization(raw,materialization,'later-rev',base)
   (raw/'image').write_bytes(b'changed bytes')
   with self.assertRaises(Conflict):verify_materialization(raw,materialization,'page-rev',base)
