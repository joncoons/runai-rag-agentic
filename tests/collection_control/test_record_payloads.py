import copy,hashlib,json,tempfile,unittest
from pathlib import Path
from collection_control.artifacts import admit,canonical,record_digest
from collection_control.repository import Conflict
from tests.collection_control.test_writer import fixture

class FullPagePayloadTests(unittest.TestCase):
 def large(self):
  a=fixture();r=a['records'][0];r['text']='Complete retained page é\n'*10000;r['metadata']['content_sha256']=hashlib.sha256(r['text'].encode()).hexdigest();return a
 def test_generic_admission_accepts_complete_large_page(self):
  a=self.large();self.assertGreater(len(canonical({'text':a['records'][0]['text'],'metadata':a['records'][0]['metadata']})),65536);self.assertIs(admit(a),a)
 def test_reference_roundtrip_inline_compatibility_and_tamper_rejection(self):
  from collection_control.record_payloads import encode_payload,decode_payload
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);record=self.large()['records'][0];reference=encode_payload(record,root)
   self.assertLess(len(canonical(reference)),65536)
   restored={**record,**decode_payload(reference,root)};self.assertEqual(record_digest(record),record_digest(restored))
   self.assertEqual(encode_payload(record,root),reference)
   small=fixture()['records'][0];inline=encode_payload(small,root);self.assertEqual(inline,{'text':small['text'],'metadata':small['metadata']});self.assertEqual(decode_payload(inline,None),inline)
   path=Path(reference['artifact_uri'].removeprefix('file://'));path.write_bytes(b'{}')
   with self.assertRaises(Conflict):decode_payload(reference,root)
 def test_missing_symlink_and_foreign_root_rejected(self):
  from collection_control.record_payloads import encode_payload,decode_payload
  with tempfile.TemporaryDirectory() as d,tempfile.TemporaryDirectory() as elsewhere:
   root=Path(d);reference=encode_payload(self.large()['records'][0],root);path=Path(reference['artifact_uri'].removeprefix('file://'));data=path.read_bytes();foreign=Path(elsewhere)/'payload.json';foreign.write_bytes(data)
   with self.assertRaises(Conflict):decode_payload({**reference,'artifact_uri':foreign.as_uri()},root)
   path.unlink()
   with self.assertRaises(Conflict):decode_payload(reference,root)
   path.symlink_to(foreign)
   with self.assertRaises(Conflict):decode_payload(reference,root)
