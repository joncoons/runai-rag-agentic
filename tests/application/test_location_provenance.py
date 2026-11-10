import hashlib,json,tempfile,unittest
from pathlib import Path
from collection_control.artifacts import canonical
from collection_control.repository import Conflict
from collection_control.location_provenance import verify_location_capture
class LocationProofTests(unittest.TestCase):
 def test_original_file_inventory_hash_scope_and_run_are_bound(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);raw=b'public document';digest=hashlib.sha256(raw).hexdigest();p=root/'file.txt';p.write_bytes(raw);manifest={'schema_version':'location-source-capture.v1','coverage':'complete','collection_id':'c','source_run_id':'run','inventory':{'complete':True,'missing_means_deleted':False},'files':[{'canonical_uri':'location://approved/a.txt','raw_sha256':digest,'raw_uri':p.as_uri()}]};data=canonical(manifest);f=root/'manifest.json';f.write_bytes(data);ref={'artifact_uri':f.as_uri(),'sha256':hashlib.sha256(data).hexdigest()}
   self.assertEqual(verify_location_capture(root,ref,'location://approved/a.txt',digest,'c','run'),manifest)
   for uri,sha,cid,run in [('location://approved/other',digest,'c','run'),('location://approved/a.txt','0'*64,'c','run'),('location://approved/a.txt',digest,'other','run'),('location://approved/a.txt',digest,'c','old')]:
    with self.subTest(uri=uri,cid=cid,run=run),self.assertRaises(Conflict):verify_location_capture(root,ref,uri,sha,cid,run)
   p.write_bytes(b'corrupt')
   with self.assertRaises(Conflict):verify_location_capture(root,ref,'location://approved/a.txt',digest,'c','run')
