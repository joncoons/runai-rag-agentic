import os,tempfile,unittest
from pathlib import Path
from datetime import datetime,timedelta,timezone
from uuid import uuid4
from unittest.mock import patch
from application.private_scope import sign,verify
from collection_control.repository import Conflict
class ScopeTests(unittest.TestCase):
 def test_signature_owner_binding_and_expiry(self):
  with tempfile.TemporaryDirectory() as root:
   key=Path(root)/'key';key.write_bytes(b'qualification-key-only-32-bytes-xxxxxxxx')
   w={'workspace_id':uuid4(),'owner_id':uuid4(),'expires_at':datetime.now(timezone.utc)+timedelta(minutes=10)}
   with patch.dict(os.environ,{'PRIVATE_SCOPE_KEY_FILE':str(key)}):
    token=sign(w);self.assertEqual(verify(token),(w['workspace_id'],w['owner_id']))
    with self.assertRaises(Conflict):verify(token[:-1]+('a' if token[-1]!='a' else 'b'))
    w['expires_at']=datetime.now(timezone.utc)-timedelta(seconds=1)
    with self.assertRaises(Conflict):verify(sign(w))
