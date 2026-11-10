"""The screenshot exception must not admit arbitrary data or embedded metadata."""
import hashlib,json,shutil,struct,subprocess,tempfile,unittest,zlib
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]

def png(metadata=False):
 def chunk(kind,data):return struct.pack('>I',len(data))+kind+data+struct.pack('>I',zlib.crc32(kind+data))
 return b'\x89PNG\r\n\x1a\n'+chunk(b'IHDR',struct.pack('>IIBBBBB',1,1,8,2,0,0,0))+(chunk(b'tEXt',b'Author\x00not-allowed') if metadata else b'')+chunk(b'IDAT',zlib.compress(b'\x00\x00\x00\x00'))+chunk(b'IEND',b'')

class PublicationImagePolicy(unittest.TestCase):
 def check(self,*,approved=True,tamper=False,metadata=False):
  with tempfile.TemporaryDirectory() as tmp:
   r=Path(tmp);(r/'tools').mkdir();shutil.copyfile(ROOT/'tools/audit_publication.py',r/'tools/audit_publication.py')
   image=r/'docs/gui/images/example.png';image.parent.mkdir(parents=True);raw=png(metadata);image.write_bytes(raw)
   if approved:
    manifest={'fictional':True,'screenshots':[{'path':'docs/gui/images/example.png','sha256':hashlib.sha256(raw).hexdigest(),'width':1,'height':1}]}
    (r/'docs/gui/screenshots.json').write_text(json.dumps(manifest))
   if tamper:image.write_bytes(raw+b'changed')
   subprocess.run(['git','init','-q'],cwd=r,check=True);subprocess.run(['git','add','.'],cwd=r,check=True)
   return subprocess.run(['python3','tools/audit_publication.py'],cwd=r,capture_output=True,text=True).returncode
 def test_hash_approved_fictional_png_is_allowed(self):self.assertEqual(self.check(),0)
 def test_unlisted_png_is_rejected(self):self.assertNotEqual(self.check(approved=False),0)
 def test_changed_png_is_rejected(self):self.assertNotEqual(self.check(tamper=True),0)
 def test_embedded_text_metadata_is_rejected(self):self.assertNotEqual(self.check(metadata=True),0)

if __name__=='__main__':unittest.main()
