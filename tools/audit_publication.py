"""Audit tracked publication content, including its file types and local identifiers."""
from pathlib import Path
import hashlib,json,re,struct,subprocess,sys,zlib
root=Path(__file__).resolve().parents[1]
paths=subprocess.check_output(['git','ls-files','-z'],cwd=root).decode().split('\0')
patterns=[r'/home/(?!USER\b)[^/\s]+',r'/mnt/nvme[0-9]+',r'localhost:5000',r'GPU-[0-9a-f]{8}-[0-9a-f-]{27,}',r'gh[pousr]_[A-Za-z0-9]{30,}',r'github_pat_[A-Za-z0-9_]{50,}',r'AKIA[0-9A-Z]{16}',r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----']
# Only individually reviewed fictional GUI captures may bypass the binary ban.
manifest_name='docs/gui/screenshots.json'
approved={}
if manifest_name in paths:
 try:
  manifest=json.loads((root/manifest_name).read_text())
  if manifest.get('fictional') is not True:raise ValueError('Not fictional')
  for entry in manifest['screenshots']:
   rel=entry['path'];parts=Path(rel).parts
   if len(parts)!=4 or parts[:3]!=('docs','gui','images') or not rel.endswith('.png') or rel in approved:raise ValueError('Invalid screenshot path')
   if not re.fullmatch('[0-9a-f]{64}',entry['sha256']):raise ValueError('Invalid screenshot hash')
   if rel not in paths:raise ValueError('Screenshot is not tracked')
   approved[rel]=entry
 except (ValueError,KeyError,TypeError):raise SystemExit('Invalid fictional screenshot manifest')

def valid_screenshot(raw,entry):
 if len(raw)>1_000_000 or not raw.startswith(b'\x89PNG\r\n\x1a\n') or hashlib.sha256(raw).hexdigest()!=entry['sha256']:return False
 offset=8;chunks=[];dimensions=None
 while offset+12<=len(raw):
  size=struct.unpack('>I',raw[offset:offset+4])[0];kind=raw[offset+4:offset+8];end=offset+12+size
  if end>len(raw) or kind not in (b'IHDR',b'IDAT',b'IEND'):return False
  data=raw[offset+8:offset+8+size];crc=struct.unpack('>I',raw[offset+8+size:end])[0]
  if zlib.crc32(kind+data)!=crc:return False
  if kind==b'IEND' and size!=0:return False
  if kind==b'IHDR':
   if chunks or size!=13:return False
   dimensions=struct.unpack('>II',data[:8])
  chunks.append(kind);offset=end
  if kind==b'IEND':break
 return offset==len(raw) and chunks[0:1]==[b'IHDR'] and b'IDAT' in chunks and chunks[-1:]==[b'IEND'] and dimensions==(entry.get('width'),entry.get('height'))

# The scanner's own expressions contain identifiers by definition.
violations=[]
for rel in filter(None,paths):
 path=root/rel
 if path.is_symlink():violations.append((rel,'symlink'));continue
 if any(p in {'local-only','local','output','evidence','downloads','artifacts','planning','decisions','operations','.env','__pycache__'} for p in path.relative_to(root).parts):violations.append((rel,'excluded directory'))
 if rel in approved:
  if not valid_screenshot(path.read_bytes(),approved[rel]):violations.append((rel,'unapproved or metadata-bearing screenshot'))
  continue
 if path.suffix.lower() in {'.pdf','.docx','.pptx','.xlsx','.csv','.jsonl','.parquet','.whl','.pyc','.safetensors','.engine','.png','.jpg','.jpeg','.gif','.zip'}:violations.append((rel,'binary/data file'))
 raw=path.read_bytes()
 if b'\x00' in raw:violations.append((rel,'binary data'));continue
 text=raw.decode('utf-8')
 if rel!='tools/audit_publication.py':
  for pattern in patterns:
   if re.search(pattern,text,re.I):violations.append((rel,'environment or secret pattern'));break
 if path.stat().st_size>1_000_000:violations.append((rel,'oversized source file'))
if violations:
 for rel,reason in violations:print(reason+': '+rel)
 sys.exit(1)
print('Publication audit passed:',len(list(filter(None,paths))),'tracked files;',len(approved),'approved fictional screenshots')
