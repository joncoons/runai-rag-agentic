"""Audit tracked publication content, including its file types and local identifiers."""
from pathlib import Path
import re,subprocess,sys
root=Path(__file__).resolve().parents[1]
paths=subprocess.check_output(['git','ls-files','-z'],cwd=root).decode().split('\0')
patterns=[r'/home/(?!USER\b)[^/\s]+',r'/mnt/nvme[0-9]+',r'localhost:5000',r'GPU-[0-9a-f]{8}-[0-9a-f-]{27,}',r'gh[pousr]_[A-Za-z0-9]{30,}',r'github_pat_[A-Za-z0-9_]{50,}',r'AKIA[0-9A-Z]{16}',r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----']
# The scanner's own expressions contain identifiers by definition.
violations=[]
for rel in filter(None,paths):
 path=root/rel
 if path.is_symlink():violations.append((rel,'symlink'));continue
 if any(p in {'local-only','local','output','evidence','downloads','artifacts','planning','decisions','operations','.env','__pycache__'} for p in path.relative_to(root).parts):violations.append((rel,'excluded directory'))
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
print('Publication audit passed:',len(list(filter(None,paths))),'tracked text files')
