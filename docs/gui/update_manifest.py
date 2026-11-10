"""Record reviewed fictional captures; reject missing or unexpected image names."""
import hashlib,json,re,struct
from pathlib import Path
root=Path(__file__).resolve().parents[2]
folder=root/'docs/gui/images'
expected=set(re.findall(r"shot\('([^']+)'\)",(root/'docs/gui/capture.js').read_text()))
actual={p.stem for p in folder.glob('*.png')}
if actual!=expected:raise SystemExit('Capture image names differ from the documented capture script')
files=[]
for name in sorted(expected):
    p=folder/(name+'.png');raw=p.read_bytes()
    if not raw.startswith(b'\x89PNG\r\n\x1a\n'):raise SystemExit('Invalid PNG: '+name)
    width,height=struct.unpack('>II',raw[16:24])
    if (width,height)!=(1440,1040):raise SystemExit('Unexpected screenshot dimensions: '+name)
    files.append({'path':str(p.relative_to(root)),'sha256':hashlib.sha256(raw).hexdigest(),'width':width,'height':height})
manifest={'fictional':True,'source':'Real application static frontend with docs/gui/demo_server.py synthetic API fixtures','screenshots':files}
(root/'docs/gui/screenshots.json').write_text(json.dumps(manifest,indent=2)+'\n')
print('Recorded',len(files),'fictional screenshots; run the publication audit after staging')
