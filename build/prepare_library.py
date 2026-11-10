"""Download and patch the qualified public Retriever library, verifying every change."""
import hashlib,json,subprocess,sys,tempfile,zipfile
from pathlib import Path
VERSION="26.8.2"
SHA="8efcd63dc290715b79b9767547c1a2591f976aa190c27a3217f9c3a4f01d6e46"
root=Path(__file__).resolve().parents[1]
out=Path(sys.argv[1]);out.mkdir(parents=True,exist_ok=True)
with tempfile.TemporaryDirectory() as tmp:
 subprocess.run([sys.executable,"-m","pip","download","--no-deps","--dest",tmp,"nemo-retriever=="+VERSION],check=True)
 wheel=next(Path(tmp).glob("*.whl"))
 if hashlib.sha256(wheel.read_bytes()).hexdigest()!=SHA:raise ValueError("Retriever wheel checksum mismatch")
 with zipfile.ZipFile(wheel) as archive:
  for name in archive.namelist():
   if not (out/name).resolve().is_relative_to(out.resolve()):raise ValueError("Unsafe wheel path")
  archive.extractall(out)
for entry in json.loads((root/"ingestion/patches/library-lock.json").read_text()):
 path=out/"nemo_retriever"/entry["file"]
 if hashlib.sha256(path.read_bytes()).hexdigest()!=entry["before_sha256"]:raise ValueError("Unexpected upstream source")
 subprocess.run(["git","apply","--unidiff-zero",str(root/"ingestion/patches"/entry["patch"])],cwd=out,check=True)
 if hashlib.sha256(path.read_bytes()).hexdigest()!=entry["after_sha256"]:raise ValueError("Patch verification failed")
