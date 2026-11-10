"""Install the matching browser/driver pair; no binaries are stored in Git."""
import hashlib,json,os,shutil,tempfile,urllib.request,zipfile
from pathlib import Path
root=Path(__file__).parent
with tempfile.TemporaryDirectory() as tmp:
 for item in json.loads((root/"browser-artifacts.json").read_text())["artifacts"]:
  archive=Path(tmp)/item["url"].rsplit("/",1)[1]
  urllib.request.urlretrieve(item["url"],archive)
  if hashlib.sha256(archive.read_bytes()).hexdigest()!=item["sha256"]:raise ValueError("Browser checksum mismatch")
  with zipfile.ZipFile(archive) as z:
   for entry in z.infolist():
    if not (Path(tmp)/entry.filename).resolve().is_relative_to(Path(tmp)):raise ValueError("Unsafe browser member")
   z.extractall(tmp)
 dest=Path("/opt/browser");dest.mkdir(parents=True,exist_ok=True)
 shutil.move(str(Path(tmp)/"chrome-linux64"),dest/"chrome")
 shutil.copyfile(Path(tmp)/"chromedriver-linux64/chromedriver","/usr/bin/chromedriver")
 for p in (dest/"chrome").rglob("*"):
  if p.is_file() and p.name in {"chrome","chrome_crashpad_handler","chrome_sandbox"}:p.chmod(0o755)
 Path("/usr/bin/chromedriver").chmod(0o755)
 Path("/usr/bin/google-chrome").symlink_to(dest/"chrome/chrome")
