"""Write a salted local admin verifier; the password is never printed or placed in arguments."""
import argparse,getpass,hashlib,json,os,secrets
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument("--username",required=True);p.add_argument("--output",type=Path,required=True);a=p.parse_args()
password=getpass.getpass("Admin password: ")
if not password or password!=getpass.getpass("Confirm password: "):raise SystemExit("Passwords must match and be nonempty")
salt=secrets.token_bytes(16);record={"username":a.username,"password":{"algorithm":"scrypt","salt":salt.hex(),"digest":hashlib.scrypt(password.encode(),salt=salt,n=16384,r=8,p=1,dklen=32).hex()},"collection_write":["*"]}
a.output.parent.mkdir(parents=True,exist_ok=True)
fd=os.open(a.output,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
with os.fdopen(fd,"w") as f:json.dump(record,f)
