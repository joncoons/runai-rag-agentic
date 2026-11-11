"""Short-lived controller capability; never placed in model prompts or Git."""
import base64,hashlib,hmac,json,os,time
from pathlib import Path
from uuid import UUID
from collection_control.repository import Conflict

def _key():
 value=Path(os.environ['PRIVATE_SCOPE_KEY_FILE']).read_bytes().strip()
 if len(value)<32:raise ValueError('Private scope signing key is not configured')
 return value

def sign(workspace):
 body=json.dumps({'workspace':str(workspace['workspace_id']),'owner':str(workspace['owner_id']),
  'expires':min(int(workspace['expires_at'].timestamp()),int(time.time())+300)},sort_keys=True,separators=(',',':')).encode()
 encoded=base64.urlsafe_b64encode(body).decode().rstrip('=')
 return encoded+'.'+hmac.new(_key(),encoded.encode(),hashlib.sha256).hexdigest()

def verify(token):
 try:
  if not isinstance(token,str) or len(token)>1024:raise ValueError()
  encoded,signature=token.split('.')
  if not hmac.compare_digest(hmac.new(_key(),encoded.encode(),hashlib.sha256).hexdigest(),signature):raise ValueError()
  body=json.loads(base64.urlsafe_b64decode(encoded+'='*(-len(encoded)%4)))
  if set(body)!={'workspace','owner','expires'} or isinstance(body['expires'],bool) or body['expires']<=time.time():raise ValueError()
  return UUID(body['workspace']),UUID(body['owner'])
 except (ValueError,TypeError,KeyError):raise Conflict('Private analysis scope is invalid or expired') from None
