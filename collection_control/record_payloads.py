"""Historical ADR0057 payload compatibility; active writers use native fields."""
import hashlib
import json
import os
from pathlib import Path
from uuid import uuid4
from .artifacts import canonical,_unique_object
from .provenance import verified_file
from .repository import Conflict,digest

SCHEMA='retained-record-payload.v1'


def encode_payload(record,root):
    payload={'text':record['text'],'metadata':record['metadata']}
    raw=canonical(payload)
    if len(raw)<=65536:return payload
    if root is None:raise Conflict('Full payload artifact root is not configured')
    root=Path(root).resolve();sha=hashlib.sha256(raw).hexdigest()
    folder=root/sha[:2];folder.mkdir(parents=True,exist_ok=True)
    if folder.is_symlink():raise Conflict('Payload artifact directory cannot be a symlink')
    target=folder/(sha+'.json');temporary=folder/('.pending-'+uuid4().hex)
    try:
        if not target.exists():
            with temporary.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
            try:os.link(temporary,target)
            except FileExistsError:pass
        for directory in (folder,root,root.parent):
            fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try:os.fsync(fd)
            finally:os.close(fd)
        verified_file(root,target.as_uri(),sha,len(raw))
    finally:temporary.unlink(missing_ok=True)
    return {'schema_version':SCHEMA,'artifact_uri':target.as_uri(),'sha256':sha}


def decode_payload(payload,root):
    if not isinstance(payload,dict):raise Conflict('Invalid record payload')
    if set(payload)=={'text','metadata'}:result=payload
    else:
        if root is None or set(payload)!={'schema_version','artifact_uri','sha256'} or payload['schema_version']!=SCHEMA:raise Conflict('Unsupported full payload reference')
        raw=verified_file(root,payload['artifact_uri'],digest(payload['sha256']))
        try:result=json.loads(raw,object_pairs_hook=_unique_object)
        except (ValueError,TypeError) as error:raise Conflict('Invalid retained record payload') from error
        if canonical(result)!=raw:raise Conflict('Retained record payload is not canonical')
    if not isinstance(result,dict) or set(result)!={'text','metadata'} or not isinstance(result['text'],str) or not isinstance(result['metadata'],dict):raise Conflict('Incomplete full record payload')
    return result
