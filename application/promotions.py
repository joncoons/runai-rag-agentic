"""Authorized copy into ordinary durable intake; no direct vector writes."""
import asyncio,hashlib,json
from uuid import UUID,uuid4
from fastapi import HTTPException,Request
from pydantic import BaseModel,ConfigDict,Field
from collection_control.repository import Conflict
from collection_control.provenance import verified_file
from .local_auth import SessionStore
from .research_api import owner_session

async def revoke_unsubmitted(request,workspace):
 from .api import _context
 await _context(request).pool.execute("UPDATE application.document_promotions SET revoked_at=clock_timestamp(),status='cancelled' WHERE workspace_id=$1 AND status='authorized'",workspace)

async def promotion_listing(request,workspace):
 from .api import _context
 session=await owner_session(request)
 rows=await _context(request).pool.fetch('SELECT p.*,a.name AS destination_name FROM application.document_promotions p JOIN application.collections a ON a.collection_id=p.destination WHERE owner_id=$1 AND workspace_id=$2 ORDER BY created_at',session['owner_id'],workspace)
 return [{'id':str(r['promotion_id']),'destination':r['destination'],'destination_name':r['destination_name'],'status':r['status'],'job_id':str(r['job_id']) if r['job_id'] else None} for r in rows]

async def authorize_intake(connection,promotion,collection,prepared):
 row=await connection.fetchrow('SELECT * FROM application.document_promotions WHERE promotion_id=$1 FOR UPDATE',UUID(str(promotion)))
 if not row or row['destination']!=collection or len(prepared)!=1 or row['source_sha256']!=prepared[0]['raw_sha256']:
  raise Conflict('Promotion capture differs from the authorized original')
 live=await connection.fetchval("SELECT application.promotion_allowed(p) AND status='copying' FROM application.document_promotions p WHERE promotion_id=$1",row['promotion_id'])
 if not live:raise Conflict('Promotion authorization expired or was revoked')
 return {'promotion_id':str(row['promotion_id']),'source_workspace_id':str(row['workspace_id']),'original_sha256':row['source_sha256']}

class Promote(BaseModel):
 model_config=ConfigDict(extra='forbid')
 destination:str=Field(min_length=1,max_length=255,strict=True)
 request_key:str=Field(min_length=1,max_length=128,strict=True)

def install_promotion_routes(app):
 from .api import _context
 from .private_api import private_context
 from .private_workspaces import PrivateWorkspaces
 from .private_context import scoped_context
 @app.post('/api/analysis/workspaces/{workspace}/promotions',status_code=202)
 async def promote(workspace:UUID,body:Promote,request:Request):
  session=await owner_session(request,write=True);main=_context(request);private=await private_context(request)
  row=await PrivateWorkspaces(private).detail(session['owner_id'],workspace)
  if not row:raise HTTPException(404,'Document is unavailable or expired')
  store=SessionStore(main.pool);await store.synchronize_policy();grant=await store.authorize_write(session,body.destination)
  prior=await main.pool.fetchrow('SELECT * FROM application.document_promotions WHERE owner_id=$1 AND request_key=$2',session['owner_id'],body.request_key)
  if prior:
   if prior['workspace_id']!=workspace or prior['destination']!=body.destination:raise Conflict('Promotion key was used for a different source or destination')
   return {'id':str(prior['promotion_id']),'status':prior['status']}
  promotion=uuid4()
  await main.pool.execute('''INSERT INTO application.document_promotions(promotion_id,owner_id,workspace_id,destination,request_key,source_sha256,principal,session_hash,policy_version,copy_expires_at,authorization_expires_at)
   VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)''',promotion,session['owner_id'],workspace,body.destination,body.request_key,row['raw_sha256'],grant['principal'],grant['session_hash'],grant['policy_version'],row['expires_at'],grant['valid_until'])
  try:
   if not row['raw_uri']:raise Conflict('Original document capture is not yet complete')
   raw=await asyncio.to_thread(verified_file,scoped_context(private,row).settings.raw,row['raw_uri'],row['raw_sha256'])
   if not await PrivateWorkspaces(private).get(session['owner_id'],workspace):raise Conflict('Private source expired before durable copy')
   await store.synchronize_policy();await store.authorize_write(session,body.destination)
   started=await main.pool.fetchval("UPDATE application.document_promotions p SET status='copying' WHERE promotion_id=$1 AND status='authorized' AND copy_expires_at>clock_timestamp() AND application.promotion_allowed(p) RETURNING true",promotion)
   if not started:raise Conflict('Promotion copy authorization expired or was revoked')
   from .intake import submit
   metadata=await private.pool.fetchval('SELECT metadata FROM public.jobs WHERE job_id=$1',row['job_id'])
   if isinstance(metadata,str):metadata=json.loads(metadata)
   job=await submit(main,body.destination,[(row['filename'],raw)],captions_enabled=bool((metadata or {}).get('ingestion_options',{}).get('captions_enabled')),_promotion=promotion)
   return {'id':str(promotion),'job_id':job,'status':'submitted'}
  except BaseException:
   failed=await main.pool.fetchval("UPDATE application.document_promotions SET status='failed' WHERE promotion_id=$1 AND status IN ('authorized','copying') AND job_id IS NULL RETURNING true",promotion)
   if failed:
    import shutil
    folder=main.settings.raw/'promotions'/str(promotion)
    if folder.exists() and not folder.is_symlink():await asyncio.to_thread(shutil.rmtree,folder)
   raise
