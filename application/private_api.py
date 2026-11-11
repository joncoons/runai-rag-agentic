"""Private document HTTP lifecycle. IDs alone are never access grants."""
import asyncio,json
from uuid import UUID
from fastapi import HTTPException,Request,UploadFile,File,Form
from fastapi.responses import Response,StreamingResponse
from pydantic import BaseModel,ConfigDict,Field
from collection_control.provenance import verified_file
from .private_context import open_private,scoped_context
from .private_workspaces import PrivateWorkspaces
from .research_api import owner_session,public_run
from .research_jobs import ResearchJobs

async def private_context(request):
 from .api import _context
 ctx=_context(request)
 if not hasattr(ctx,'_private_lock'):ctx._private_lock=asyncio.Lock()
 async with ctx._private_lock:
  if not getattr(ctx,'_private_context',None):
   try:ctx._private_context=await open_private(ctx.settings)
   except (KeyError,ValueError):raise HTTPException(503,'Document analysis is not configured') from None
  return ctx._private_context

def public_workspace(row,run=None):
 if not row:raise HTTPException(404,'Document is unavailable or expired in this session')
 return {'id':str(row['workspace_id']),'filename':row['filename'],'expires_at':row['expires_at'].isoformat(),
  'status':row.get('processing_status','queued'),'ready':row.get('ready',False),'records':row.get('records',0),
  'run':run,'promotions':[],'captions':row.get('captions',{})}

def owned_run(row,workspace,request):
 if not row or row['workspace_id']!=workspace['workspace_id']:raise HTTPException(404,'Document analysis is unavailable')
 result=public_run(row)
 origin=str(request.base_url).rstrip('/')
 for source in result['sources']:
  source['uri']=origin+'/api/analysis/workspaces/'+str(workspace['workspace_id'])+'/file'+('#page='+str(source['page']) if source.get('page') else '')
  source['title']=workspace['filename']
 return result

class Question(BaseModel):
 model_config=ConfigDict(extra='forbid')
 question:str=Field(min_length=1,max_length=131072,strict=True)
 request_key:str=Field(min_length=1,max_length=128,strict=True)

def install_private_routes(app):
 async def owned(request,workspace,*,write=False):
  session=await owner_session(request,write=write);ctx=await private_context(request)
  row=await PrivateWorkspaces(ctx).detail(session['owner_id'],workspace)
  if not row:raise HTTPException(404,'Document is unavailable or expired in this session')
  return session,ctx,row
 @app.get('/api/analysis/workspaces')
 async def listing(request:Request):
  session=await owner_session(request);ctx=await private_context(request);repo=PrivateWorkspaces(ctx)
  rows=await repo.listing(session['owner_id']);result=[]
  for row in rows:result.append(public_workspace(await repo.detail(session['owner_id'],row['workspace_id'])))
  return {'workspaces':result}
 @app.post('/api/analysis/workspaces',status_code=202)
 async def upload(request:Request,file:UploadFile=File(...),captions_enabled:str=Form('false'),question:str=Form('')):
  session=await owner_session(request,write=True)
  if captions_enabled not in ('true','false'):raise HTTPException(422,'Caption choice must be true or false')
  raw=await file.read(128*1024*1024+1)
  if not raw or len(raw)>128*1024*1024:raise HTTPException(422,'Choose a document up to 128 MiB')
  if len(question)>131072:raise HTTPException(422,'The question exceeds the explicit input limit; it was not truncated')
  ctx=await private_context(request)
  row=await PrivateWorkspaces(ctx).create(session['owner_id'],file.filename,raw,captions=captions_enabled=='true',question=question.strip() or 'Summarize this document, its key facts and material qualifications.')
  return public_workspace(row)
 @app.get('/api/analysis/workspaces/{workspace}')
 async def get(workspace:UUID,request:Request):
  session,ctx,row=await owned(request,workspace)
  run=await ctx.pool.fetchrow('SELECT r.*,(SELECT COALESCE(max(event_id),0) FROM application.research_events e WHERE e.run_id=r.run_id) AS event_cursor FROM application.research_runs r WHERE workspace_id=$1 AND owner_id=$2 ORDER BY created_at DESC LIMIT 1',workspace,session['owner_id'])
  from .research_jobs import decoded
  result=public_workspace(row,owned_run(decoded(run),row,request) if run else None)
  from .promotions import promotion_listing
  result['promotions']=await promotion_listing(request,workspace)
  return result
 @app.get('/api/analysis/workspaces/{workspace}/file')
 async def source(workspace:UUID,request:Request):
  _,ctx,row=await owned(request,workspace)
  if not row['raw_uri']:raise HTTPException(409,'Document capture is not complete')
  scoped=scoped_context(ctx,row)
  raw=await asyncio.to_thread(verified_file,scoped.settings.raw,row['raw_uri'],row['raw_sha256'])
  # Recheck after file IO: expiry cannot race a download authorization.
  if not await PrivateWorkspaces(ctx).get(row['owner_id'],workspace):raise HTTPException(404,'Document expired')
  from urllib.parse import quote
  mime=await ctx.pool.fetchval('SELECT media_type FROM application.source_revisions WHERE collection_id=$1 LIMIT 1',row['collection_id'])
  return Response(raw,media_type=mime or 'application/octet-stream',headers={'Cache-Control':'no-store','Content-Disposition':("inline" if mime=="application/pdf" else "attachment")+"; filename*=UTF-8''"+quote(row['filename']),'X-Content-Type-Options':'nosniff','Content-Security-Policy':"sandbox; default-src 'none'; frame-ancestors 'self'"})
 @app.delete('/api/analysis/workspaces/{workspace}',status_code=202)
 async def delete(workspace:UUID,request:Request):
  session,ctx,row=await owned(request,workspace,write=True)
  from .promotions import revoke_unsubmitted
  await revoke_unsubmitted(request,workspace)
  await PrivateWorkspaces(ctx).expire(session['owner_id'],workspace)
  return {'status':'expired','cleanup':'scheduled'}
 @app.post('/api/analysis/workspaces/{workspace}/questions',status_code=202)
 async def question(workspace:UUID,body:Question,request:Request):
  session,ctx,row=await owned(request,workspace,write=True)
  if not row['ready']:raise HTTPException(409,'Document extraction and publication are still in progress')
  run=await ResearchJobs(ctx.pool).create(session['owner_id'],body.request_key,{'question':body.question,'collection_ids':[row['collection_id']],'backend':'elasticsearch','search_budget':1},workspace_id=workspace,expires_at=row['expires_at'])
  return owned_run(run,row,request)
 @app.post('/api/analysis/workspaces/{workspace}/runs/{run_id}/{action}')
 async def action(workspace:UUID,run_id:UUID,action:str,request:Request):
  session,ctx,row=await owned(request,workspace,write=True)
  repo=ResearchJobs(ctx.pool);prior=await repo.get(session['owner_id'],run_id);owned_run(prior,row,request)
  if action not in ('cancel','retry'):raise HTTPException(404,'Unknown analysis action')
  return owned_run(await getattr(repo,action)(session['owner_id'],run_id),row,request)
 @app.get('/api/analysis/workspaces/{workspace}/runs/{run_id}/events')
 async def events(workspace:UUID,run_id:UUID,request:Request,after:int=0):
  session,ctx,row=await owned(request,workspace)
  owned_run(await ResearchJobs(ctx.pool).get(session['owner_id'],run_id),row,request)
  async def stream():
   cursor=max(0,after)
   while not await request.is_disconnected():
    current=await owner_session(request)
    if not await PrivateWorkspaces(ctx).get(current['owner_id'],workspace):return
    run=await ResearchJobs(ctx.pool).get(current['owner_id'],run_id)
    if not run:return
    records=await ctx.pool.fetch('SELECT * FROM application.research_events WHERE run_id=$1 AND event_id>$2 ORDER BY event_id LIMIT 100',run_id,cursor)
    for event in records:
     cursor=event['event_id'];data=event['payload'] if isinstance(event['payload'],str) else json.dumps(event['payload'])
     yield f'id: {cursor}\nevent: {event["kind"]}\ndata: {data}\n\n'
    if run['status'] in ('completed','failed','cancelled') and len(records)<100:return
    if not records:yield ': progress\n\n'
    await asyncio.sleep(1)
  return StreamingResponse(stream(),media_type='text/event-stream',headers={'Cache-Control':'no-store','X-Accel-Buffering':'no'})
 @app.get('/api/analysis/workspaces/{workspace}/runs/{run_id}/report.md')
 async def report(workspace:UUID,run_id:UUID,request:Request):
  session,ctx,row=await owned(request,workspace)
  run=await ResearchJobs(ctx.pool).get(session['owner_id'],run_id);result=owned_run(run,row,request)
  if result['status']!='completed':raise HTTPException(409,'The report is not complete')
  text=result['report']+'\n\nSources\n'+''.join('\n- ['+s['citation_id']+'] '+s['uri'] for s in result['sources'])
  return Response(text,media_type='text/markdown',headers={'Cache-Control':'no-store','Content-Disposition':'attachment; filename="document-analysis.md"'})
