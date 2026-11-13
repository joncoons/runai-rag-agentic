"""Owned durable research routes; disconnecting a browser does not cancel work."""
import asyncio,json
from uuid import UUID
from fastapi import HTTPException,Request
from fastapi.responses import Response,StreamingResponse
from pydantic import BaseModel,ConfigDict,Field,StrictBool,model_validator
from typing import Literal
from .local_auth import SessionStore,COOKIE,csrf
from .research_jobs import ResearchJobs

async def owner_session(request,*,write=False):
 from .api import _context
 store=SessionStore(_context(request).pool)
 session=await store.lookup(request.cookies.get(COOKIE))
 if not session:raise HTTPException(401,'Refresh this page to start a private session')
 if write:csrf(request,session)
 return session

def public_run(row):
 if row is None:raise HTTPException(404,'Research is unavailable in this session')
 state=row['checkpoint'];sources=[{k:v for k,v in s.items() if k!='text'} for s in state.get('sources',[])]
 return {'id':str(row['run_id']),'status':row['status'],'question':row['request']['question'],
  'backend':row['request']['backend'],'collection_ids':row['request']['collection_ids'],
  'include_web':row['request'].get('include_web',False),'search_budget':row['request']['search_budget'],'queries':state.get('queries',[]),
  'completed_queries':state.get('completed_queries',[]),'sources':sources,
  'warnings':state.get('warnings',[]),'report':row['report'],'error_type':row['error_type'],
  'attempts':row['attempts'],'event_cursor':row.get('event_cursor',0),'expires_at':row['expires_at'].isoformat()}

class ResearchRequest(BaseModel):
 model_config=ConfigDict(extra='forbid')
 question:str=Field(min_length=1,max_length=131072,strict=True)
 collection_ids:list[str]=Field(default_factory=list,max_length=12)
 include_web:StrictBool=False
 backend:Literal['elasticsearch','milvus']='elasticsearch'
 search_budget:int=Field(default=6,ge=1,strict=True)
 request_key:str=Field(min_length=1,max_length=128,strict=True)
 @model_validator(mode='after')
 def evidence_scope(self):
  if not self.collection_ids and not self.include_web:raise ValueError('Select collections or explicitly enable web search')
  return self

def install_research_routes(app,service_for):
 from .api import _context
 def repo(request):return ResearchJobs(_context(request).pool)
 @app.post('/api/research/runs',status_code=202)
 async def create(body:ResearchRequest,request:Request):
  session=await owner_session(request,write=True)
  from .web_research import enabled
  if body.include_web and not enabled():raise HTTPException(503,'Web search is not enabled for this installation')
  query=service_for(request);query._validate(body.question,body.collection_ids,body.backend,'deep_research')
  if body.collection_ids and query.catalog is None:
   # Validate scope once at job admission. MCP-backed search intentionally has
   # no API-side background catalog and remains the evidence authority.
   from .query_catalog import QueryCatalog
   from .query import RAGQuery
   ctx=_context(request);catalog=QueryCatalog(ctx.pool,allowed_collection_ids=body.collection_ids)
   await catalog.refresh();validator=RAGQuery(ctx.settings,ctx.pool,ctx.http,catalog=catalog)
  else:validator=query
  if body.collection_ids:await validator._resolve(body.collection_ids,body.backend)
  payload=body.model_dump(exclude={'request_key'})
  if not body.include_web:payload.pop('include_web') # preserve existing local request/idempotency hashes
  row=await repo(request).create(session['owner_id'],body.request_key,payload)
  return public_run(row)
 @app.get('/api/research/runs')
 async def listing(request:Request):
  session=await owner_session(request)
  from .web_research import enabled
  return {'runs':[public_run(r) for r in await repo(request).listing(session['owner_id'])],'capabilities':{'web_search':enabled()}}
 @app.get('/api/research/runs/{run_id}')
 async def get(run_id:UUID,request:Request):
  session=await owner_session(request)
  return public_run(await repo(request).get(session['owner_id'],run_id))
 @app.post('/api/research/runs/{run_id}/{action}')
 async def action(run_id:UUID,action:str,request:Request):
  session=await owner_session(request,write=True)
  if action not in ('cancel','retry'):raise HTTPException(404,'Unknown research action')
  return public_run(await getattr(repo(request),action)(session['owner_id'],run_id))
 @app.get('/api/research/runs/{run_id}/report.md')
 async def download(run_id:UUID,request:Request):
  session=await owner_session(request);row=await repo(request).get(session['owner_id'],run_id)
  if not row:raise HTTPException(404,'Research is unavailable')
  if row['status']!='completed':raise HTTPException(409,'The report is not complete')
  sources=row['checkpoint'].get('sources',[])
  text=row['report']+'\n\nEvidence scope: '+('Tavily web search'+(' and selected collections' if row['request']['collection_ids'] else '') if row['request'].get('include_web',False) else 'selected collections only')+'\n\nSources\n'+''.join('\n- ['+s['citation_id']+'] '+str(s.get('title') or s.get('uri') or s['record_id'])+(' [external web excerpt]' if s.get('evidence_origin')=='web_search' else ' [collection]')+(' — '+s['uri'] if s.get('uri') else '') for s in sources)
  return Response(text,media_type='text/markdown',headers={'Cache-Control':'no-store','Content-Disposition':'attachment; filename="research-report.md"'})
 @app.get('/api/research/runs/{run_id}/events')
 async def events(run_id:UUID,request:Request,after:int=0):
  session=await owner_session(request)
  if not await repo(request).get(session['owner_id'],run_id):raise HTTPException(404,'Research is unavailable')
  async def stream():
   cursor=max(0,after)
   while not await request.is_disconnected():
    try:current=await owner_session(request)
    except HTTPException:return
    row=await repo(request).get(current['owner_id'],run_id)
    if not row:return
    rows=await _context(request).pool.fetch('SELECT * FROM application.research_events WHERE run_id=$1 AND event_id>$2 ORDER BY event_id LIMIT 100',run_id,cursor)
    for event in rows:
     cursor=event['event_id'];payload=event['payload']
     if not isinstance(payload,str):payload=json.dumps(payload)
     yield f'id: {cursor}\nevent: {event["kind"]}\ndata: {payload}\n\n'
    if row['status'] in ('completed','cancelled','failed') and len(rows)<100:return
    if not rows:yield ': progress\n\n'
    await asyncio.sleep(1)
  return StreamingResponse(stream(),media_type='text/event-stream',headers={'Cache-Control':'no-store','X-Accel-Buffering':'no'})
