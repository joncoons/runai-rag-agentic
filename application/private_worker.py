"""Independent private analysis workers sharing models, never permanent state."""
import asyncio,json,logging,os,signal,socket,sys
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4,UUID
from collection_control.repository import Conflict
from nv_ingest_durable.artifacts import FileArtifactStore
from nv_ingest_durable.contracts import StageEnvelope,PendingArtifact,StageResult,WorkerOutcome
from nv_ingest_durable.postgres import PostgresManifestRepository
from nv_ingest_durable.caption_repository import CaptionManifestRepository
from nv_ingest_durable.caption_worker import CaptionStageWorker
from nv_ingest_durable.redis_streams import RedisStreamsTransport
from nv_ingest_durable.worker import DurableStageWorker,OutboxRelay
from nv_ingest_durable.service import StageWorkerService
from .private_context import open_private,scoped_context
from .private_workspaces import PrivateWorkspaces
from .worker import ApplicationWorker
LOG=logging.getLogger(__name__)

async def scoped_call(workspaces,workspace,awaitable):
 task=asyncio.create_task(awaitable)
 try:
  while not task.done():
   if not await workspaces.get(workspace['owner_id'],workspace['workspace_id']):
    task.cancel();await asyncio.gather(task,return_exceptions=True);raise Conflict('Private workspace expired')
   await asyncio.wait({task},timeout=1)
  return await task
 finally:
  if not task.done():task.cancel();await asyncio.gather(task,return_exceptions=True)

class Compute:
 def __init__(self,base,kind):self.base=base;self.workspaces=PrivateWorkspaces(base);self.kind=kind
 async def process(self,envelope):
  workspace=await self.workspaces.by_job(envelope.job_id)
  if not workspace:raise Conflict('Private job expired')
  stages=self.base.settings.root/str(workspace['workspace_id'])/'stages';stages.mkdir(parents=True,exist_ok=True)
  path=stages/('compute-'+uuid4().hex+'.json')
  process=await asyncio.create_subprocess_exec(sys.executable,'-m','application.private_compute',stdin=asyncio.subprocess.PIPE,stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL)
  try:
   payload=json.dumps({'workspace_id':str(workspace['workspace_id']),'kind':self.kind,'result_path':str(path),'envelope':envelope.to_dict()}).encode()
   await scoped_call(self.workspaces,workspace,process.communicate(payload))
   if process.returncode:raise RuntimeError('Private native compute failed')
   body=json.loads(path.read_text());artifact=PendingArtifact(**body['artifact'])
   return StageResult(artifact=artifact,downstream=tuple((s,StageEnvelope.from_stream_fields(e)) for s,e in body['downstream']))
  finally:
   if process.returncode is None:
    process.terminate()
    try:await asyncio.wait_for(process.wait(),5)
    except TimeoutError:process.kill();await process.wait()
   path.unlink(missing_ok=True)

class ActiveWorker:
 def __init__(self,worker,base):self.worker=worker;self.transport=worker.transport;self.workspaces=PrivateWorkspaces(base)
 async def handle(self,message):
  if not await self.workspaces.by_job(message.envelope.job_id):
   await self.transport.acknowledge(message);return WorkerOutcome.DEADLETTERED
  return await self.worker.handle(message)

async def service_loop(base,stop,kind):
 transport=RedisStreamsTransport.from_url(base.settings.redis_url)
 repo=CaptionManifestRepository(PostgresManifestRepository(base.pool),base.settings.root)
 worker=DurableStageWorker(owner=socket.gethostname(),manifests=repo,artifacts=FileArtifactStore(base.settings.root),processor=Compute(base,kind),transport=transport,lease_ttl=timedelta(seconds=180),heartbeat_interval=20,max_attempts=4,deadletter_stream='rag.agentic.'+kind+'.deadletter')
 if kind=='caption':worker=CaptionStageWorker(worker,repo)
 worker=ActiveWorker(worker,base)
 service=StageWorkerService(stream='rag.agentic.'+('caption.pending' if kind=='caption' else 'extraction.pending'),group='private-'+kind+'-v1',consumer=socket.gethostname(),worker=worker,max_concurrency=1,read_count=1,block_ms=500,reclaim_idle_ms=30000)
 try:await service.run(stop)
 finally:await transport.client.aclose()

async def relay_loop(base,stop):
 worker=ApplicationWorker(base)
 repo=CaptionManifestRepository(PostgresManifestRepository(base.pool),base.settings.root)
 relay=OutboxRelay(repo,worker.transport)
 try:
  while not stop.is_set():
   await worker.relay_once();await relay.publish_once();await asyncio.sleep(.5)
 finally:await worker.transport.client.aclose()

async def admission_loop(base,stop,kind):
 driver=ApplicationWorker(base);spaces=PrivateWorkspaces(base)
 async def handle(message):
  workspace=await spaces.by_job(message.envelope.job_id)
  if not workspace:return # expiry is the durable terminal disposition
  worker=ApplicationWorker(scoped_context(base,workspace))
  try:await scoped_call(spaces,workspace,getattr(worker,kind)(message))
  finally:await worker.transport.client.aclose()
 try:
  while not stop.is_set():
   await driver.consume_once('rag.agentic.'+('caption.results' if kind=='caption' else 'index'),handle,batch=16)
   await asyncio.sleep(.1)
 finally:await driver.transport.client.aclose()

async def writer_loop(base,stop):
 spaces=PrivateWorkspaces(base)
 while not stop.is_set():
  rows=await base.pool.fetch("SELECT o.* FROM collection_control.operations o WHERE o.status='pending' ORDER BY o.operation_id LIMIT 8")
  for row in rows:
   workspace=await spaces.by_collection(row['collection_id'])
   if not workspace:continue
   context=scoped_context(base,workspace)
   try:await scoped_call(spaces,workspace,context.writer.run(row['operation_id'],ttl_seconds=180))
   except Exception as error:LOG.warning('private_writer_retry error_type=%s',type(error).__name__)
   worker=ApplicationWorker(context)
   try:
    await worker.reconcile_publications();await worker.refresh_jobs()

   finally:await worker.transport.client.aclose()
  await initial_analysis_once(base)
  await asyncio.sleep(.5)

async def initial_analysis_once(base):
 # Reconcile independently of pending operations: publication may have committed
 # immediately before a process crash. The unique request key makes replay safe.
 from .research_jobs import ResearchJobs
 spaces=PrivateWorkspaces(base)
 rows=await base.pool.fetch("SELECT w.* FROM private_control.workspaces w JOIN collection_control.collections c USING(collection_id) JOIN collection_control.operations o ON o.operation_id=c.current_operation WHERE w.state='active' AND w.expires_at>clock_timestamp() AND w.initial_run_id IS NULL AND o.status='published'")
 for workspace in rows:
  worker=ApplicationWorker(scoped_context(base,workspace))
  try:
   await worker.reconcile_publications();await worker.refresh_jobs()
   live=await spaces.detail(workspace['owner_id'],workspace['workspace_id'])
   if not live or not live['ready']:continue
   run=await ResearchJobs(base.pool).create(live['owner_id'],'initial-'+str(live['workspace_id']),{'question':live['initial_question'],'collection_ids':[live['collection_id']],'backend':'elasticsearch','search_budget':1},workspace_id=live['workspace_id'],expires_at=live['expires_at'])
   await base.pool.execute('UPDATE private_control.workspaces SET initial_run_id=$2 WHERE workspace_id=$1 AND initial_run_id IS NULL',live['workspace_id'],run['run_id'])
  finally:await worker.transport.client.aclose()

async def private_research_loop(base,stop):
 from .research_jobs import ResearchJobs
 from .research_engine import ResearchEngine,run_owned
 from .query import RAGQuery
 from .private_scope import sign
 from .research_worker import serve
 from redis.asyncio import Redis
 from .query_catalog import QueryCatalog
 spaces=PrivateWorkspaces(base);repo=ResearchJobs(base.pool);redis=Redis.from_url(base.settings.redis_url)
 class Query(RAGQuery):
  async def _remote_evidence(self,*args,**kwargs):
   live=await spaces.get(self.workspace['owner_id'],self.workspace['workspace_id'])
   if not live:raise Conflict('Private workspace expired')
   self.mcp_headers={'X-Analysis-Scope':sign(live)}
   return await super()._remote_evidence(*args,**kwargs)
 async def execute(lease):
  workspace=await spaces.get(lease['owner_id'],lease['workspace_id'])
  if not workspace:await repo.cancel(lease['owner_id'],lease['run_id']);return
  context=scoped_context(base,workspace)
  catalog=QueryCatalog(base.pool,snapshots=context.settings.snapshots,allowed_collection_ids=[workspace['collection_id']]);await catalog.refresh()
  query=Query(context.settings,base.pool,base.http,catalog=catalog);query.workspace=workspace;query.mcp_url=os.environ['PRIVATE_QUERY_MCP_URL']
  try:await scoped_call(spaces,workspace,run_owned(repo,lease,ResearchEngine(repo,query)))
  except Conflict:pass
 try:await serve(base.pool,redis,None,stop,stream='rag.private.research.pending',execute_lease=execute)
 finally:await redis.aclose()

async def supervise(coroutines,stop):
 running=[asyncio.create_task(c) for c in coroutines]
 stopping=asyncio.create_task(stop.wait())
 try:
  done,_=await asyncio.wait([*running,stopping],return_when=asyncio.FIRST_COMPLETED)
  for task in done:
   if task is not stopping:
    task.result() # worker failure must fail its process, not leave a healthy idle pod
    if not stop.is_set():raise RuntimeError('Private worker stopped unexpectedly')
 finally:
  stop.set()
  for task in [*running,stopping]:task.cancel()
  await asyncio.gather(*running,stopping,return_exceptions=True)

async def main():
 base=await open_private();stop=asyncio.Event();role=os.environ['PRIVATE_WORKER_ROLE']
 from nv_ingest_durable.pipeline_worker import HealthServer
 from redis.asyncio import Redis
 redis=Redis.from_url(base.settings.redis_url);health=HealthServer(base.pool,redis,int(os.environ.get('PRIVATE_HEALTH_PORT','8090')))
 for sig in (signal.SIGINT,signal.SIGTERM):asyncio.get_running_loop().add_signal_handler(sig,stop.set)
 tasks=[]
 try:
  await health.start()
  if role in ('extraction','caption'):tasks=[service_loop(base,stop,role)]
  elif role=='admission':tasks=[relay_loop(base,stop),admission_loop(base,stop,'source'),admission_loop(base,stop,'caption')]
  elif role=='writer':tasks=[writer_loop(base,stop)]
  elif role=='research':tasks=[private_research_loop(base,stop)]
  elif role=='cleanup':
   from .private_cleanup import cleanup_loop
   tasks=[cleanup_loop(base,stop)]
  else:raise ValueError('Unknown private worker role')
  await supervise(tasks,stop)
 finally:await health.stop();await redis.aclose();await base.close()
if __name__=='__main__':logging.basicConfig(level=logging.INFO);asyncio.run(main())
