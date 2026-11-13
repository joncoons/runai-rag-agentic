"""Independent research executor with outbox delivery and lease recovery."""
import asyncio,json,logging,os,signal,socket
from uuid import UUID
from redis.asyncio import Redis
from .context import Context
from .settings import Settings
from .query import RAGQuery
from .query_catalog import QueryCatalog
from .research_jobs import ResearchJobs
from .research_engine import ResearchEngine,run_owned
LOG=logging.getLogger(__name__)

async def notify(pool,redis,stream):
 async with pool.acquire() as c,c.transaction():
  rows=await c.fetch('SELECT run_id FROM application.research_outbox WHERE published_at IS NULL FOR UPDATE SKIP LOCKED LIMIT 32')
  for row in rows:
   await redis.xadd(stream,{'run_id':str(row['run_id'])})
   await c.execute('UPDATE application.research_outbox SET published_at=clock_timestamp() WHERE run_id=$1',row['run_id'])

async def serve(pool,redis,query,stop,*,stream='rag.agentic.research.pending',execute_lease=None):
 repo=ResearchJobs(pool);engine=ResearchEngine(repo,query);execute_lease=execute_lease or (lambda lease:run_owned(repo,lease,engine));group='research-v1';consumer=socket.gethostname()
 try:await redis.xgroup_create(stream,group,id='0-0',mkstream=True)
 except Exception as error:
  if 'BUSYGROUP' not in str(error):raise
 while not stop.is_set():
  try:
   await notify(pool,redis,stream)
   entries=await redis.xreadgroup(group,consumer,{stream:'>'},count=1,block=500)
   notification=None
   if entries:
    _,messages=entries[0];notification=messages[0][0];fields=messages[0][1]
    raw=fields.get(b'run_id',fields.get('run_id'))
    lease=await repo.claim(UUID(raw.decode() if isinstance(raw,bytes) else raw))
   else:lease=await repo.claim() # recover a missed notification or expired process lease
   if lease:
    task=asyncio.create_task(execute_lease(lease));stopping=asyncio.create_task(stop.wait())
    await asyncio.wait({task,stopping},return_when=asyncio.FIRST_COMPLETED)
    if stopping.done() and not task.done():task.cancel()
    stopping.cancel();await asyncio.gather(task,stopping,return_exceptions=True)
   if notification:await redis.xack(stream,group,notification)
  except asyncio.CancelledError:raise
  except Exception as error:
   LOG.warning('research_retry error_type=%s',type(error).__name__)
   try:await asyncio.wait_for(stop.wait(),timeout=2)
   except TimeoutError:pass

async def main():
 from .web_research import enabled
 if enabled() and not os.environ.get('TAVILY_API_KEY','').strip():raise RuntimeError('Enabled web research requires its worker credential')
 ctx=await Context(Settings.load()).open();catalog=QueryCatalog(ctx.pool,snapshots=ctx.settings.snapshots)
 redis=Redis.from_url(ctx.settings.redis_url);stop=asyncio.Event();health=None
 try:
  await ctx.pool.fetchval('SELECT count(*) FROM application.research_runs');await catalog.start()
  from nv_ingest_durable.pipeline_worker import HealthServer
  health=HealthServer(ctx.pool,redis,int(os.environ.get('RESEARCH_HEALTH_PORT','8089')))
  await health.start()
  for sig in (signal.SIGINT,signal.SIGTERM):asyncio.get_running_loop().add_signal_handler(sig,stop.set)
  await serve(ctx.pool,redis,RAGQuery(ctx.settings,ctx.pool,ctx.http,catalog=catalog),stop)
 finally:
  if health:await health.stop()
  await catalog.close();await redis.aclose();await ctx.close()
if __name__=='__main__':logging.basicConfig(level=logging.INFO);asyncio.run(main())
