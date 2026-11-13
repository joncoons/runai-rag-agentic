"""Expiry reaper for isolated private state; permanent stores are never scanned."""
import asyncio,json,logging,re,shutil
from pathlib import Path
from uuid import UUID
from redis.asyncio import Redis
from collection_control.repository import Conflict
LOG=logging.getLogger(__name__)

async def purge_rows(pool,workspace,manifest):
 scope={'workspace_id':([workspace['workspace_id']],'uuid'),'collection_id':([workspace['collection_id']],'text'),
  'corpus_namespace':([workspace['collection_id']],'text'),
  'job_id':([UUID(x) for x in manifest['jobs']],'uuid'),'parent_job_id':([UUID(x) for x in manifest['jobs']],'uuid'),
  'document_id':(manifest['documents'],'text'),'source_revision_id':(manifest['revisions'],'text'),
  'source_id':(manifest['sources'],'text'),'operation_id':([UUID(x) for x in manifest['operations']],'uuid'),
  'run_id':([UUID(x) for x in manifest['runs']],'uuid'),
  'stage_key':(manifest['stages'],'text'),'caption_stage_key':(manifest['stages'],'text'),'idempotency_key':(manifest['stages'],'text')}
 columns=await pool.fetch("""SELECT table_schema,table_name,column_name,udt_name FROM information_schema.columns
  WHERE table_schema IN ('application','collection_control','caption_control','public') AND table_name NOT LIKE 'pg_%'""")
 tables={}
 for r in columns:tables.setdefault((r['table_schema'],r['table_name']),{})[r['column_name']]=r['udt_name']
 pending={}
 for table,cols in tables.items():
  for key in ('workspace_id','collection_id','corpus_namespace','job_id','parent_job_id','document_id','operation_id','run_id','source_revision_id','source_id','stage_key','caption_stage_key','idempotency_key'):
   if key in cols and cols[key]==scope[key][1] and scope[key][0]:pending[table]=(key,*scope[key]);break
 async with pool.acquire() as c,c.transaction():
  # All producer-generated deadletter envelopes retain the original job ID.
  if ('public','deadletters') in tables:
   for field in ('raw_fields','original_fields','envelope'):
    if tables[('public','deadletters')].get(field)=='jsonb':
     await c.execute(f'DELETE FROM public.deadletters WHERE "{field}"->>\'job_id\'=ANY($1::text[])',manifest['jobs'])
  for _ in range(len(pending)+1):
   advanced=False
   for (schema,table),(key,values,kind) in list(pending.items()):
    if not all(re.fullmatch('[a-z_][a-z0-9_]*',x) for x in (schema,table,key)):raise Conflict('Invalid schema identifier')
    try:
     async with c.transaction():await c.execute(f'DELETE FROM "{schema}"."{table}" WHERE "{key}"=ANY($1::{kind}[])',values)
    except Exception as error:
     if getattr(error,'sqlstate',None)=='23503':continue
     raise
    del pending[(schema,table)];advanced=True
   if not pending:break
   if not advanced:raise Conflict('Private cleanup has unresolved foreign-key references')

async def capture_manifest(base,workspace):
 p=base.pool;cid=workspace['collection_id']
 jobs=[str(x['job_id']) for x in await p.fetch('SELECT job_id FROM public.jobs WHERE corpus_namespace=$1',cid)]
 ids=[UUID(x) for x in jobs]
 return {'jobs':jobs,'documents':[r['document_id'] for r in await p.fetch('SELECT document_id FROM public.documents WHERE job_id=ANY($1::uuid[])',ids)],
  'stages':[r['idempotency_key'] for r in await p.fetch('SELECT idempotency_key FROM public.stage_units WHERE job_id=ANY($1::uuid[])',ids)],
  'operations':[str(r['operation_id']) for r in await p.fetch('SELECT operation_id FROM collection_control.operations WHERE collection_id=$1',cid)],
  'runs':[str(r['run_id']) for r in await p.fetch('SELECT run_id FROM application.research_runs WHERE workspace_id=$1',workspace['workspace_id'])],
  'sources':[r['source_id'] for r in await p.fetch('SELECT source_id FROM application.sources WHERE collection_id=$1',cid)],
  'revisions':[r['source_revision_id'] for r in await p.fetch('SELECT source_revision_id FROM application.source_revisions WHERE collection_id=$1',cid)]}

async def purge_queue(redis,manifest):
 jobs=set(manifest['jobs']);runs=set(manifest['runs'])
 async for stream in redis.scan_iter(match='rag.*'):
  if await redis.type(stream) not in (b'stream','stream'):continue
  cursor='-'
  while True:
   rows=await redis.xrange(stream,min=cursor,max='+',count=256)
   if not rows:break
   deleted=[]
   for identity,fields in rows:
    job=fields.get(b'job_id',fields.get('job_id',b''));run=fields.get(b'run_id',fields.get('run_id',b''))
    if isinstance(job,bytes):job=job.decode()
    if isinstance(run,bytes):run=run.decode()
    if job in jobs or run in runs:deleted.append(identity)
   if deleted:
    for group in await redis.xinfo_groups(stream):await redis.xack(stream,group.get('name',group.get(b'name')),*deleted)
    await redis.xdel(stream,*deleted)
   last=rows[-1][0];cursor='('+(last.decode() if isinstance(last,bytes) else last)

async def purge_indexes(base,manifest):
 # Only names derived from this workspace's recorded operation UUIDs qualify.
 prefixes=['rag_analysis_'+UUID(x).hex+'_' for x in manifest['operations']]
 if not prefixes:return
 endpoint=base.settings.es_endpoint.rstrip('/')
 # Index metadata needs only the scoped private-index privileges. The CAT API
 # would require cluster monitoring rights that private workers do not hold.
 async with base.es_http.get(endpoint+'/rag_analysis_*?allow_no_indices=true&expand_wildcards=all&filter_path=*.aliases') as response:
  if response.status!=200:raise Conflict('Private index cleanup could not list its projection')
  indices=await response.json()
 for name in indices:
  if any(name.startswith(prefix) for prefix in prefixes):
   from collection_control.stores import require_handle
   require_handle(name,'es')
   async with base.es_http.delete(endpoint+'/'+name) as response:
    if response.status not in (200,404):raise Conflict('Private index deletion is pending')

def purge_directory(root,workspace):
 target=root/str(workspace['workspace_id'])
 if target.parent.resolve()!=root.resolve() or target.is_symlink():raise Conflict('Unsafe private cleanup root')
 if target.exists():shutil.rmtree(target)

async def reap(base,redis):
 await base.pool.execute("UPDATE private_control.workspaces SET state='expired',expired_at=COALESCE(expired_at,clock_timestamp()) WHERE state='active' AND expires_at<=clock_timestamp()")
 rows=await base.pool.fetch("SELECT * FROM private_control.workspaces WHERE state IN ('expired','purged') ORDER BY expires_at")
 for row in rows:
  workspace=dict(row)
  try:
   manifest=workspace['cleanup_manifest'];manifest=json.loads(manifest) if isinstance(manifest,str) else manifest
   if not manifest:
    # Freeze publication and snapshot cleanup identities before deleting content.
    async with base.pool.acquire() as c,c.transaction():
     await c.execute('UPDATE collection_control.collections SET deleted=true WHERE collection_id=$1',workspace['collection_id'])
     manifest=await capture_manifest(base,workspace)
     await c.execute('UPDATE private_control.workspaces SET cleanup_manifest=$2::jsonb WHERE workspace_id=$1',workspace['workspace_id'],json.dumps(manifest))
   # Killable native subprocesses and request handlers observe expiry each second.
   age=await base.pool.fetchval('SELECT extract(epoch FROM clock_timestamp()-expired_at) FROM private_control.workspaces WHERE workspace_id=$1',workspace['workspace_id'])
   if age is not None and age<15:continue
   await purge_indexes(base,manifest);await purge_queue(redis,manifest)
   if workspace['state']!='purged':await purge_rows(base.pool,workspace,manifest)
   await asyncio.to_thread(purge_directory,base.settings.root,workspace)
   await base.pool.execute("UPDATE private_control.workspaces SET state='purged',filename='',raw_sha256=NULL,initial_question='',cleanup_error=NULL WHERE workspace_id=$1",workspace['workspace_id'])
  except Exception as error:
   await base.pool.execute('UPDATE private_control.workspaces SET cleanup_error=$2 WHERE workspace_id=$1',workspace['workspace_id'],type(error).__name__)
   LOG.warning('private_cleanup_retry error_type=%s',type(error).__name__)

async def cleanup_loop(base,stop):
 redis=Redis.from_url(base.settings.redis_url)
 try:
  while not stop.is_set():
   await reap(base,redis)
   try:await asyncio.wait_for(stop.wait(),timeout=10)
   except TimeoutError:pass
 finally:await redis.aclose()
