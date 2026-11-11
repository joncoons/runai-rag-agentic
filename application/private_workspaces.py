"""Session-owned, expiring workspaces backed by an isolated native pipeline."""
import hashlib,json,os
from datetime import timedelta
from pathlib import Path
from uuid import UUID,uuid4
from collection_control.artifacts import canonical
from collection_control.repository import Conflict
from .private_context import scoped_context

class PrivateWorkspaces:
 def __init__(self,context):self.context=context;self.pool=context.pool;self.root=context.settings.root
 async def get(self,owner,workspace,*,active=True):
  row=await self.pool.fetchrow("SELECT * FROM private_control.workspaces WHERE workspace_id=$1 AND owner_id=$2"+(" AND state='active' AND expires_at>clock_timestamp()" if active else ''),workspace,owner)
  return dict(row) if row else None
 async def by_job(self,job):
  row=await self.pool.fetchrow("SELECT w.* FROM private_control.workspaces w JOIN public.jobs j ON j.corpus_namespace=w.collection_id WHERE j.job_id=$1 AND w.state='active' AND w.expires_at>clock_timestamp()",UUID(str(job)))
  return dict(row) if row else None
 async def by_collection(self,collection):
  row=await self.pool.fetchrow("SELECT * FROM private_control.workspaces WHERE collection_id=$1 AND state='active' AND expires_at>clock_timestamp()",collection)
  return dict(row) if row else None
 async def listing(self,owner):
  return [dict(r) for r in await self.pool.fetch("SELECT * FROM private_control.workspaces WHERE owner_id=$1 AND state='active' AND expires_at>clock_timestamp() ORDER BY created_at DESC",owner)]
 async def create(self,owner,filename,raw,*,captions=False,question='Summarize this document, its key facts and material qualifications.'):
  from .intake import inspect_upload,submit
  import asyncio
  await asyncio.to_thread(inspect_upload,filename,raw,captions_enabled=captions)
  workspace=uuid4();collection='analysis-'+workspace.hex
  lifetime=int(os.environ.get('PRIVATE_WORKSPACE_LIFETIME_SECONDS','86400'))
  if not 60<=lifetime<=604800:raise ValueError('Workspace lifetime must be 60 seconds to 7 days')
  row=await self.pool.fetchrow("""INSERT INTO private_control.workspaces(workspace_id,owner_id,collection_id,filename,raw_sha256,initial_question,expires_at)
   VALUES($1,$2,$3,$4,$5,$6,clock_timestamp()+$7::interval) RETURNING *""",workspace,owner,collection,filename,hashlib.sha256(raw).hexdigest(),question,timedelta(seconds=lifetime))
  scoped=scoped_context(self.context,row)
  for path in (scoped.settings.raw,scoped.settings.stages,scoped.settings.snapshots):path.mkdir(parents=True,exist_ok=True,mode=0o700)
  try:
   await self.context.control.create_collection(collection,hashlib.sha256(canonical(self.context.settings.contract)).hexdigest())
   await self.pool.execute('INSERT INTO application.collections(collection_id,name) VALUES($1,$2)',collection,filename)
   job=await submit(scoped,collection,[(filename,raw)],captions_enabled=captions)
   await self.pool.execute('UPDATE private_control.workspaces SET job_id=$2 WHERE workspace_id=$1',workspace,UUID(job))
  except BaseException:
   await self.pool.execute("UPDATE private_control.workspaces SET state='expired',expired_at=clock_timestamp() WHERE workspace_id=$1",workspace)
   raise
  return await self.get(owner,workspace)
 async def expire(self,owner,workspace):
  return bool(await self.pool.fetchval("UPDATE private_control.workspaces SET state='expired',expired_at=clock_timestamp() WHERE workspace_id=$1 AND owner_id=$2 AND state='active' RETURNING true",workspace,owner))
 async def detail(self,owner,workspace):
  row=await self.get(owner,workspace)
  if not row:return None
  source=await self.pool.fetchrow('SELECT status,records,raw_uri,source_revision_id FROM application.source_revisions WHERE collection_id=$1 ORDER BY observed_at DESC LIMIT 1',row['collection_id'])
  publication=await self.pool.fetchrow('SELECT c.current_operation,o.status FROM collection_control.collections c LEFT JOIN collection_control.operations o ON o.operation_id=c.current_operation WHERE c.collection_id=$1',row['collection_id'])
  row['processing_status']=source['status'] if source else 'queued'
  row['ready']=bool(publication and publication['current_operation'] and publication['status']=='published')
  row['records']=source['records'] if source else 0
  row['raw_uri']=source['raw_uri'] if source else None
  row['captions']=dict(await self.pool.fetchrow("""SELECT count(*) FILTER(WHERE s.status='committed') AS generated,
   count(*) FILTER(WHERE COALESCE(p.status,'')='published') AS published,
   count(*) FILTER(WHERE s.status<>'committed' AND a.terminal_reason IS NULL) AS pending
   FROM caption_control.assets a LEFT JOIN public.stage_units s ON s.idempotency_key=a.stage_key
   LEFT JOIN application.caption_publications p ON p.caption_stage_key=a.stage_key WHERE a.collection_id=$1""",row['collection_id']))
  if source and source['status']=='captured' and not source['records']:row['processing_status']='no_searchable_content'
  return row
