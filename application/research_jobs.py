"""Owned durable research jobs; PostgreSQL authority, Redis notification only."""
import hashlib,json
from datetime import timedelta
from uuid import uuid4
from collection_control.artifacts import canonical
from collection_control.repository import Conflict


def decoded(row):
 if not row:return None
 value=dict(row)
 for key in ('request','checkpoint'):
  if isinstance(value.get(key),str):value[key]=json.loads(value[key])
 return value


class ResearchJobs:
 def __init__(self,pool):self.pool=pool
 async def _event(self,c,run,kind,payload=None):
  await c.execute('INSERT INTO application.research_events(run_id,kind,payload) VALUES($1,$2,$3::jsonb)',run,kind,json.dumps(payload or {}))
 async def create(self,owner,key,request,*,workspace_id=None,expires_at=None):
  if not isinstance(key,str) or not 1<=len(key)<=128:raise Conflict('Provide a request key of at most 128 characters')
  sha=hashlib.sha256(canonical(request)).hexdigest()
  async with self.pool.acquire() as c,c.transaction():
   row=await c.fetchrow("""INSERT INTO application.research_runs(run_id,owner_id,workspace_id,request_key,request_sha256,request,expires_at)
    VALUES($1,$2,$3,$4,$5,$6::jsonb,COALESCE($7,clock_timestamp()+interval '7 days'))
    ON CONFLICT(owner_id,request_key) DO NOTHING RETURNING *""",uuid4(),owner,workspace_id,key,sha,json.dumps(request),expires_at)
   if not row:
    row=await c.fetchrow('SELECT * FROM application.research_runs WHERE owner_id=$1 AND request_key=$2',owner,key)
    if row['request_sha256']!=sha or row['workspace_id']!=workspace_id:raise Conflict('Request key was already used for different research')
   else:
    await self._event(c,row['run_id'],'queued')
    await c.execute('INSERT INTO application.research_outbox(run_id) VALUES($1)',row['run_id'])
   return decoded(row)
 async def get(self,owner,run):
  return decoded(await self.pool.fetchrow('SELECT r.*,(SELECT COALESCE(max(event_id),0) FROM application.research_events e WHERE e.run_id=r.run_id) AS event_cursor FROM application.research_runs r WHERE run_id=$1 AND owner_id=$2 AND expires_at>clock_timestamp()',run,owner))
 async def listing(self,owner):
  return [decoded(r) for r in await self.pool.fetch('SELECT * FROM application.research_runs WHERE owner_id=$1 AND workspace_id IS NULL AND expires_at>clock_timestamp() ORDER BY created_at DESC LIMIT 100',owner)]
 async def claim(self,run=None,*,ttl=90):
  async with self.pool.acquire() as c,c.transaction():
   exhausted=await c.fetch("UPDATE application.research_runs SET status='failed',lease_token=NULL,lease_until=NULL,error_type='WorkerInterrupted' WHERE status='running' AND lease_until<=clock_timestamp() AND attempts>=max_attempts RETURNING run_id")
   for old in exhausted:await self._event(c,old['run_id'],'failed',{'error_type':'WorkerInterrupted'})
   row=await c.fetchrow("""SELECT * FROM application.research_runs WHERE ($1::uuid IS NULL OR run_id=$1)
    AND expires_at>clock_timestamp() AND attempts<max_attempts AND
    ((status='queued' AND next_attempt_at<=clock_timestamp()) OR (status='running' AND lease_until<=clock_timestamp()))
    ORDER BY next_attempt_at,created_at FOR UPDATE SKIP LOCKED LIMIT 1""",run)
   if not row:return None
   token=uuid4()
   result=await c.fetchrow("UPDATE application.research_runs SET status='running',attempts=attempts+1,lease_token=$2,lease_until=clock_timestamp()+$3::interval,report='',updated_at=clock_timestamp() WHERE run_id=$1 RETURNING *",row['run_id'],token,timedelta(seconds=ttl))
   if row['report']:await self._event(c,row['run_id'],'report_reset')
   await self._event(c,row['run_id'],'running',{'attempt':result['attempts']})
   return decoded(result)
 async def heartbeat(self,lease,*,ttl=90):
  return bool(await self.pool.fetchval("""UPDATE application.research_runs SET lease_until=clock_timestamp()+$3::interval
   WHERE run_id=$1 AND lease_token=$2 AND status='running' AND lease_until>clock_timestamp() AND expires_at>clock_timestamp() RETURNING true""",lease['run_id'],lease['lease_token'],timedelta(seconds=ttl)))
 async def _locked(self,c,lease):
  row=await c.fetchrow("""SELECT * FROM application.research_runs WHERE run_id=$1 AND lease_token=$2 AND status='running'
   AND lease_until>clock_timestamp() AND expires_at>clock_timestamp() FOR UPDATE""",lease['run_id'],lease['lease_token'])
  if not row:raise Conflict('Research job was cancelled, expired or reassigned')
  if row['workspace_id'] and not await c.fetchval("SELECT true FROM private_control.workspaces WHERE workspace_id=$1 AND owner_id=$2 AND state='active' AND expires_at>clock_timestamp() FOR SHARE",row['workspace_id'],row['owner_id']):raise Conflict('Private workspace expired or was deleted')
  return row
 async def checkpoint(self,lease,state,stage):
  async with self.pool.acquire() as c,c.transaction():
   await self._locked(c,lease)
   await c.execute('UPDATE application.research_runs SET checkpoint=$2::jsonb,updated_at=clock_timestamp() WHERE run_id=$1',lease['run_id'],json.dumps(state))
   await self._event(c,lease['run_id'],'progress',{'stage':stage,'completed_queries':len(state.get('completed_queries',[])),'planned_queries':len(state.get('queries',[])),'sources':len(state.get('sources',[]))})
 async def append(self,lease,text):
  if not isinstance(text,str) or not text:return
  async with self.pool.acquire() as c,c.transaction():
   await self._locked(c,lease)
   await c.execute('UPDATE application.research_runs SET report=report||$2,updated_at=clock_timestamp() WHERE run_id=$1',lease['run_id'],text)
   await self._event(c,lease['run_id'],'delta',{'text':text})
 async def complete(self,lease):
  async with self.pool.acquire() as c,c.transaction():
   row=await self._locked(c,lease)
   if not row['report'].strip():raise Conflict('Research report is empty')
   await c.execute("UPDATE application.research_runs SET status='completed',lease_token=NULL,lease_until=NULL,updated_at=clock_timestamp(),error_type=NULL WHERE run_id=$1",lease['run_id'])
   await self._event(c,lease['run_id'],'completed')
 async def fail(self,lease,category):
  async with self.pool.acquire() as c,c.transaction():
   try:row=await self._locked(c,lease)
   except Conflict:return
   status='failed' if row['attempts']>=row['max_attempts'] else 'queued'
   await c.execute("UPDATE application.research_runs SET status=$2,error_type=$3,lease_token=NULL,lease_until=NULL,next_attempt_at=clock_timestamp()+$4::interval,updated_at=clock_timestamp() WHERE run_id=$1",lease['run_id'],status,category,timedelta(seconds=min(60,2**row['attempts'])))
   await self._event(c,lease['run_id'],status,{'error_type':category})
 async def cancel(self,owner,run):
  async with self.pool.acquire() as c,c.transaction():
   row=await c.fetchrow('SELECT * FROM application.research_runs WHERE run_id=$1 AND owner_id=$2 AND expires_at>clock_timestamp() FOR UPDATE',run,owner)
   if not row:return None
   if row['status'] in ('completed','cancelled'):return decoded(row)
   row=await c.fetchrow("UPDATE application.research_runs SET status='cancelled',lease_token=NULL,lease_until=NULL,updated_at=clock_timestamp() WHERE run_id=$1 RETURNING *",run)
   await self._event(c,run,'cancelled');return decoded(row)
 async def retry(self,owner,run):
  async with self.pool.acquire() as c,c.transaction():
   row=await c.fetchrow('SELECT * FROM application.research_runs WHERE run_id=$1 AND owner_id=$2 AND expires_at>clock_timestamp() FOR UPDATE',run,owner)
   if not row:return None
   if row['status'] not in ('failed','cancelled'):raise Conflict('Only failed or stopped research can be resumed')
   row=await c.fetchrow("UPDATE application.research_runs SET status='queued',attempts=0,report='',error_type=NULL,next_attempt_at=clock_timestamp(),updated_at=clock_timestamp() WHERE run_id=$1 RETURNING *",run)
   await c.execute('INSERT INTO application.research_outbox(run_id) VALUES($1) ON CONFLICT(run_id) DO UPDATE SET published_at=NULL',run)
   await self._event(c,run,'report_reset');await self._event(c,run,'queued');return decoded(row)
