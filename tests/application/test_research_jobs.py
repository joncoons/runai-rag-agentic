import asyncio,json,os,unittest
from pathlib import Path
from uuid import uuid4

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'disposable PostgreSQL required')
class ResearchJobsTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  import asyncpg
  from urllib.parse import urlsplit
  from application.research_jobs import ResearchJobs
  dsn=os.environ['COLLECTION_TEST_DSN'];self.assertIn(urlsplit(dsn).hostname,('127.0.0.1','localhost'))
  self.pool=await asyncpg.create_pool(dsn)
  self.assertEqual(tuple(await self.pool.fetchrow('SELECT current_database(),current_user')),('qualification','qualification'))
  await self.pool.execute('CREATE SCHEMA IF NOT EXISTS application')
  await self.pool.execute(Path('deployment/application/015_research_jobs.sql').read_text())
  await self.pool.execute('TRUNCATE application.research_runs CASCADE')
  self.addAsyncCleanup(self.pool.close);self.repo=ResearchJobs(self.pool);self.owner=uuid4()
 async def create(self,key='request'):
  return await self.repo.create(self.owner,key,{'question':'Compare terms','collection_ids':['one'],'backend':'elasticsearch','search_budget':3})
 async def test_owner_isolation_and_idempotency(self):
  from collection_control.repository import Conflict
  first=await self.create();second=await self.create()
  self.assertEqual(first['run_id'],second['run_id'])
  self.assertIsNone(await self.repo.get(uuid4(),first['run_id']))
  with self.assertRaises(Conflict):await self.repo.create(self.owner,'request',{'question':'different'})
 async def test_checkpoints_survive_restart_and_stale_owner_is_fenced(self):
  first=await self.create();lease=await self.repo.claim(first['run_id'])
  await self.repo.checkpoint(lease,{'queries':['a','b'],'completed_queries':['a']},'evidence')
  await self.pool.execute("UPDATE application.research_runs SET lease_until=clock_timestamp()-interval '1 second'")
  current=await self.repo.claim(first['run_id'])
  self.assertEqual(current['checkpoint']['completed_queries'],['a'])
  from collection_control.repository import Conflict
  with self.assertRaises(Conflict):await self.repo.append(lease,'stale text')
  await self.repo.append(current,'answer');await self.repo.complete(current)
  self.assertEqual((await self.repo.get(self.owner,first['run_id']))['report'],'answer')
 async def test_cancel_fences_late_report_and_retry_preserves_evidence(self):
  from collection_control.repository import Conflict
  run=await self.create();lease=await self.repo.claim(run['run_id'])
  await self.repo.checkpoint(lease,{'completed_queries':['a']},'evidence')
  await self.repo.append(lease,'partial')
  await self.repo.cancel(self.owner,run['run_id'])
  with self.assertRaises(Conflict):await self.repo.complete(lease)
  await self.repo.retry(self.owner,run['run_id']);again=await self.repo.claim(run['run_id'])
  self.assertEqual(again['report'],'');self.assertEqual(again['checkpoint']['completed_queries'],['a'])
 async def test_retry_budget_is_finite_and_expiry_fences_access(self):
  run=await self.create()
  for _ in range(3):
   lease=await self.repo.claim(run['run_id']);await self.repo.fail(lease,'EndpointUnavailable')
   await self.pool.execute('UPDATE application.research_runs SET next_attempt_at=clock_timestamp()')
  self.assertEqual((await self.repo.get(self.owner,run['run_id']))['status'],'failed')
  self.assertIsNone(await self.repo.claim(run['run_id']))
  await self.pool.execute("UPDATE application.research_runs SET expires_at=clock_timestamp()-interval '1 second'")
  self.assertIsNone(await self.repo.get(self.owner,run['run_id']))

 async def test_crash_on_last_attempt_reaches_terminal_failure(self):
  run=await self.create()
  for _ in range(3):
   self.assertIsNotNone(await self.repo.claim(run['run_id']))
   await self.pool.execute("UPDATE application.research_runs SET lease_until=clock_timestamp()-interval '1 second'")
  self.assertIsNone(await self.repo.claim(run['run_id']))
  failed=await self.repo.get(self.owner,run['run_id'])
  self.assertEqual(failed['status'],'failed');self.assertEqual(failed['error_type'],'WorkerInterrupted')
  self.assertGreater(failed['event_cursor'],0)
