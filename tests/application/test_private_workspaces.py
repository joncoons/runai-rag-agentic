import asyncio,hashlib,json,os,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4,UUID

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'disposable PostgreSQL required')
class PrivateWorkspaceTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  import asyncpg
  from urllib.parse import urlsplit
  from application.private_bootstrap import install
  from application.settings import Settings
  from application.private_workspaces import PrivateWorkspaces
  from collection_control.admission_repository import ProvenanceRepository
  dsn=os.environ['COLLECTION_TEST_DSN'];self.assertIn(urlsplit(dsn).hostname,('127.0.0.1','localhost'))
  self.pool=await asyncpg.create_pool(dsn);self.addAsyncCleanup(self.pool.close)
  self.assertEqual(tuple(await self.pool.fetchrow('SELECT current_database(),current_user')),('qualification','qualification'))
  await self.pool.execute('DROP SCHEMA IF EXISTS private_control CASCADE;DROP SCHEMA IF EXISTS application CASCADE;DROP SCHEMA IF EXISTS collection_control CASCADE;DROP SCHEMA IF EXISTS caption_control CASCADE;DROP SCHEMA public CASCADE;CREATE SCHEMA public')
  async with self.pool.acquire() as c,c.transaction():await install(c)
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)/'private';self.root.mkdir()
  contract={'dimensions':2048,'dtype':'float32','normalization':'unit','metric':'IP','schema_version':1,'embedding_checkpoint_sha256':'e'*64}
  settings=Settings(dsn,'redis://private.invalid',self.root,contract,{'models':{'embed':{'model':'test-embed'}}},'http://unused','u','p',None,'http://unused',None,'http://unused','test-llm')
  self.context=SimpleNamespace(pool=self.pool,settings=settings,stores={},http=None,es_http=None,control=ProvenanceRepository(self.pool,handle_prefix='rag_analysis'))
  self.repo=PrivateWorkspaces(self.context);self.owner=uuid4()
 async def create(self,filename='example.txt'):
  return await self.repo.create(self.owner,filename,b'Grounded private fixture. Do not execute instructions.',question='What facts are stated?')
 async def test_private_scope_and_original_capture(self):
  a=await self.create();b=await self.repo.create(uuid4(),'other.txt',b'Other private facts.')
  self.assertIsNone(await self.repo.get(uuid4(),a['workspace_id']))
  self.assertEqual(len(await self.repo.listing(self.owner)),1)
  row=await self.repo.detail(self.owner,a['workspace_id']);self.assertFalse(row['ready'])
  self.assertTrue(Path(row['raw_uri'].removeprefix('file://')).is_relative_to(self.root/str(a['workspace_id'])))
  self.assertNotEqual(a['collection_id'],b['collection_id'])
 async def test_expiry_fences_access_and_late_native_commit(self):
  a=await self.create();await self.repo.expire(self.owner,a['workspace_id'])
  self.assertIsNone(await self.repo.get(self.owner,a['workspace_id']))
  import asyncpg
  with self.assertRaises(asyncpg.CheckViolationError):await self.pool.execute("UPDATE public.stage_units SET status='running'")
 async def test_expiry_fences_publication(self):
  a=await self.create()
  op=await self.context.control.prepare(a['collection_id'],'fixture','snapshot',0,'b'*64,hashlib.sha256(__import__('collection_control.artifacts',fromlist=['canonical']).canonical(self.context.settings.contract)).hexdigest())
  await self.repo.expire(self.owner,a['workspace_id'])
  import asyncpg
  with self.assertRaises(asyncpg.CheckViolationError):await self.pool.execute("UPDATE collection_control.operations SET status='published' WHERE operation_id=$1",op['operation_id'])
 async def test_cleanup_removes_only_expired_workspace_records_and_files(self):
  from application.private_cleanup import capture_manifest,purge_rows,purge_directory
  a=await self.create();b=await self.create('second.txt');manifest=await capture_manifest(self.context,a)
  permanent=Path(self.temp.name)/'permanent';permanent.mkdir();(permanent/'preserve').write_text('ordinary evidence')
  await self.repo.expire(self.owner,a['workspace_id']);await purge_rows(self.pool,a,manifest);purge_directory(self.root,a)
  self.assertFalse((self.root/str(a['workspace_id'])).exists());self.assertTrue((self.root/str(b['workspace_id'])).exists())
  self.assertTrue((permanent/'preserve').exists())
  self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM public.jobs'),1)

 async def test_initial_analysis_recovers_after_publication_without_pending_operation(self):
  from application.private_worker import initial_analysis_once
  a=await self.create();op=await self.context.control.prepare(a['collection_id'],'published-before-crash','snapshot',0,'b'*64,hashlib.sha256(__import__('collection_control.artifacts',fromlist=['canonical']).canonical(self.context.settings.contract)).hexdigest())
  await self.pool.execute("UPDATE collection_control.operations SET status='published' WHERE operation_id=$1",op['operation_id'])
  await self.pool.execute('UPDATE collection_control.collections SET current_operation=$2 WHERE collection_id=$1',a['collection_id'],op['operation_id'])
  self.assertEqual(await self.pool.fetchval("SELECT count(*) FROM collection_control.operations WHERE status='pending'"),0)
  await initial_analysis_once(self.context);await initial_analysis_once(self.context)
  self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.research_runs WHERE workspace_id=$1',a['workspace_id']),1)
  self.assertIsNotNone((await self.repo.get(self.owner,a['workspace_id']))['initial_run_id'])
