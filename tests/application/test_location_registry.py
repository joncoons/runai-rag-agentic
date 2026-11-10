import asyncio,json,os,unittest
from pathlib import Path
from urllib.parse import urlsplit
@unittest.skipUnless(os.environ.get('LOCATION_SOURCE_TEST_DSN'),'requires isolated connector PG')
class LocationRegistryTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  import asyncpg
  dsn=os.environ['LOCATION_SOURCE_TEST_DSN'];self.assertEqual(urlsplit(dsn).port,55446);self.pool=await asyncpg.create_pool(dsn);self.addAsyncCleanup(self.pool.close);who=await self.pool.fetchrow('SELECT current_database() db,current_user usr');self.assertEqual((who['db'],who['usr']),('qualification','qualification'))
  await self.pool.execute("DROP SCHEMA IF EXISTS application CASCADE;DROP SCHEMA IF EXISTS collection_control CASCADE;CREATE SCHEMA application;CREATE SCHEMA collection_control;CREATE TABLE collection_control.collections(collection_id text PRIMARY KEY,deleted boolean DEFAULT false);CREATE TABLE application.collections(collection_id text PRIMARY KEY REFERENCES collection_control.collections);CREATE TABLE application.jobs(job_id uuid PRIMARY KEY,collection_id text REFERENCES application.collections,status text DEFAULT 'queued');CREATE TABLE application.source_revisions(source_revision_id text PRIMARY KEY);INSERT INTO collection_control.collections VALUES('c',false);INSERT INTO application.collections VALUES('c');")
  for p in ('deployment/application/004_source_rechecks.sql','deployment/application/007_location_sources.sql'):await self.pool.execute(Path(p).read_text())
 async def test_registered_source_claim_snapshots_location_and_empty_inventory_is_observed(self):
  from application.source_registry import SourceRegistry
  r=SourceRegistry(self.pool);s=await r.create_location('c',name='Approved directory',location={'uri':'location://approved/'},schedule={});self.assertEqual(s['kind'],'location');run=await r.request(s['source_id'],'c');claimed=await r.claim('worker');self.assertEqual(claimed['spec']['kind'],'location');self.assertEqual(claimed['spec']['location']['uri'],'location://approved/')
  await r.finish(claimed['run_id'],claimed['fence'],status='observed',counts={'missing':1});row=await self.pool.fetchrow('SELECT * FROM application.website_source_runs WHERE run_id=$1',run['run_id']);self.assertEqual(row['state'],'observed');self.assertIsNone(row['job_id'])
 async def test_website_caption_choice_is_saved_and_versioned(self):
  from application.source_registry import SourceRegistry
  r=SourceRegistry(self.pool);s=await r.create('c',name='Web',seed_urls=['https://example.com/'],policy={'allowed_hosts':['example.com']},schedule={},captions_enabled=True);self.assertTrue(s['captions_enabled']);s=await r.update(s['source_id'],'c',captions_enabled=False);self.assertEqual(s['version'],2);run=await r.request(s['source_id'],'c');claimed=await r.claim('worker');self.assertFalse(claimed['spec']['captions_enabled'])
 async def test_unregistered_protocol_and_credentials_are_rejected(self):
  from application.source_registry import SourceRegistry
  r=SourceRegistry(self.pool)
  for uri in ('file:///etc/passwd','s3://bucket/key','https://a:secret@example.com/data.pdf','location://approved/../secret'):
   with self.subTest(uri=uri),self.assertRaises(ValueError):await r.create_location('c',name='Unsafe',location={'uri':uri},schedule={})

 async def test_stale_application_pointer_cannot_authorize_unchanged(self):
  from application.location_sources import baseline_current
  from uuid import uuid4
  job=uuid4();op=uuid4()
  await self.pool.execute("ALTER TABLE collection_control.collections ADD current_operation uuid;CREATE TABLE application.sources(source_id text PRIMARY KEY,current_revision_id text);ALTER TABLE application.source_revisions ADD source_id text;ALTER TABLE application.source_revisions ADD document_id text;ALTER TABLE application.source_revisions ADD job_id uuid;ALTER TABLE application.source_revisions ADD collection_id text;INSERT INTO application.sources VALUES('source','old');")
  await self.pool.execute("INSERT INTO application.source_revisions(source_revision_id,source_id,document_id,job_id,collection_id) VALUES('old','source','old-doc',$1,'c')",job)
  await self.pool.execute("UPDATE collection_control.collections SET current_operation=$1 WHERE collection_id='c'",op)
  await self.pool.execute("CREATE TABLE collection_control.record_lineage(operation_id uuid,stage_key text);CREATE TABLE collection_control.stage_admissions(stage_key text,document_id text,source_revision_id text);CREATE TABLE collection_control.caption_record_lineage(operation_id uuid,stage_key text);CREATE TABLE collection_control.caption_admissions(stage_key text,document_id text,source_revision_id text);")
  await self.pool.execute("INSERT INTO collection_control.record_lineage VALUES($1,'replacement')",op)
  await self.pool.execute("INSERT INTO collection_control.stage_admissions VALUES('replacement','new-doc','new')")
  async with self.pool.acquire() as c:self.assertFalse(await baseline_current(c,{'collection_id':'c','baseline':{'job_id':job}}))
