import os,json,unittest,hashlib
from pathlib import Path
from uuid import uuid4
from datetime import datetime,timedelta,timezone
from application.api import CAPTION_STATES,COLLECTIONS_SQL
from application.gui_health import visible_captions

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'isolated PostgreSQL required')
class CurrentHealthTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  from tests.collection_control.test_stage_bridge import StageBridgeTests
  self.h=StageBridgeTests();await self.h.asyncSetUp();self.addAsyncCleanup(self.h.asyncTearDown);self.pool=self.h.pool
  for p in sorted(Path('deployment/collection-control').glob('*.sql')):await self.pool.execute(p.read_text())
  await self.pool.execute(Path('deployment/ingestion/captioning/001_caption_jobs.sql').read_text())
  for p in sorted(Path('deployment/application').glob('*.sql')):await self.pool.execute(p.read_text())
  await self.pool.execute("INSERT INTO application.collections(collection_id,name) VALUES('collection','Current health')")
 async def source(self,name,*,source_id=None,status='queued',current=False,web_role=None,offset=0,job=None):
  existing_job=job is not None;job=job or uuid4();doc='doc-'+uuid4().hex;revision='revision-'+uuid4().hex;source=source_id or 'source-'+uuid4().hex
  uri='https://fixture.invalid/'+name
  if not existing_job:await self.pool.execute("INSERT INTO public.jobs(job_id,corpus_namespace,status,pipeline_version,metadata) VALUES($1,'collection','running','test','{\"ingestion_options\":{\"captions_enabled\":true}}')",job)
  if not existing_job:await self.pool.execute("INSERT INTO application.jobs(job_id,collection_id) VALUES($1,'collection')",job)
  await self.pool.execute("INSERT INTO public.documents(document_id,job_id,source_uri,source_digest,status) VALUES($1,$2,$3,$4,'processing')",doc,job,uri,'sha256:'+'a'*64)
  await self.pool.execute("INSERT INTO application.sources(source_id,collection_id,canonical_uri) VALUES($1,'collection',$2) ON CONFLICT DO NOTHING",source,uri)
  await self.pool.execute("INSERT INTO application.source_revisions(source_revision_id,source_id,collection_id,document_id,job_id,canonical_uri,filename,raw_sha256,raw_uri,observed_at,ingestion_run_id,page_count,status,web_role) VALUES($1,$2,'collection',$3,$4,$5,$6,$7,'file:///fixture',$8,'test',1,$9,$10)",revision,source,doc,job,uri,name,'a'*64,datetime.now(timezone.utc)+timedelta(seconds=offset),status,web_role)
  if current:await self.pool.execute('UPDATE application.sources SET current_revision_id=$2 WHERE source_id=$1',source,revision)
  return {'job':job,'doc':doc,'revision':revision,'source':source,'uri':uri}
 async def caption(self,r,*,terminal='RuntimeError',publication_error=None):
  key='sha256:'+uuid4().hex*2
  await self.pool.execute("INSERT INTO caption_control.assets(stage_key,parent_job_id,document_id,source_digest,source_uri,collection_id,manifest_uri,manifest_sha256,asset_sha256,terminal_reason) VALUES($1,$2,$3,$4,$5,'collection','file:///missing',$6,$6,$7)",key,r['job'],r['doc'],'sha256:'+'a'*64,r['uri'],'b'*64,terminal)
  await self.pool.execute("INSERT INTO public.stage_units(idempotency_key,job_id,document_id,stage,status,input_envelope) VALUES($1,$2,$3,'caption','pending','{}')",key,r['job'],r['doc'])
  if publication_error:await self.pool.execute("INSERT INTO application.caption_publications(caption_stage_key,status,last_error_type) VALUES($1,'failed',$2)",key,publication_error)
  return key
 async def reference(self,child,parent,caption=True):
  download=uuid4()
  await self.pool.execute("INSERT INTO application.web_asset_downloads(download_id,observation_id,collection_id,requested_uri,kind,policy,policy_sha256,child_source_revision_id) VALUES($1,'test','collection',$2,$3,'{}',$4,$5)",download,child['uri'],'image' if child.get('image',True) else 'binary','a'*64,child['revision'])
  await self.pool.execute("INSERT INTO application.web_asset_references(reference_id,download_id,parent_source_revision_id,parent_document_id,parent_job_id,parent_url,locator,alt,captions_enabled) VALUES($1,$2,$3,$4,$5,$6,'img[1]','',$7)",uuid4(),download,parent['revision'],parent['doc'],parent['job'],parent['uri'],caption)
 async def keys(self):return {r['stage_key'] for r in await self.pool.fetch(CAPTION_STATES)}
 async def test_genuine_current_failure_remains_but_superseded_source_disappears(self):
  old=await self.source('doc.pdf',status='published');oldkey=await self.caption(old)
  new=await self.source('doc.pdf',source_id=old['source'],status='published',current=True,offset=1);newkey=await self.caption(new)
  self.assertEqual(await self.keys(),{newkey})
 async def test_removed_published_source_and_deleted_collection_are_hidden(self):
  r=await self.source('removed.pdf',status='published');await self.caption(r);self.assertEqual(await self.keys(),set())
  await self.pool.execute('UPDATE application.sources SET current_revision_id=$1',r['revision']);self.assertEqual(len(await self.keys()),1)
  await self.pool.execute('UPDATE collection_control.collections SET deleted=true');self.assertEqual(await self.keys(),set())
 async def test_current_and_latest_failed_replacement_remain_relevant(self):
  old=await self.source('doc.pdf',status='published',current=True);a=await self.caption(old)
  new=await self.source('doc.pdf',source_id=old['source'],status='failed',offset=1);b=await self.caption(new)
  self.assertEqual(await self.keys(),{a,b})
 async def test_orphaned_image_hidden_but_shared_active_reference_keeps_it(self):
  parent=await self.source('page.html',status='published',current=True,web_role='page')
  child=await self.source('image.png',web_role='image');key=await self.caption(child)
  self.assertEqual(await self.keys(),set());await self.reference(child,parent);self.assertEqual(await self.keys(),{key})
  await self.pool.execute('UPDATE application.sources SET current_revision_id=NULL WHERE source_id=$1',parent['source']);self.assertEqual(await self.keys(),set())
 async def test_stale_publication_disposition_is_not_a_true_error(self):
  r=await self.source('doc.pdf',status='published',current=True);await self.caption(r,terminal=None,publication_error='StaleWebImage');self.assertEqual(await self.keys(),set())

 async def client(self):
  from types import SimpleNamespace
  import httpx
  from application.api import create_app
  ctx=SimpleNamespace(pool=self.pool,settings=SimpleNamespace(stages=self.h.stage_root),control=SimpleNamespace(handle_prefix='rag_agentic'))
  client=httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(context=ctx)),base_url='http://fixture')
  self.addAsyncCleanup(client.aclose);return client
 async def test_api_counts_and_job_lists_share_relevance_without_deleting_history(self):
  old=await self.source('doc.pdf',status='published');await self.caption(old)
  live=await self.source('doc.pdf',source_id=old['source'],status='published',current=True,offset=1);await self.caption(live)
  client=await self.client();data=(await client.get('/api/collections')).json()
  self.assertEqual(data['collections'][0]['caption_failed'],1)
  stale=(await client.get('/api/jobs/'+str(old['job']))).json();self.assertFalse(stale['relevant']);self.assertEqual(stale['captions']['images'],[])
  current=(await client.get('/api/jobs/'+str(live['job']))).json();self.assertTrue(current['relevant']);self.assertEqual(current['captions']['failed'],1)
  await self.pool.execute('UPDATE collection_control.collections SET deleted=true')
  self.assertEqual((await client.get('/api/collections')).json()['collections'],[])
  self.assertFalse((await client.get('/api/jobs/'+str(live['job']))).json()['relevant'])
  self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM caption_control.assets WHERE parent_job_id=ANY($1::uuid[])',[old['job'],live['job']]),2)
 async def test_api_excludes_current_undersized_failures_from_counts_and_details(self):
  from io import BytesIO
  from PIL import Image
  from nv_ingest_durable.deferred_caption import retain
  r=await self.source('doc.pdf',status='published',current=True);key=await self.caption(r)
  b=BytesIO();Image.new('RGB',(19,19)).save(b,'PNG');uri,sha=retain(self.h.stage_root,b.getvalue(),'.png')
  mu,ms=retain(self.h.stage_root,json.dumps({'asset_uri':uri,'asset_sha256':sha}).encode(),'.json')
  await self.pool.execute('UPDATE caption_control.assets SET manifest_uri=$2,manifest_sha256=$3,asset_sha256=$4 WHERE stage_key=$1',key,mu,ms.removeprefix('sha256:'),sha.removeprefix('sha256:'))
  client=await self.client();self.assertEqual((await client.get('/api/collections')).json()['collections'][0]['caption_failed'],0)
  data=(await client.get('/api/jobs/'+str(r['job']))).json();self.assertTrue(data['relevant']);self.assertEqual(data['captions']['images'],[]);self.assertEqual(data['captions']['failed'],0)

 async def test_mixed_job_does_not_keep_an_obsolete_failure_summary(self):
  old=await self.source('bad.pdf',status='failed')
  good=await self.source('good.pdf',status='published',current=True,job=old['job'])
  await self.pool.execute("UPDATE application.jobs SET status='failed',stage='extraction',errors='[{\"type\":\"OldSourceError\"}]'::jsonb WHERE job_id=$1",old['job'])
  client=await self.client()
  self.assertEqual((await client.get('/api/jobs/'+str(old['job']))).json()['status'],'failed')
  await self.source('bad.pdf',source_id=old['source'],status='published',current=True,offset=1)
  data=(await client.get('/api/jobs/'+str(old['job']))).json()
  self.assertTrue(data['relevant']);self.assertEqual(data['status'],'completed');self.assertEqual(data['errors'],[])
  self.assertEqual(await self.pool.fetchval('SELECT status FROM application.jobs WHERE job_id=$1',old['job']),'failed')

class CaptionPolicyVisibilityTests(unittest.IsolatedAsyncioTestCase):
 async def test_valid_small_image_skip_is_hidden_but_corrupt_proof_is_visible(self):
  from types import SimpleNamespace
  from tempfile import TemporaryDirectory
  from io import BytesIO
  from PIL import Image
  from nv_ingest_durable.deferred_caption import retain
  with TemporaryDirectory() as folder:
   root=Path(folder);settings=SimpleNamespace(stages=root)
   b=BytesIO();Image.new('RGB',(19,19)).save(b,'PNG');uri,sha=retain(root,b.getvalue(),'.png')
   manifest={'asset_uri':uri,'asset_sha256':sha};mu,ms=retain(root,json.dumps(manifest).encode(),'.json')
   row={'stage_key':'tiny','status':'failed','manifest_uri':mu,'manifest_sha256':ms.removeprefix('sha256:'),'asset_sha256':sha.removeprefix('sha256:')}
   self.assertEqual(await visible_captions(settings,[row]),[])
   bad={**row,'stage_key':'bad','manifest_sha256':'0'*64}
   self.assertEqual(await visible_captions(settings,[bad]),[bad])


class CorruptSmallCaptionTests(unittest.IsolatedAsyncioTestCase):
 async def test_truncated_jpeg_is_a_true_error_not_a_size_skip(self):
  from tempfile import TemporaryDirectory
  from types import SimpleNamespace
  from io import BytesIO
  from PIL import Image
  from nv_ingest_durable.deferred_caption import retain
  with TemporaryDirectory() as folder:
   root=Path(folder);b=BytesIO();Image.new('RGB',(19,19)).save(b,'JPEG')
   raw=b.getvalue()[:-20];uri,sha=retain(root,raw,'.jpg')
   mu,ms=retain(root,json.dumps({'asset_uri':uri,'asset_sha256':sha}).encode(),'.json')
   row={'stage_key':'corrupt-jpeg','status':'failed','manifest_uri':mu,'manifest_sha256':ms.removeprefix('sha256:'),'asset_sha256':sha.removeprefix('sha256:')}
   self.assertEqual(await visible_captions(SimpleNamespace(stages=root),[row]),[row])

class CaptionProgressProjectionTests(unittest.TestCase):
 def test_generation_advances_before_publication(self):
  from application.gui_health import image_origin_summary
  rows=[{'stage_key':'q','origin':'webpage','status':'queued','inference_status':'pending'},
        {'stage_key':'r','origin':'webpage','status':'processing','inference_status':'running'},
        {'stage_key':'g','origin':'webpage','status':'publishing','inference_status':'committed'},
        {'stage_key':'p','origin':'webpage','status':'completed','inference_status':'committed'}]
  result=image_origin_summary(rows)['webpage']
  self.assertEqual(result['generated'],2);self.assertEqual(result['completed'],1)
  self.assertEqual(result['generating'],1);self.assertEqual(result['queued'],1)
  self.assertEqual(result['publishing'],1);self.assertEqual(result['pending'],3)

class SourceProgressLifetimeTests(unittest.IsolatedAsyncioTestCase):
 async def test_latest_published_run_keeps_async_progress_and_older_history_is_unchanged(self):
  from unittest.mock import AsyncMock,patch
  from application.source_registry import SourceRegistry
  pool=AsyncMock();pool.fetchval.return_value=True
  source=uuid4();latest=uuid4();old=uuid4()
  pool.fetch.return_value=[{'run_id':latest,'state':'published','counts':{'new':2}}, {'run_id':old,'state':'published','counts':{'new':1}}]
  with patch('application.gui_health.live_run_progress',AsyncMock(return_value={'pages_captured':2,'captions_generated':1})) as progress:
   result=await SourceRegistry(pool).history(source,'collection')
  self.assertEqual(result[0]['counts'],{'new':2,'pages_captured':2,'captions_generated':1})
  self.assertEqual(result[1]['counts'],{'new':1});progress.assert_awaited_once_with(pool,latest)
 async def test_nonwebsite_or_nonstreamed_run_preserves_original_counters(self):
  from unittest.mock import AsyncMock
  from application.gui_health import live_run_progress
  pool=AsyncMock();pool.fetchrow.return_value=None
  self.assertEqual(await live_run_progress(pool,uuid4()),{})
