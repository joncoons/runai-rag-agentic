import os,unittest
from uuid import uuid4
from tests.application import test_current_health as fixtures

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'isolated PostgreSQL required')
class CollectionManagementTests(unittest.IsolatedAsyncioTestCase):
 asyncSetUp=fixtures.CurrentHealthTests.asyncSetUp
 source=fixtures.CurrentHealthTests.source
 caption=fixtures.CurrentHealthTests.caption
 reference=fixtures.CurrentHealthTests.reference
 client=fixtures.CurrentHealthTests.client
 async def test_published_collection_remains_searchable_while_assets_wait(self):
  parent=await self.source('page.html',status='published',current=True,web_role='page')
  image=await self.source('image.png',web_role='image');await self.reference(image,parent);await self.caption(image,terminal=None)
  await self.pool.execute('UPDATE collection_control.collections SET current_operation=$1',uuid4())
  result=(await (await self.client()).get('/api/collections')).json()['collections'][0]
  self.assertEqual(result['status'],'ready')
  self.assertEqual(result['elasticsearch_status'],'ready')
  self.assertGreater(result['processing']['sources_pending'],0)
  self.assertGreater(result['processing']['captions_pending'],0)
 async def test_image_origins_count_unique_hashes_separately_from_associations(self):
  parent=await self.source('page.html',status='published',current=True,web_role='page')
  for name in ['a.png','b.png']:
   image=await self.source(name,web_role='image');await self.reference(image,parent)
   await self.pool.execute("UPDATE application.source_revisions SET source_kind='web_image' WHERE document_id=$1",image['doc'])
   await self.caption(image,terminal=None)
  pdf=await self.source('guide.pdf',status='published',current=True);key=await self.caption(pdf,terminal=None)
  await self.pool.execute('UPDATE caption_control.assets SET asset_sha256=$2 WHERE stage_key=$1',key,'c'*64)
  result=(await (await self.client()).get('/api/collections')).json()['collections'][0]
  self.assertEqual(result['image_origins']['webpage']['unique_images'],1)
  self.assertEqual(result['image_origins']['webpage']['associations'],2)
  self.assertEqual(result['image_origins']['pdf']['unique_images'],1)
  self.assertEqual(result['image_origins']['pdf']['associations'],1)
 async def test_generic_source_inventory_includes_persisted_uploaded_files(self):
  r=await self.source('report.pdf',status='published',current=True)
  await self.pool.execute("UPDATE application.source_revisions SET canonical_uri='upload://collection/report.pdf' WHERE document_id=$1",r['doc'])
  result=await (await self.client()).get('/api/collections/collection/sources')
  self.assertEqual(result.status_code,200)
  self.assertEqual(result.json()['uploads'][0]['filename'],'report.pdf')
  self.assertEqual(result.json()['uploads'][0]['capabilities'],['replace'])
 async def test_edit_website_scope_schedule_and_name_is_versioned_and_audited(self):
  from application.source_registry import SourceRegistry
  source=await SourceRegistry(self.pool).create('collection',name='Original',seed_urls=['https://docs.example/a'],policy={'allowed_hosts':['docs.example']},schedule={})
  client=await self.client();base='/api/collections/collection/sources/'+str(source['source_id'])
  body={'expected_version':source['version'],'name':'Updated docs','seed_urls':['https://docs.example/b'],
        'policy':{'allowed_hosts':['docs.example'],'max_depth':None,'include_images':True},
        'schedule':{'enabled':True,'timezone':'UTC','start':'02:00','end':'05:00'}}
  response=await client.patch(base,json=body)
  self.assertEqual(response.status_code,200,response.text)
  result=response.json()['source'];self.assertEqual(result['name'],'Updated docs');self.assertEqual(result['version'],2)
  self.assertIsNone(result['policy']['max_depth']);self.assertEqual(result['seed_urls'],['https://docs.example/b'])
  self.assertTrue(result['schedule']['enabled'])
  self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.source_configuration_history WHERE source_id=$1',source['source_id']),1)
  self.assertEqual((await client.patch(base,json=body)).status_code,409)
 async def test_edit_locations_retains_registered_root_boundary(self):
  from unittest.mock import patch
  from application.source_registry import SourceRegistry
  source=await SourceRegistry(self.pool).create_location('collection',name='Directory',location={'uri':'location://approved/','recursive':True,'max_depth':4,'max_files':50},schedule={})
  client=await self.client();base='/api/collections/collection/sources/'+str(source['source_id'])
  with patch('application.location_api._roots',return_value={'approved':'/source-roots/approved'}):
   good=await client.patch(base,json={'expected_version':1,'name':'Selected directory','location':{'uri':'location://approved/subdir','recursive':False,'max_depth':1,'max_files':10}})
   self.assertEqual(good.status_code,200,good.text)
   bad=await client.patch(base,json={'expected_version':2,'location':{'uri':'location://unregistered/private'}})
   self.assertEqual(bad.status_code,422)
  self.assertEqual((await client.get('/api/collections/collection/sources')).json()['sources'][0]['location']['uri'],'location://approved/subdir')
 async def test_edit_direct_document_url_is_visible_without_website_seeds(self):
  from application.source_registry import SourceRegistry
  source=await SourceRegistry(self.pool).create_location('collection',name='Direct URL',location={'uri':'https://docs.example/guide.pdf','allowed_hosts':['docs.example'],'max_files':1},schedule={})
  client=await self.client();base='/api/collections/collection/sources/'+str(source['source_id'])
  response=await client.patch(base,json={'expected_version':1,'location':{'uri':'https://docs.example/new.pdf','allowed_hosts':['docs.example']}})
  self.assertEqual(response.status_code,200,response.text)
  self.assertEqual(response.json()['source']['location']['uri'],'https://docs.example/new.pdf')
 async def test_edit_never_accepts_private_scope_or_seed_outside_hosts(self):
  from application.source_registry import SourceRegistry
  source=await SourceRegistry(self.pool).create('collection',name='Original',seed_urls=['https://docs.example/'],policy={'allowed_hosts':['docs.example']},schedule={})
  client=await self.client();base='/api/collections/collection/sources/'+str(source['source_id'])
  for body in [{'expected_version':1,'policy':{'private_hosts':['private.internal']}},{'expected_version':1,'seed_urls':['https://elsewhere.example/']},{'expected_version':1,'location':{'uri':'file:///etc/passwd'}}]:
   self.assertEqual((await client.patch(base,json=body)).status_code,422)
  self.assertEqual((await SourceRegistry(self.pool).list('collection'))[0]['version'],1)
 async def test_pending_upload_replacement_is_one_logical_source_row(self):
  old=await self.source('report.pdf',status='published',current=True)
  new=await self.source('report.pdf',source_id=old['source'],status='queued',offset=1)
  await self.pool.execute("UPDATE application.source_revisions SET canonical_uri='upload://collection/report.pdf' WHERE source_id=$1",old['source'])
  result=(await (await self.client()).get('/api/collections/collection/sources')).json()
  self.assertEqual(len(result['uploads']),1)
  self.assertEqual(result['uploads'][0]['status'],'queued')
  self.assertTrue(result['uploads'][0]['published_available'])
 async def test_one_time_captures_remain_visible_without_a_saved_connector(self):
  r=await self.source('legacy.html',status='published',current=True,web_role='page')
  await self.pool.execute("UPDATE application.source_revisions SET source_kind='html' WHERE document_id=$1",r['doc'])
  result=(await (await self.client()).get('/api/collections/collection/sources')).json()
  self.assertEqual(result['one_time_sources'][0]['uri'],r['uri'])
  self.assertEqual(result['one_time_sources'][0]['capabilities'],['register_source'])
 async def test_extracted_assets_waiting_for_admission_are_reported_separately(self):
  parent=await self.source('page.html',status='published',current=True,web_role='page')
  child=await self.source('chart.png',web_role='image');await self.reference(child,parent)
  await self.pool.execute("INSERT INTO public.stage_units(idempotency_key,job_id,document_id,stage,status,input_envelope) VALUES($1,$2,$3,'extraction','committed','{}')",'sha256:'+'d'*64,child['job'],child['doc'])
  result=(await (await self.client()).get('/api/collections')).json()['collections'][0]
  self.assertEqual(result['processing']['admission_waiting'],1)
 async def test_url_edits_reject_saved_credentials_and_fragments(self):
  from application.source_registry import SourceRegistry
  registry=SourceRegistry(self.pool)
  website=await registry.create('collection',name='Website',seed_urls=['https://docs.example/'],policy={'allowed_hosts':['docs.example']},schedule={})
  direct=await registry.create_location('collection',name='Document',location={'uri':'https://docs.example/a.pdf','allowed_hosts':['docs.example']},schedule={})
  client=await self.client()
  for uri in ['https://docs.example/a.pdf?api_key=fixture-secret','https://docs.example/a.pdf#fragment']:
   for source,change in [(website,{'seed_urls':[uri]}),(direct,{'location':{'uri':uri}})]:
    response=await client.patch('/api/collections/collection/sources/'+str(source['source_id']),json={'expected_version':1,**change})
    self.assertEqual(response.status_code,422)
  self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.source_configuration_history'),0)
 async def test_unrelated_same_host_connector_cannot_hide_one_time_input(self):
  from application.source_registry import SourceRegistry
  r=await self.source('old.html',status='published',current=True,web_role='page')
  await SourceRegistry(self.pool).create('collection',name='Unrelated',seed_urls=['https://fixture.invalid/new'],policy={'allowed_hosts':['fixture.invalid'],'max_depth':0,'page_prefixes':[]},schedule={})
  result=(await (await self.client()).get('/api/collections/collection/sources')).json()
  self.assertIn(r['uri'],[r['uri'] for r in result['one_time_sources']])
 async def test_nonrecursive_directory_cannot_hide_unassociated_descendant(self):
  from application.source_registry import SourceRegistry
  r=await self.source('old.pdf',status='published',current=True)
  uri='location://approved/subdir/old.pdf'
  await self.pool.execute('UPDATE application.source_revisions SET canonical_uri=$2 WHERE document_id=$1',r['doc'],uri)
  await SourceRegistry(self.pool).create_location('collection',name='Root only',location={'uri':'location://approved/','recursive':False,'max_depth':0,'max_files':10},schedule={})
  result=(await (await self.client()).get('/api/collections/collection/sources')).json()
  self.assertIn(uri,[r['uri'] for r in result['one_time_sources']])
 async def test_unchanged_historical_extension_selection_does_not_block_a_full_editor_save(self):
  from application.source_registry import SourceRegistry
  source=await SourceRegistry(self.pool).create('collection',name='Legacy source',seed_urls=['https://docs.example/'],policy={'allowed_hosts':['docs.example']},schedule={})
  response=await (await self.client()).patch('/api/collections/collection/sources/'+str(source['source_id']),json={'expected_version':1,'name':'Renamed source','policy':{'included_extensions':source['policy']['included_extensions'],'ignored_extensions':source['policy']['ignored_extensions']}})
  self.assertEqual(response.status_code,200,response.text)
  self.assertEqual(response.json()['source']['policy']['included_extensions'],source['policy']['included_extensions'])
