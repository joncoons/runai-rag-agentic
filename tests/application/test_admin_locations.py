"""Real PostgreSQL + actual HTTP routes for administrator location lifecycle."""
import json,os,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from tests.application.test_local_auth import LocalAuthTests

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'isolated PostgreSQL required')
class AdminLocationTests(LocalAuthTests):
 async def asyncSetUp(self):
  await super().asyncSetUp()
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
  self.root=Path(self.temp.name)/'mounted';self.root.mkdir();(self.root/'reports').mkdir()
  self.config=Path(self.temp.name)/'roots.json';self.config.write_text(json.dumps({'mounted':str(self.root)}))
  env=patch.dict(os.environ,{'LOCATION_ROOTS_FILE':str(self.config),'LOCATION_REGISTRY_DATABASE':'true'});env.start();self.addCleanup(env.stop)
  if await self.pool.fetchval("SELECT to_regclass('application.location_roots')"):
   await self.pool.execute('TRUNCATE application.location_roots,application.location_root_events')
  await self.configured([])
 async def admin(self):
  client=await self.client();self.assertEqual((await self.login(client)).status_code,200)
  state=(await client.get('/api/session')).json();return client,{'X-CSRF-Token':state['csrf_token']}
 async def register(self,client,headers,alias='reports',path=None):
  return await client.post('/api/admin/locations',json={'alias':alias,'path':path or str(self.root/'reports')},headers=headers)
 async def test_anonymous_cannot_inspect_register_or_test_private_paths(self):
  client=await self.client()
  self.assertEqual((await client.get('/api/admin/locations')).status_code,401)
  for uri,body in [('/api/admin/locations',{'alias':'leak','path':'/etc'}),('/api/admin/locations/test',{'path':'/etc'})]:
   r=await client.post(uri,json=body);self.assertEqual(r.status_code,401);self.assertNotIn('/etc',r.text)
 async def test_register_test_disable_reenable_and_public_alias_only(self):
  client,h=await self.admin();r=await self.register(client,h);self.assertEqual(r.status_code,201,r.text)
  root=r.json()['location'];self.assertEqual(root['version'],1)
  probe=await client.post('/api/admin/locations/test',json={'path':str(self.root/'reports')},headers=h)
  self.assertEqual(probe.status_code,200,probe.text);self.assertTrue(probe.json()['accessible'])
  listing=(await client.get('/api/location-roots')).json();self.assertEqual(listing,{'roots':[{'alias':'reports','uri':'location://reports/'}]})
  self.assertNotIn(str(self.root),json.dumps(listing))
  from application.location_registry import active_roots
  self.assertEqual(await active_roots(self.pool),{'reports':str(self.root/'reports')})
  r=await client.patch('/api/admin/locations/reports',json={'expected_version':1,'enabled':False},headers=h);self.assertEqual(r.status_code,200,r.text)
  self.assertEqual(await active_roots(self.pool),{})
  self.assertEqual((await client.get('/api/location-roots')).json(),{'roots':[]})
  r=await client.post('/api/collections/collection/location-sources',json={'name':'Disabled','uri':'location://reports/'})
  self.assertEqual(r.status_code,422)
  r=await client.patch('/api/admin/locations/reports',json={'expected_version':2,'enabled':True},headers=h);self.assertEqual(r.status_code,200,r.text)
  self.assertIn('reports',await active_roots(self.pool))
  self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.location_root_events WHERE alias=$1','reports'),3)
 async def test_csrf_path_boundaries_symlinks_and_immutable_alias(self):
  client,h=await self.admin()
  self.assertEqual((await self.register(client,{})).status_code,403)
  (self.root/'escape').symlink_to('/etc',target_is_directory=True)
  for p in ['/etc',str(self.root/'../mounted/reports'),str(self.root/'escape'),str(self.root)+'-outside',str(self.root/'missing')]:
   r=await self.register(client,h,path=p);self.assertEqual(r.status_code,422,(p,r.text))
  self.assertEqual((await self.register(client,h)).status_code,201)
  self.assertEqual((await self.register(client,h,path=str(self.root))).status_code,409)
  r=await client.patch('/api/admin/locations/reports',json={'expected_version':77,'enabled':False},headers=h);self.assertEqual(r.status_code,409)
  r=await client.patch('/api/admin/locations/reports',json={'expected_version':1,'enabled':False,'path':str(self.root)},headers=h);self.assertEqual(r.status_code,422)
 async def test_unavailable_unrelated_alias_does_not_break_selected_worker_capture(self):
  from types import SimpleNamespace
  from application.location_sources import process_location
  from application.location_acquisition import capture_location
  client,h=await self.admin()
  self.assertEqual((await self.register(client,h)).status_code,201)
  (self.root/'reports'/'notes.txt').write_text('A grounded text document.')
  (self.root/'gone').mkdir()
  self.assertEqual((await self.register(client,h,alias='gone',path=str(self.root/'gone'))).status_code,201)
  (self.root/'gone').rmdir()
  class ProbeFinished(Exception):pass
  async def capture(spec,roots,**kwargs):
   result=await capture_location(spec,roots,**kwargs)
   self.assertTrue(result.complete,result.errors)
   self.assertEqual(len(result.files),1)
   raise ProbeFinished()
  with patch('application.location_sources.capture_location',side_effect=capture),self.assertRaises(ProbeFinished):
   await process_location(SimpleNamespace(pool=self.pool),{'spec':{'location':{'uri':'location://reports/'}}})
 async def test_registered_roots_outside_removed_operator_mount_are_not_resolved(self):
  client,h=await self.admin();self.assertEqual((await self.register(client,h)).status_code,201)
  self.config.write_text('{}')
  from application.location_registry import active_roots
  self.assertEqual(await active_roots(self.pool),{})

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'isolated PostgreSQL required')
class AdminMigrationTests(unittest.IsolatedAsyncioTestCase):
 asyncSetUp=LocalAuthTests.asyncSetUp
 async def test_additive_admin_migration_does_not_lock_existing_data_tables(self):
  import asyncio
  from deployment.application.migrate_admin_locations import apply
  await self.pool.execute('DROP TABLE application.local_sessions,application.local_login_attempts,application.local_principals,application.location_root_events,application.location_roots')
  async with self.pool.acquire() as reader,reader.transaction():
   await reader.execute('LOCK TABLE application.source_revisions,collection_control.collections IN ACCESS SHARE MODE')
   async with self.pool.acquire() as migration:await asyncio.wait_for(apply(migration),timeout=2)
  self.assertTrue(await self.pool.fetchval("SELECT to_regclass('application.location_roots') IS NOT NULL"))
