import json,os,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from tests.application import test_current_health as fixtures

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'isolated PostgreSQL required')
class LocalAuthTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  await fixtures.CurrentHealthTests.asyncSetUp(self)
  await self.pool.execute('TRUNCATE application.local_sessions,application.local_principals,application.local_login_attempts CASCADE')
 client=fixtures.CurrentHealthTests.client
 async def configured(self,grants=None):
  from application.local_auth import password_record
  temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
  path=Path(temp.name)/'admin.json';path.write_text(json.dumps({'username':'fixture-admin','password':password_record('fixture-password'),'collection_write':grants if grants is not None else ['collection']}))
  env=patch.dict(os.environ,{'LOCAL_ADMIN_CONFIG_FILE':str(path)});env.start();self.addCleanup(env.stop)
  return path
 async def login(self,client,password='fixture-password'):
  state=(await client.get('/api/session')).json()
  return await client.post('/api/session/login',json={'username':'fixture-admin','password':password},headers={'X-CSRF-Token':state['csrf_token']})
 async def test_anonymous_session_is_server_owned_and_cookie_is_http_only(self):
  client=await self.client();r=await client.get('/api/session')
  self.assertEqual(r.status_code,200);self.assertFalse(r.json()['authenticated'])
  self.assertIn('HttpOnly',r.headers['set-cookie']);self.assertIn('SameSite=strict',r.headers['set-cookie'])
  self.assertNotIn('owner_id',r.json())
  self.assertEqual((await client.get('/api/session/collection-destinations')).status_code,401)
 async def test_valid_login_rotates_cookie_preserves_owner_and_exposes_only_permitted_destinations(self):
  await self.configured();client=await self.client();await client.get('/api/session')
  old=client.cookies.get('rag_session');owner=await self.pool.fetchval('SELECT owner_id FROM application.local_sessions ORDER BY created_at DESC LIMIT 1')
  response=await self.login(client);self.assertEqual(response.status_code,200,response.text)
  self.assertNotEqual(old,client.cookies.get('rag_session'))
  self.assertEqual(await self.pool.fetchval('SELECT owner_id FROM application.local_sessions WHERE revoked_at IS NULL ORDER BY created_at DESC LIMIT 1'),owner)
  r=await client.get('/api/session/collection-destinations');self.assertEqual(r.status_code,200)
  self.assertEqual([x['id'] for x in r.json()['collections']],['collection'])
  self.assertNotIn('fixture-password',response.text)
 async def test_login_and_logout_require_csrf_and_same_origin(self):
  await self.configured();client=await self.client();state=(await client.get('/api/session')).json()
  self.assertEqual((await client.post('/api/session/login',json={'username':'fixture-admin','password':'fixture-password'})).status_code,403)
  self.assertEqual((await client.post('/api/session/login',json={'username':'fixture-admin','password':'fixture-password'},headers={'X-CSRF-Token':state['csrf_token'],'Origin':'https://elsewhere.invalid'})).status_code,403)
  self.assertEqual((await self.login(client)).status_code,200)
  state=(await client.get('/api/session')).json()
  self.assertEqual((await client.post('/api/session/logout',headers={'X-CSRF-Token':state['csrf_token']})).status_code,200)
  self.assertFalse((await client.get('/api/session')).json()['authenticated'])
  events=await self.pool.fetch("SELECT event_type,payload FROM application.events WHERE event_type IN ('admin_login','admin_logout')")
  self.assertEqual({r['event_type'] for r in events},{'admin_login','admin_logout'})
  self.assertNotIn('fixture-password',str(events))
 async def test_wrong_credentials_are_bounded_by_persistent_rate_limit(self):
  await self.configured();client=await self.client()
  for _ in range(5):self.assertEqual((await self.login(client,'wrong')).status_code,401)
  response=await self.login(client);self.assertEqual(response.status_code,429);self.assertIn('retry-after',response.headers)
 async def test_login_does_not_grant_destination_permission_without_a_grant(self):
  await self.configured([]);client=await self.client();self.assertEqual((await self.login(client)).status_code,200)
  response=await client.get('/api/session/collection-destinations')
  self.assertEqual(response.status_code,200);self.assertEqual(response.json()['collections'],[])
 async def test_expired_session_and_revocation_deny_write_grants(self):
  await self.configured();client=await self.client();await self.login(client)
  from application.local_auth import SessionStore
  store=SessionStore(self.pool);session=await store.lookup(client.cookies.get('rag_session'))
  grant=await store.authorize_write(session,'collection');self.assertEqual(grant['destination'],'collection')
  await self.pool.execute('UPDATE application.local_sessions SET revoked_at=clock_timestamp()')
  with self.assertRaises(Exception):await store.authorize_write(session,'collection')
 async def test_revoked_account_is_not_reenabled_by_session_refresh(self):
  await self.configured();client=await self.client();await self.login(client)
  await self.pool.execute('UPDATE application.local_principals SET enabled=false')
  self.assertFalse((await client.get('/api/session')).json()['authenticated'])
  self.assertEqual((await client.get('/api/session/collection-destinations')).status_code,401)
 async def test_expiry_denies_existing_write_authorization(self):
  await self.configured();client=await self.client();await self.login(client)
  from application.local_auth import SessionStore
  store=SessionStore(self.pool);session=await store.lookup(client.cookies.get('rag_session'))
  await self.pool.execute("UPDATE application.local_sessions SET expires_at=clock_timestamp()-interval '1 second'")
  from fastapi import HTTPException
  with self.assertRaises(HTTPException) as e:await store.authorize_write(session,'collection')
  self.assertEqual(e.exception.status_code,401)
