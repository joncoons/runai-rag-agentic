import asyncio,os,unittest
from unittest.mock import AsyncMock
from application.query_catalog import QueryCatalog
from tests.application import test_current_health as fixtures

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'isolated PostgreSQL required')
class QueryCatalogDatabaseTests(unittest.IsolatedAsyncioTestCase):
 asyncSetUp=fixtures.CurrentHealthTests.asyncSetUp
 async def publish(self):
  envelope,lease,body,_=await self.h.fixture()
  admission=await self.h.commit(envelope,lease,body)
  await self.h.publish(admission['operation_id'])
 async def test_cache_refresh_does_not_wait_for_ingestion_row_lock(self):
  await self.publish();catalog=QueryCatalog(self.pool,snapshots=self.h.snapshots)
  async with self.pool.acquire() as writer,writer.transaction():
   await writer.execute("UPDATE collection_control.collections SET target_epoch=target_epoch+1 WHERE collection_id='collection'")
   await asyncio.wait_for(catalog.refresh(),timeout=2)
  data=catalog.selected(['collection'])[0]
  self.assertEqual(len(data['versions']),1)
  self.assertEqual(data['legal_versions'],frozenset())
 async def test_background_refresh_removes_deleted_collection_for_future_searches(self):
  from collection_control.repository import Conflict
  await self.publish();catalog=await QueryCatalog(self.pool,snapshots=self.h.snapshots,refresh_seconds=1).start();self.addAsyncCleanup(catalog.close)
  old=catalog.selected(['collection'])[0]
  await self.pool.execute("UPDATE collection_control.collections SET deleted=true WHERE collection_id='collection'")
  await catalog.refresh()
  with self.assertRaises(Conflict):catalog.selected(['collection'])
  self.assertTrue(old['versions'],'Existing read metadata remains an ordinary immutable value')

class QueryCatalogFailureTests(unittest.IsolatedAsyncioTestCase):
 async def test_failed_refresh_retains_last_good_cached_metadata(self):
  pool=type('Pool',(),{'fetch':AsyncMock(side_effect=RuntimeError('temporary unavailable'))})()
  catalog=QueryCatalog(pool);catalog.entries={'collection':{'handles':{}}};catalog.ready=True
  with self.assertRaises(RuntimeError):await catalog.refresh()
  self.assertEqual(catalog.selected(['collection']),[{'handles':{}}])
