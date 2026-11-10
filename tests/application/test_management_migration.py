import asyncio,os,unittest
from tests.application import test_current_health as fixtures

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'isolated PostgreSQL required')
class ManagementMigrationTests(unittest.IsolatedAsyncioTestCase):
 asyncSetUp=fixtures.CurrentHealthTests.asyncSetUp
 async def test_new_history_table_can_be_added_without_locking_existing_data_tables(self):
  from deployment.application.migrate_source_configuration_history import apply
  await self.pool.execute('DROP TABLE application.source_configuration_history')
  async with self.pool.acquire() as reader,reader.transaction():
   await reader.execute('LOCK TABLE application.source_revisions,collection_control.collections IN ACCESS SHARE MODE')
   async with self.pool.acquire() as migration:
    await asyncio.wait_for(apply(migration),timeout=2)
  self.assertTrue(await self.pool.fetchval("SELECT to_regclass('application.source_configuration_history') IS NOT NULL"))
