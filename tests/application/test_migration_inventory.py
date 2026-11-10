from pathlib import Path
import unittest
from unittest.mock import AsyncMock
from application.migrate import migrations,apply

class MigrationInventoryTests(unittest.IsolatedAsyncioTestCase):
 def test_durable_schema_precedes_collection_schema(self):
  root=Path(__file__).resolve().parents[2]
  items=list(migrations(root))
  self.assertTrue(items[0][0].startswith('runtime/'))
  self.assertTrue(any(n.startswith('deployment/application/') for n,_,_ in items))
  self.assertTrue(all(not text.startswith('BEGIN;') and not text.endswith('COMMIT;') for _,_,text in items))
 async def test_changed_applied_migration_is_rejected(self):
  class Transaction:
   async def __aenter__(self):return self
   async def __aexit__(self,*a):return False
  connection=AsyncMock();connection.transaction=lambda:Transaction();connection.fetchval.return_value='changed'
  with self.assertRaisesRegex(ValueError,'Previously applied'):
   await apply(connection,Path(__file__).resolve().parents[2])
