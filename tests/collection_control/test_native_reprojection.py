import os,unittest
from pathlib import Path
from tests.collection_control.test_stage_bridge import StageBridgeTests
from collection_control.admission_repository import ProvenanceRepository
from collection_control.artifacts import canonical,content_digest
from collection_control.repository import Conflict

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'requires isolated qualification PostgreSQL')
class NativeReprojectionTests(unittest.IsolatedAsyncioTestCase):
 asyncSetUp=StageBridgeTests.asyncSetUp
 asyncTearDown=StageBridgeTests.asyncTearDown
 fixture=StageBridgeTests.fixture
 finalized=StageBridgeTests.finalized
 commit=StageBridgeTests.commit
 publish=StageBridgeTests.publish
 async def test_reprojection_preserves_complete_admission_and_does_not_publish_without_receipts(self):
  for path in sorted(Path('deployment/collection-control').glob('*.sql')):await self.pool.execute(path.read_text())
  env,lease,body,raw=await self.fixture();admitted=await self.commit(env,lease,body);await self.publish(admitted['operation_id'])
  before=await self.repo.resolve('collection');repo=ProvenanceRepository(self.pool,handle_prefix='rag_agentic',incremental=True)
  with self.assertRaises(Conflict):await repo.reproject('collection','stale',expected_operation_id='00000000-0000-0000-0000-000000000000')
  op=await repo.reproject('collection','native-fields-migration',expected_operation_id=before['operation_id'])
  self.assertEqual((await repo.reproject('collection','native-fields-migration',expected_operation_id=before['operation_id']))['operation_id'],op['operation_id'])
  self.assertEqual(await self.repo.resolve('collection'),before)
  parent=await self.pool.fetchrow('SELECT * FROM collection_control.generation_admissions WHERE operation_id=$1',before['operation_id'])
  copied=await self.pool.fetchrow('SELECT * FROM collection_control.generation_admissions WHERE operation_id=$1',op['operation_id'])
  self.assertEqual((copied['artifact_sha256'],copied['content_sha256'],copied['row_count']),(parent['artifact_sha256'],parent['content_sha256'],parent['row_count']))
  self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.record_lineage WHERE operation_id=$1',op['operation_id']),parent['row_count'])
  attempt=await repo.claim(op['operation_id'],ttl_seconds=60)
  with self.assertRaises(Conflict):await repo.publish(attempt)
  await repo.receipt(attempt,'elasticsearch',parent['content_sha256'],parent['row_count']);await repo.publish(attempt)
  self.assertEqual((await repo.reproject('collection','native-fields-migration'))['operation_id'],op['operation_id'])
  self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM public.documents'),1)
