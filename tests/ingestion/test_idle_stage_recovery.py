import importlib.util,sys,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock,patch
from nv_ingest_durable import service as original

class IdleStageRecoveryTests(unittest.IsolatedAsyncioTestCase):
 async def test_idle_consumer_reclaims_on_wall_clock_without_new_messages(self):
  path=Path('ingestion/qualified_stage_service.py')
  self.assertTrue(path.exists(),'Idle recovery must not depend on the count of new messages')
  spec=importlib.util.spec_from_file_location('nv_ingest_durable.recovery_under_test',path);module=importlib.util.module_from_spec(spec)
  with patch.dict(sys.modules,{'nv_ingest_durable.service_original':original}):spec.loader.exec_module(module)
  s=module.StageWorkerService(stream='pages',group='g',consumer='c',worker=SimpleNamespace(),reclaim_idle_ms=30000)
  with patch.object(original.StageWorkerService,'_claim_one',new=AsyncMock(return_value=None)) as claim:
   await s._claim_one(reclaim=False)
   self.assertTrue(claim.await_args.kwargs['reclaim'])
   await s._claim_one(reclaim=False)
   self.assertFalse(claim.await_args.kwargs['reclaim'])
   s._recovery_due=0
   await s._claim_one(reclaim=False)
   self.assertTrue(claim.await_args.kwargs['reclaim'])
