import asyncio,unittest
from uuid import uuid4
from unittest.mock import AsyncMock,patch
from application.research_worker import serve

class ResearchDeliveryTests(unittest.IsolatedAsyncioTestCase):
 async def test_private_executor_receives_claim_and_notification_is_acknowledged(self):
  stop=asyncio.Event();run=uuid4();lease={'run_id':run}
  redis=type('Redis',(),{})();redis.xgroup_create=AsyncMock();redis.xack=AsyncMock();redis.xreadgroup=AsyncMock(return_value=[(b'private',[(b'1-0',{b'run_id':str(run).encode()})])])
  repo=type('Repo',(),{})();repo.claim=AsyncMock(return_value=lease)
  async def execute(value):self.assertEqual(value,lease);stop.set()
  with patch('application.research_worker.ResearchJobs',return_value=repo),patch('application.research_worker.notify',new=AsyncMock()):
   await serve(None,redis,None,stop,stream='private',execute_lease=execute)
  redis.xack.assert_awaited_once_with('private','research-v1',b'1-0')
