"""Pending notifications cannot starve the fresh dependencies they need."""
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock
from application.worker import ApplicationWorker
from collection_control.repository import Conflict

class AdmissionFairnessTests(IsolatedAsyncioTestCase):
 def worker(self,reclaim,read):
  worker=object.__new__(ApplicationWorker)
  worker.transport=SimpleNamespace(ensure_group=AsyncMock(),reclaim=AsyncMock(side_effect=reclaim),read=AsyncMock(side_effect=read),acknowledge=AsyncMock())
  return worker
 async def test_retryable_notification_reads_its_fresh_dependency(self):
  pending=object();fresh=object();seen=[];ready=False
  worker=self.worker([('2-0',[pending]),('0-0',[pending])],[[fresh],[]])
  async def handle(message):
   nonlocal ready
   seen.append(message)
   if message is pending and not ready:raise Conflict('Missing candidate')
   if message is fresh:ready=True
  await worker.consume_once('index',handle);await worker.consume_once('index',handle)
  self.assertTrue(ready);self.assertEqual(seen,[pending,fresh,pending]);self.assertEqual(worker.transport.acknowledge.await_count,2)
 async def test_reclaim_cursor_advances_past_slow_oldest_pending(self):
  oldest=object();dependency=object();seen=[]
  async def reclaim(*args,**kwargs):return ('2-0',[oldest]) if kwargs.get('start_id','0-0')=='0-0' else ('0-0',[dependency])
  worker=self.worker(reclaim,[[],[]])
  async def handle(message):
   seen.append(message)
   if message is oldest:raise Conflict('Wait for pending dependency')
  await worker.consume_once('index',handle);await worker.consume_once('index',handle)
  self.assertEqual(seen,[oldest,dependency]);worker.transport.acknowledge.assert_awaited_once_with(dependency)
 async def test_reclaim_cursor_is_independent_per_stream(self):
  worker=self.worker([('11-0',[]),('22-0',[]),('0-0',[])],[[],[],[]])
  async def handle(message):self.fail('No messages expected')
  for stream in ('index','caption','index'):await worker.consume_once(stream,handle)
  self.assertEqual([c.kwargs.get('start_id','0-0') for c in worker.transport.reclaim.await_args_list],['0-0','0-0','11-0'])
