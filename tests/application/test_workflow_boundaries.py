"""HTTP upload and worker process boundaries without external resources."""
import asyncio,unittest
from unittest.mock import patch
from application.api import BodyLimitMiddleware

class WorkflowBoundaries(unittest.IsolatedAsyncioTestCase):
 async def test_analysis_upload_uses_document_limit_without_changing_json_limit(self):
  seen=[]
  async def endpoint(scope,receive,send):
   seen.append(scope['path']);await send({'type':'http.response.start','status':204,'headers':[]});await send({'type':'http.response.body','body':b''})
  for path,expected in [('/api/analysis/workspaces',204),('/api/research/runs',413)]:
   output=[]
   async def send(message):output.append(message)
   async def receive():return {'type':'http.request','body':b'', 'more_body':False}
   with patch('application.api.MAX_REQUEST_BYTES',4*1024*1024):
    await BodyLimitMiddleware(endpoint)({'type':'http','method':'POST','path':path,'headers':[(b'content-length',b'3145728')]},receive,send)
   self.assertEqual(output[0]['status'],expected,path)
  self.assertEqual(seen,['/api/analysis/workspaces'])
 async def test_worker_failure_reaches_process_and_stops_siblings(self):
  from application.private_worker import supervise
  stopped=asyncio.Event()
  async def failed():raise RuntimeError('qualification failure')
  async def sibling():
   try:await asyncio.Event().wait()
   finally:stopped.set()
  with self.assertRaisesRegex(RuntimeError,'qualification failure'):
   await supervise([failed(),sibling()],asyncio.Event())
  self.assertTrue(stopped.is_set())
