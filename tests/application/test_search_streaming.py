import asyncio,json,unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock,patch
from tests.application import test_query
from collection_control.repository import Conflict,Fenced

class SearchStreamingTests(unittest.IsolatedAsyncioTestCase):
 def setUp(self):
  self.fixture=test_query.QueryTests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups);self.query=self.fixture.query
  self.query._stream_authorized=AsyncMock()
 async def events(self):return [e async for e in self.query.stream_search('Question',['collection'])]
 async def test_first_answer_delta_is_released_before_model_completion(self):
  released=asyncio.Event()
  async def chunks(*a,**k):
   yield 'First answer '
   await released.wait()
   yield '[S1].'
  self.query._chat_stream=chunks
  stream=self.query.stream_search('Question',['collection'])
  while True:
   e=await asyncio.wait_for(anext(stream),timeout=1)
   if e['event']=='delta':break
  self.assertEqual(e['data']['text'],'First answer ')
  released.set();remaining=[e async for e in stream]
  self.assertEqual(remaining[-1]['event'],'complete')
  self.assertEqual(remaining[-1]['data']['answer'],'First answer [S1].')
  self.query._stream_authorized.assert_not_awaited()
 async def test_invalid_citations_split_across_chunks_never_escape(self):
  async def chunks(*a,**k):
   for text in ('Answer [','S99','] and [S','1','].'):yield text
  self.query._chat_stream=chunks;events=await self.events()
  text=''.join(e['data']['text'] for e in events if e['event']=='delta')
  self.assertEqual(text,'Answer  and [S1].');self.assertNotIn('S99',text)
  self.assertTrue(events[-1]['data']['warnings'])
 async def test_ingestion_update_does_not_interrupt_the_answer(self):
  async def chunks(*a,**k):yield 'First';yield ' and second'
  self.query._chat_stream=chunks
  self.query.repository.resolve.side_effect=AssertionError('No query-time publication resolution')
  self.query._stream_authorized.side_effect=AssertionError('No stream-batch checks')
  events=await self.events()
  self.assertEqual(events[-1]['event'],'complete')
  self.assertEqual(events[-1]['data']['answer'],'First and second')
  self.query._stream_authorized.assert_not_awaited()
 async def test_closing_search_stream_closes_upstream_generator(self):
  closed=asyncio.Event()
  async def chunks(*a,**k):
   try:yield 'First';await asyncio.Event().wait()
   finally:closed.set()
  self.query._chat_stream=chunks;stream=self.query.stream_search('Question',['collection'])
  while (await anext(stream))['event']!='delta':pass
  await stream.aclose();self.assertTrue(closed.is_set())

class NativeStreamTests(unittest.IsolatedAsyncioTestCase):
 async def run_model(self,events):
  from application.query import RAGQuery
  from langchain_core.messages import HumanMessage
  from aiohttp import web
  payloads=[]
  async def chat(request):
   payloads.append(await request.json());response=web.StreamResponse(headers={'Content-Type':'text/event-stream'});await response.prepare(request)
   raw=''.join('data: '+(e if isinstance(e,str) else json.dumps(e,ensure_ascii=False))+'\r\n\r\n' for e in events).encode()
   for i in range(0,len(raw),3):await response.write(raw[i:i+3])
   await response.write_eof();return response
  app=web.Application();app.router.add_post('/v1/chat/completions',chat);runner=web.AppRunner(app);await runner.setup();site=web.TCPSite(runner,'127.0.0.1',0);await site.start()
  import aiohttp
  try:
   async with aiohttp.ClientSession() as session:
    q=RAGQuery(SimpleNamespace(es_ca=None,llm_endpoint='http://127.0.0.1:'+str(site._server.sockets[0].getsockname()[1]),llm_model='fixture'),None,session)
    values=[v async for v in q._chat_stream([HumanMessage(content='Complete question')])]
   return values,payloads
  finally:await runner.cleanup()
 async def test_native_sse_fragments_utf8_and_excludes_reasoning(self):
  events=[{'choices':[{'delta':{'reasoning_content':'Hidden reasoning'}}]},{'choices':[{'delta':{'content':'Café '}}]},{'choices':[{'delta':{'content':'answer'},'finish_reason':'stop'}]},'[DONE]']
  values,payloads=await self.run_model(events)
  self.assertEqual(values,['Café ','answer']);self.assertTrue(payloads[0]['stream'])
  self.assertEqual(payloads[0]['chat_template_kwargs'],{'enable_thinking':False})
 async def test_truncation_missing_done_and_no_final_content_are_rejected(self):
  for events in ([{'choices':[{'delta':{'content':'partial'},'finish_reason':'length'}]},'[DONE]'],[{'choices':[{'delta':{'content':'partial'}}]}],[{'choices':[{'delta':{},'finish_reason':'stop'}]},'[DONE]']):
   with self.subTest(events=events),self.assertRaises(Conflict):await self.run_model(events)

class StreamAPITests(unittest.IsolatedAsyncioTestCase):
 async def test_disconnect_cancels_pending_event_before_closing_generator(self):
  from application.api import create_app,SearchRequest
  from starlette.requests import Request
  closed=asyncio.Event()
  async def stream(*a,**kw):
   try:
    yield {'event':'status','data':{'stage':'retrieval'}}
    await asyncio.Event().wait()
   finally:closed.set()
  app=create_app(context=SimpleNamespace(),query_service=SimpleNamespace(stream_search=stream))
  endpoint=next(r.endpoint for r in app.routes if getattr(r,'path',None)=='/api/search/stream')
  response=await endpoint(SearchRequest(query='Question'),Request({'type':'http','app':app}))
  iterator=response.body_iterator;await anext(iterator)
  pending=asyncio.create_task(anext(iterator));await asyncio.sleep(.01);pending.cancel()
  with self.assertRaises(asyncio.CancelledError):await pending
  self.assertTrue(closed.is_set())

class MCPFenceErrorTests(unittest.IsolatedAsyncioTestCase):
 async def test_mcp_client_checks_scope_without_repeating_publication_validation(self):
  from contextlib import asynccontextmanager
  from application.query import RAGQuery
  @asynccontextmanager
  async def transport(*a,**k):
   try:yield (None,None)
   except Exception as error:raise ExceptionGroup('transport exit',[error])
  class Client:
   def __init__(self,*a):pass
   async def __aenter__(self):return self
   async def __aexit__(self,*a):return False
   async def initialize(self):pass
   async def call_tool(self,*a,**k):return SimpleNamespace(is_error=False,structured_content={'sources':[],'publications':[{'collection_id':'collection'}]})
  q=SimpleNamespace(mcp_url='http://fixture',_revalidate=AsyncMock(side_effect=Fenced('Publication changed')))
  with patch('mcp.client.streamable_http.streamable_http_client',transport),patch('mcp.ClientSession',Client):
   result=await RAGQuery._remote_evidence(q,'Question',['collection'],'elasticsearch')
  self.assertEqual(result['sources'],[])
  q._revalidate.assert_not_awaited()
