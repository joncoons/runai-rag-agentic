import copy,importlib.util,json,tempfile,tomllib,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from collection_control.repository import Conflict
ROOT=Path(__file__).resolve().parents[2]

def render():
 spec=importlib.util.spec_from_file_location('render_search',ROOT/'deployment/routing/render.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
 return m.render(efficient_endpoint='http://small:8000/v1',capable_endpoint='http://medium:8000/v1',efficient_model='model/small',capable_model='model/medium',router_endpoint='http://router:4000/v1',hosted_endpoint='https://provider.test/v1',hosted_model='model/large',hosted_context_window=131072)

class SearchPolicyTests(unittest.TestCase):
 def test_separate_consent_routes_and_three_targets(self):
  files=render();routes=tomllib.loads(files['routes.toml']);cfg=json.loads(files['search-client.json'])
  self.assertEqual(len(cfg['targets']),3)
  local=routes['routes']['search_local'];remote=routes['routes']['search_hosted']
  self.assertNotIn('large',json.loads(local['response_schema'])['properties']['target']['enum'])
  self.assertIn('large',json.loads(remote['response_schema'])['properties']['target']['enum'])
  self.assertEqual(remote['classify_trigger'],'new_session');self.assertEqual(remote['models']['judge'],['search_capable'])
  self.assertEqual(routes['targets']['search_capable']['extra_body']['chat_template_kwargs'],{'enable_thinking':False})
  self.assertEqual(routes['targets']['search_capable']['extra_body']['temperature'],0)
  self.assertEqual(routes['llm_clients']['large']['api_key_env'],'NVIDIA_REMOTE_API_KEY')
 def test_route_selection_obeys_consent_and_full_context(self):
  from application.search_routing import SearchModels
  cfg=json.loads(render()['search-client.json']);q=SimpleNamespace(settings=SimpleNamespace(llm_endpoint='old',llm_model='old'),http=object())
  from langchain_core.messages import HumanMessage
  router=SearchModels(q,cfg)
  a=router.client([HumanMessage(content='question')],allow_remote=False)
  self.assertEqual(a.settings.llm_model,'search-local');self.assertEqual(q.settings.llm_model,'old')
  b=router.client([HumanMessage(content='question')],allow_remote=True)
  self.assertEqual(b.settings.llm_model,'search-hosted')
  c=router.client([HumanMessage(content='x'*140000)],allow_remote=True)
  self.assertEqual(c.settings.llm_model,'search-local');self.assertTrue(c.search_routing['remote_context_excluded'])
  with self.assertRaises(Conflict):router.client([],allow_remote='true')
 def test_config_is_validated_before_any_request(self):
  from application.search_routing import validate_config
  cfg=json.loads(render()['search-client.json']);validate_config(cfg)
  for change in ({'endpoint':'https://public.example/v1'},{'schema_version':True},{'targets':{}},{'hosted_context_window':True}):
   with self.subTest(change=change),self.assertRaises(Conflict):validate_config({**cfg,**change})

class SearchIntegrationTests(unittest.IsolatedAsyncioTestCase):
 async def test_stream_routes_after_evidence_and_keeps_model_fixed(self):
  from tests.application.test_query import QueryTests
  from unittest.mock import Mock
  f=QueryTests();f.setUp();self.addCleanup(f.doCleanups);q=f.query
  routed=copy.copy(q);routed.search_routing={'model':'model/small','tier':'small'}
  async def chunks(*args,**kwargs):yield 'First ';yield 'answer [S1].'
  routed._chat_stream=chunks
  with patch('application.search_routing.answer_client',return_value=routed) as factory:
   events=[e async for e in q.stream_search('Question',['collection'],allow_remote=True)]
  factory.assert_called_once();self.assertTrue(factory.call_args.kwargs['allow_remote'])
  self.assertIn('evidence',factory.call_args.args[1][-1].content)
  self.assertEqual(events[-1]['data']['routing']['tier'],'small')
  self.assertEqual(events[-1]['data']['answer'],'First answer [S1].')
 async def test_nonstream_uses_same_router_and_returns_telemetry(self):
  from tests.application.test_query import QueryTests
  from unittest.mock import AsyncMock
  f=QueryTests();f.setUp();self.addCleanup(f.doCleanups);q=f.query
  routed=copy.copy(q);routed.search_routing={'model':'model/large','tier':'large'};routed._chat=AsyncMock(return_value='Answer [S1]')
  with patch('application.search_routing.answer_client',return_value=routed) as factory:
   result=await q.search('Question',['collection'],allow_remote=True)
  self.assertEqual(result['routing']['model'],'model/large');factory.assert_called_once()
 async def test_consent_strict_and_not_permission_for_legacy_research(self):
  from application.api import SearchRequest
  from pydantic import ValidationError
  self.assertFalse(SearchRequest(query='Q').allow_remote)
  for value in ('true',1,None):
   with self.subTest(value=value),self.assertRaises(ValidationError):SearchRequest(query='Q',allow_remote=value)
  with self.assertRaises(ValidationError):SearchRequest(query='Q',mode='deep_research',allow_remote=True)

class SearchTransportTests(unittest.IsolatedAsyncioTestCase):
 async def exercise(self,*,allow_remote=False,selected='model/small',event_model=None,block=False):
  from aiohttp import web,ClientSession
  from application.query import RAGQuery
  from application.search_routing import SearchModels
  from langchain_core.messages import HumanMessage
  import asyncio
  calls=[];release=asyncio.Event()
  async def chat(request):
   calls.append({'payload':await request.json(),'affinity':request.headers.get('x-switchyard-session-id')})
   response=web.StreamResponse(headers={'Content-Type':'text/event-stream','x-model-router-selected-model':selected});await response.prepare(request)
   event={'model':event_model or selected,'choices':[{'delta':{'content':'first'},'finish_reason':None}]}
   try:
    await response.write(('data: '+json.dumps(event)+'\n\n').encode())
    if block:await release.wait()
    await response.write(('data: '+json.dumps({'model':selected,'choices':[{'delta':{'content':' second'},'finish_reason':'stop'}]})+'\n\ndata: [DONE]\n\n').encode())
   except ConnectionResetError:pass
   return response
  app=web.Application();app.router.add_post('/v1/chat/completions',chat);runner=web.AppRunner(app);await runner.setup();site=web.TCPSite(runner,'127.0.0.1',0);await site.start()
  try:
   async with ClientSession() as http:
    cfg=json.loads(render()['search-client.json']);cfg['endpoint']='http://127.0.0.1:'+str(site._server.sockets[0].getsockname()[1])+'/v1'
    q=RAGQuery(SimpleNamespace(es_ca=None,llm_endpoint='original',llm_model='original'),None,http)
    client=SearchModels(q,cfg).client([HumanMessage(content='Question')],allow_remote=allow_remote)
    stream=client._chat_stream([HumanMessage(content='Question')]);first=await asyncio.wait_for(anext(stream),timeout=2)
    self.assertEqual(first,'first');release.set();rest=[x async for x in stream]
    self.assertEqual(rest,[' second']);self.assertEqual(q.settings.llm_model,'original');self.assertEqual(len(calls),1)
    return client.search_routing,calls
  finally:release.set();await runner.cleanup()
 async def test_incremental_stream_and_local_model(self):
  record,calls=await self.exercise(block=True)
  self.assertEqual(record['tier'],'small');self.assertEqual(calls[0]['payload']['model'],'search-local');self.assertTrue(calls[0]['affinity'])
 async def test_hosted_only_with_explicit_consent(self):
  with self.assertRaises(Conflict):await self.exercise(selected='model/large')
  record,calls=await self.exercise(selected='model/large',allow_remote=True)
  self.assertEqual(record['tier'],'large');self.assertEqual(calls[0]['payload']['model'],'search-hosted')
 async def test_model_cannot_change_midstream_or_be_unapproved(self):
  for changes in ({'selected':'model/other'},{'event_model':'model/medium'}):
   with self.subTest(changes=changes),self.assertRaises(Conflict):await self.exercise(**changes)

class SearchFailureIsolationTests(unittest.IsolatedAsyncioTestCase):
 async def test_failed_stream_does_not_retry_or_switch_after_first_delta(self):
  from tests.application.test_query import QueryTests
  f=QueryTests();f.setUp();self.addCleanup(f.doCleanups);q=f.query;routed=copy.copy(q)
  async def chunks(*args,**kwargs):
   yield 'Visible text'
   raise Conflict('upstream disconnected')
  routed._chat_stream=chunks
  with patch('application.search_routing.answer_client',return_value=routed) as factory:
   stream=q.stream_search('Question',['collection'])
   while (await anext(stream))['event']!='delta':pass
   with self.assertRaises(Conflict):await anext(stream)
  factory.assert_called_once()
 def test_concurrent_clients_have_independent_identity_and_consent(self):
  from application.search_routing import SearchModels
  cfg=json.loads(render()['search-client.json']);q=SimpleNamespace(settings=SimpleNamespace(llm_endpoint='old',llm_model='old'),http=object())
  a=SearchModels(q,cfg).client([],allow_remote=True);b=SearchModels(q,cfg).client([])
  self.assertNotEqual(a.http.affinity,b.http.affinity);self.assertNotIn('model/large',b.http.allowed)
  a.search_routing['model']='model/large';self.assertIsNone(b.search_routing['model'])
