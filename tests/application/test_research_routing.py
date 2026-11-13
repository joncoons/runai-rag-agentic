import copy,json,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock,patch
from aiohttp import web,ClientSession
from langchain_core.messages import HumanMessage
from application.query import RAGQuery
from application.research_routing import ResearchModels

class ResearchRoutingTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.calls=[];self.unknown=False;self.model_override=None;self.answers=['["useful query"]'];self.state={}
  async def answer(request):
   p=await request.json();self.calls.append((p,dict(request.headers)))
   model='model/capable' if p['model']=='research-capable' else 'model/efficient'
   model=self.model_override or model
   headers={'x-model-router-selected-model':'unapproved' if self.unknown else model}
   text=self.answers.pop(0) if self.answers else '["useful query"]'
   if p.get('stream'):
    response=web.StreamResponse(headers={**headers,'Content-Type':'text/event-stream'});await response.prepare(request)
    for event in [{'choices':[{'delta':{'content':'First '},'finish_reason':None}]},{'choices':[{'delta':{'content':'answer'},'finish_reason':'stop'}]}]:await response.write(('data: '+json.dumps(event)+'\n\n').encode())
    await response.write(b'data: {"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}\n\n');await response.write(b'data: [DONE]\n\n');await response.write_eof();return response
   return web.json_response({'model':model,'choices':[{'message':{'content':text},'finish_reason':'stop'}],'usage':{'prompt_tokens':10,'completion_tokens':3,'total_tokens':13}},headers=headers)
  app=web.Application();app.router.add_post('/v1/chat/completions',answer);self.runner=web.AppRunner(app);await self.runner.setup();site=web.TCPSite(self.runner,'127.0.0.1',0);await site.start();port=site._server.sockets[0].getsockname()[1]
  self.url=f'http://127.0.0.1:{port}/v1';self.tmp=tempfile.TemporaryDirectory();self.config=Path(self.tmp.name)/'client.json'
  self.config.write_text(json.dumps({'schema_version':1,'policy_sha256':'a'*64,'endpoint':self.url,'targets':{'model/efficient':'research-efficient','model/capable':'research-capable'},'capable_model':'model/capable','phases':{'plan':'research-plan','document_selection':'research-document-selection','synthesis':'research-synthesis'}}))
  self.env=patch.dict('os.environ',{'RESEARCH_ROUTING_ENABLED':'true','RESEARCH_ROUTING_CONFIG_FILE':str(self.config)});self.env.start()
  self.http=ClientSession();self.q=object.__new__(RAGQuery);self.q.settings=SimpleNamespace(llm_endpoint=self.url,llm_model='original-fixed');self.q.http=self.http
  self.saved=[]
  async def checkpoint(lease,state,stage):self.saved.append(copy.deepcopy(state))
  self.repo=SimpleNamespace(checkpoint=AsyncMock(side_effect=checkpoint));self.lease={'run_id':'job-one'};self.models=ResearchModels(self.repo,self.q,self.lease)
 async def asyncTearDown(self):
  await self.http.close();await self.runner.cleanup();self.tmp.cleanup();self.env.stop()
 async def test_disabled_preserves_fixed_model_and_does_not_add_headers_or_state(self):
  with patch.dict('os.environ',{'RESEARCH_ROUTING_ENABLED':'false'}):m=ResearchModels(self.repo,self.q,self.lease)
  await m.json_strings(self.state,'plan',[HumanMessage(content='Question')],max_tokens=128,max_items=2)
  self.assertEqual(self.calls[0][0]['model'],'original-fixed');self.assertNotIn('X-Switchyard-Session-Id',self.calls[0][1]);self.assertEqual(self.state,{})
 async def test_restart_pins_selected_model_and_keeps_original_client_unchanged(self):
  await self.models.json_strings(self.state,'plan',[HumanMessage(content='Question')],max_tokens=128,max_items=2)
  restored=copy.deepcopy(self.saved[-1]);await ResearchModels(self.repo,self.q,self.lease).json_strings(restored,'plan',[HumanMessage(content='Question')],max_tokens=128,max_items=2)
  self.assertEqual([c[0]['model'] for c in self.calls],['research-plan','research-efficient']);self.assertEqual(self.q.settings.llm_model,'original-fixed')
  phase=restored['model_routing']['phases']['plan'];self.assertEqual(phase['model'],'model/efficient');self.assertEqual(phase['calls'][0]['usage']['total_tokens'],13)
  self.assertNotIn('Question',json.dumps(restored))
 async def test_different_jobs_and_phases_have_distinct_affinity(self):
  for run,phase in [('job-one','plan'),('job-one','document_selection'),('job-two','plan')]:
   await ResearchModels(self.repo,self.q,{'run_id':run}).json_strings({},phase,[HumanMessage(content='Question')],max_tokens=128,max_items=2)
  self.assertEqual(len({c[1]['x-switchyard-session-id'] for c in self.calls}),3)
 async def test_concurrent_jobs_do_not_mutate_shared_client_or_share_affinity(self):
  import asyncio
  states=[{},{}]
  await asyncio.gather(*(ResearchModels(self.repo,self.q,{'run_id':run}).json_strings(state,'plan',[HumanMessage(content='Question')],max_tokens=128,max_items=2) for run,state in zip(['one','two'],states)))
  self.assertEqual(self.q.settings.llm_model,'original-fixed');self.assertIs(self.q.http,self.http)
  self.assertNotEqual(self.calls[0][1]['x-switchyard-session-id'],self.calls[1][1]['x-switchyard-session-id'])
  self.assertIsNot(states[0]['model_routing'],states[1]['model_routing'])
 async def test_invalid_output_escalates_once_and_stays_bounded_across_resume(self):
  from collection_control.repository import Conflict
  self.answers=['not JSON','["valid"]'];result=await self.models.json_strings(self.state,'plan',[HumanMessage(content='Question')],max_tokens=128,max_items=2)
  self.assertEqual(result,['valid']);self.assertEqual([c[0]['model'] for c in self.calls],['research-plan','research-capable'])
  self.answers=['invalid again']
  with self.assertRaises(Conflict):await self.models.json_strings(self.state,'plan',[HumanMessage(content='Question')],max_tokens=128,max_items=2)
  self.assertEqual(len(self.calls),3)
 async def test_exhausted_validation_cannot_issue_another_model_request_after_resume(self):
  from collection_control.repository import Conflict
  self.answers=['invalid initial','invalid capable','["should not run"]']
  with self.assertRaises(Conflict):await self.models.json_strings(self.state,'plan',[HumanMessage(content='Question')],max_tokens=128,max_items=2)
  self.assertEqual(len(self.calls),2)
  restored=copy.deepcopy(self.saved[-1])
  with self.assertRaises(Conflict):await ResearchModels(self.repo,self.q,self.lease).json_strings(restored,'plan',[HumanMessage(content='Question')],max_tokens=128,max_items=2)
  self.assertEqual(len(self.calls),2)
 async def test_unknown_model_rejected_before_text_and_stream_reaches_consumer(self):
  from collection_control.repository import Conflict
  self.unknown=True
  with self.assertRaises(Conflict):
   async for _ in self.models.stream(self.state,'synthesis',[HumanMessage(content='Question')],max_tokens=128):self.fail('Unapproved text escaped')
 async def test_stream_remains_incremental_and_phase_choice_is_saved_first(self):
  chunks=[]
  async for text in self.models.stream(self.state,'synthesis',[HumanMessage(content='Question')],max_tokens=128):
   self.assertTrue(self.saved);chunks.append(text)
  self.assertEqual(chunks,['First ','answer']);self.assertEqual(self.state['model_routing']['phases']['synthesis']['model'],'model/efficient')
  call=self.state['model_routing']['phases']['synthesis']['calls'][0];self.assertEqual(call['usage']['total_tokens'],12);self.assertGreaterEqual(call['first_content_ms'],0)
 async def test_fixed_alias_cannot_switch_to_another_approved_model(self):
  from collection_control.repository import Conflict
  await self.models.json_strings(self.state,'plan',[HumanMessage(content='Question')],max_tokens=128,max_items=2)
  self.model_override='model/capable'
  with self.assertRaisesRegex(Conflict,'pinned'):
   await self.models.json_strings(self.state,'plan',[HumanMessage(content='Question')],max_tokens=128,max_items=2)
  self.assertEqual(self.state['model_routing']['phases']['plan']['model'],'model/efficient')
 async def test_policy_change_or_disabling_never_silently_reclassifies_existing_phase(self):
  from collection_control.repository import Conflict
  await self.models.json_strings(self.state,'plan',[HumanMessage(content='Question')],max_tokens=128,max_items=2)
  d=json.loads(self.config.read_text());d['policy_sha256']='b'*64;self.config.write_text(json.dumps(d))
  with self.assertRaises(Conflict):await ResearchModels(self.repo,self.q,self.lease).json_strings(self.state,'plan',[],max_tokens=128,max_items=2)
  with patch.dict('os.environ',{'RESEARCH_ROUTING_ENABLED':'false'}):
   with self.assertRaises(Conflict):await ResearchModels(self.repo,self.q,self.lease).json_strings(self.state,'plan',[],max_tokens=128,max_items=2)
