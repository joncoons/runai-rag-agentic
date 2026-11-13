import json,unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock
from application.research_engine import ResearchEngine

class Query:
 mcp_url='http://scoped.test/mcp'
 def _validate(self,*args):pass
 async def _chat(self,*args,**kwargs):return '["compare eligibility"]'
 async def _needs_legal(self,*args):return False
 async def _remote_evidence(self,question,collections,backend,**kwargs):
  self.called.append(question)
  return {'sources':[{'collection_id':'one','record_id':question,'text':'Grounded content','generated_annotation':False,'citation_id':'old'}]}
 def _answer_messages(self,state):self.answer_state=state;return []
 async def _chat_stream(self,*args,**kwargs):
  yield 'Grounded report [S1]';yield ' invalid [S999]'

class ResearchEngineTests(unittest.IsolatedAsyncioTestCase):
 async def test_resume_uses_completed_evidence_and_filters_unknown_citations(self):
  q=Query();q.called=[];repo=SimpleNamespace(checkpoint=AsyncMock(),append=AsyncMock(),complete=AsyncMock())
  lease={'request':{'question':'initial','collection_ids':['one'],'backend':'elasticsearch','search_budget':2},'checkpoint':{'queries':['initial','compare eligibility'],'completed_queries':['initial'],'sources':[{'collection_id':'one','record_id':'initial','text':'Prior evidence','citation_id':'S1','generated_annotation':False}],'warnings':[],'evidence_proofs':[],'include_legal':False}}
  await ResearchEngine(repo,q).execute(lease)
  self.assertEqual(q.called,['compare eligibility'])
  report=''.join(call.args[1] for call in repo.append.await_args_list)
  self.assertIn('[S1]',report);self.assertNotIn('[S999]',report)
  repo.complete.assert_awaited_once();self.assertEqual(len(q.answer_state['sources']),2)
 async def test_empty_collection_scope_never_becomes_model_only_research(self):
  from collection_control.repository import Conflict
  q=Query();q.called=[]
  with self.assertRaisesRegex(Conflict,'Select collections'):
   await ResearchEngine(None,q).execute({'request':{'question':'x','collection_ids':[],'backend':'elasticsearch','search_budget':1},'checkpoint':{}})
  self.assertEqual(q.called,[])

class WebResearchEngineTests(unittest.IsolatedAsyncioTestCase):
 def setup_case(self,*,web=True,collections=None):
  q=Query();q.called=[];repo=SimpleNamespace(checkpoint=AsyncMock(),append=AsyncMock(),complete=AsyncMock())
  provider=SimpleNamespace(search=AsyncMock(return_value={'sources':[{'collection_id':'web:tavily','record_id':'web:fixture','text':'External grounded evidence','generated_annotation':False,'uri':'https://public.example/guide','evidence_origin':'web_search'}],'warnings':[],'proof':{'provider':'tavily','response_sha256':'a'*64}}))
  lease={'request':{'question':'initial','collection_ids':['one'] if collections is None else collections,'backend':'elasticsearch','search_budget':1,'include_web':web},'checkpoint':{}}
  return q,repo,provider,lease
 async def test_combined_search_checkpoints_web_separately_and_tags_evidence(self):
  from unittest.mock import patch
  q,repo,provider,lease=self.setup_case()
  with patch.dict('os.environ',{'RESEARCH_WEB_SEARCH_ENABLED':'true'}):await ResearchEngine(repo,q,web=provider).execute(lease)
  provider.search.assert_awaited_once_with('initial');self.assertEqual(q.called,['initial'])
  self.assertEqual({s['collection_id'] for s in q.answer_state['sources']},{'one','web:tavily'})
  self.assertTrue(q.answer_state['evidence_only'])
  states=[call.args[1] for call in repo.checkpoint.await_args_list]
  self.assertTrue(any({'query':'initial','provider':'tavily'} in state['completed_steps'] for state in states))
 async def test_web_only_does_not_require_or_call_local_mcp(self):
  from unittest.mock import patch
  q,repo,provider,lease=self.setup_case(collections=[]);q.mcp_url=''
  with patch.dict('os.environ',{'RESEARCH_WEB_SEARCH_ENABLED':'true'}):await ResearchEngine(repo,q,web=provider).execute(lease)
  self.assertEqual(q.called,[]);provider.search.assert_awaited_once()
 async def test_opt_out_never_calls_injected_provider_even_when_available(self):
  q,repo,provider,lease=self.setup_case(web=False)
  await ResearchEngine(repo,q,web=provider).execute(lease);provider.search.assert_not_awaited()
 async def test_private_scope_rejects_web_even_if_configured(self):
  from unittest.mock import patch
  from collection_control.repository import Conflict
  q,repo,provider,lease=self.setup_case();lease['workspace_id']='private'
  with patch.dict('os.environ',{'RESEARCH_WEB_SEARCH_ENABLED':'true'}):
   with self.assertRaisesRegex(Conflict,'Private'):await ResearchEngine(repo,q,web=provider).execute(lease)
  provider.search.assert_not_awaited();self.assertEqual(q.called,[])
 async def test_resume_does_not_repeat_a_committed_web_request(self):
  from unittest.mock import patch
  q,repo,provider,lease=self.setup_case();lease['checkpoint']={'queries':['initial'],'completed_queries':[],'sources':[],'warnings':[],'evidence_proofs':[],'include_legal':False,'completed_steps':[{'query':'initial','provider':'tavily'}]}
  with patch.dict('os.environ',{'RESEARCH_WEB_SEARCH_ENABLED':'true'}):await ResearchEngine(repo,q,web=provider).execute(lease)
  provider.search.assert_not_awaited();self.assertEqual(q.called,['initial'])
