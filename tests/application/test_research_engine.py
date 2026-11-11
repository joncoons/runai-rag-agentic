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
