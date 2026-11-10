import copy,unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock
from tests.application import test_query

class SimpleSearchTests(unittest.IsolatedAsyncioTestCase):
 def setUp(self):
  self.fixture=test_query.QueryTests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups);self.q=self.fixture.query;self.q._revalidate=AsyncMock()
 async def test_global_ten_before_rerank_and_best_five_after(self):
  base=self.fixture.record
  manifests=[{'collection_id':'one','handles':{'elasticsearch':'one'},'operation_id':'one','revision':1},{'collection_id':'two','handles':{'elasticsearch':'two'},'operation_id':'two','revision':1}]
  self.q._resolve=AsyncMock(return_value=manifests)
  async def fetch(fn,m,*a,**kw):
   return [{**copy.deepcopy(base),'record_id':m['collection_id']+str(i),'score':100-i if m['collection_id']=='one' else 20-i} for i in range(10)]
  self.q._published_fetch=fetch
  self.q._source=AsyncMock(side_effect=lambda r,m:{'record_id':r['record_id'],'collection_id':m['collection_id'],'score':r['score'],'text':'text','content_role':'primary'})
  self.q._rerank=AsyncMock(side_effect=lambda query,rows:list(reversed(rows)))
  result=await self.q.query_evidence('Question',['one','two'])
  candidates=self.q._rerank.await_args.args[1]
  self.assertEqual(len(candidates),10)
  self.assertTrue(all(r['collection_id']=='one' for r in candidates),'Global top ten may all come from one collection')
  self.assertEqual(len(result['sources']),5)
  self.assertEqual(result['sources'][0]['record_id'],'one9')
 async def test_legal_decision_precedes_one_retrieval(self):
  order=[]
  async def legal(query):order.append('decision');return True
  async def evidence(*a,**kw):order.append(('retrieval',kw.get('include_legal')));return {'sources':[],'warnings':[],'publications':[]}
  self.q._needs_legal=legal;self.q.query_evidence=evidence
  await self.q._evidence({'query':'Terms','collection_ids':['collection'],'mode':'search','backend':'elasticsearch'})
  self.assertEqual(order,['decision',('retrieval',True)])
 async def test_search_uses_no_postgres_queries_or_publication_checks(self):
  self.q.pool.fetchrow=AsyncMock(side_effect=AssertionError('Search must not query Postgres'))
  self.q.pool.fetch=AsyncMock(side_effect=AssertionError('Search must not query Postgres'))
  self.q.pool.execute=AsyncMock(side_effect=AssertionError('Search must not write Postgres'))
  self.q.catalog.entries['collection'].update(handles={'elasticsearch':'rag_agentic_inc_'+'a'*32+'_es'},versions={'a'*64},legal_versions=frozenset())
  self.q.repository.resolve.side_effect=AssertionError('No publication resolution')
  self.q.repository.revalidate.side_effect=AssertionError('No final publication check')
  result=await self.q.search('Question',['collection'])
  self.assertTrue(result['answer'])
  self.q.pool.fetchrow.assert_not_awaited();self.q.pool.fetch.assert_not_awaited();self.q.pool.execute.assert_not_awaited()
  self.assertEqual(self.q._elasticsearch.await_args.kwargs['versions'],['a'*64])
 async def test_best_five_are_not_replaced_by_a_lower_ranked_content_role(self):
  base=self.fixture.record
  manifest={'collection_id':'one','handles':{'elasticsearch':'one'},'revision':1}
  self.q._resolve=AsyncMock(return_value=[manifest])
  self.q._published_fetch=AsyncMock(return_value=[{**copy.deepcopy(base),'record_id':str(i),'score':10-i} for i in range(10)])
  self.q._source=AsyncMock(side_effect=lambda r,m:{'record_id':r['record_id'],'collection_id':'one','score':r['score'],'text':'text','content_role':'legal_disclosure' if int(r['record_id'])>=5 else 'primary'})
  self.q._rerank=AsyncMock(side_effect=lambda query,rows:list(reversed(rows)))
  result=await self.q.query_evidence('Legal question',['one'],include_legal=True)
  self.assertEqual([s['record_id'] for s in result['sources']],['9','8','7','6','5'])
