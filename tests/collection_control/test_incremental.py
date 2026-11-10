import copy,json,unittest,os
from collection_control.artifacts import content_digest,record_digest
from tests.collection_control.test_writer import fixture
from collection_control.incremental import IncrementalElasticsearch,version_handle

class InMemoryES(IncrementalElasticsearch):
 def __init__(self):
  super().__init__(None,'unused');self.indices={};self.creates=0;self.writes=0
 async def _request(self,method,path,**kw):
  if method=='GET' and '/' not in path:
   if path not in self.indices:raise __import__('collection_control.repository',fromlist=['Conflict']).Conflict('Elasticsearch operation failed: HTTP 404')
   return {path:{}}
  if method=='PUT':self.indices[path]={};self.creates+=1;return {}
  if path=='_bulk':
   lines=kw['data'].splitlines();items=[]
   for a,b in zip(lines[::2],lines[1::2]):
    op=json.loads(a)['create'];doc=json.loads(b);self.indices[op['_index']][op['_id']]=doc;self.writes+=1
    items.append({'create':{'status':201,'_id':op['_id']}})
   return {'items':items}
  handle,action=path.split('/',1)
  if action=='_mget':
   return {'docs':[{'_id':v,'found':v in self.indices[handle],**({'_source':copy.deepcopy(self.indices[handle][v])} if v in self.indices[handle] else {})} for v in kw['json']['ids']]}
  if action=='_refresh':return {}
  raise AssertionError((method,path))

class IncrementalTests(unittest.IsolatedAsyncioTestCase):
 async def test_repeated_snapshot_writes_only_changed_record_versions_and_never_recreates_index(self):
  es=InMemoryES();x=fixture();h=version_handle('collection','elasticsearch')
  self.assertEqual(await es.write_verify(h,x),(content_digest(x['records']),1))
  await es.write_verify(h,x)
  y=copy.deepcopy(x);y['records'][0]['text']='replacement';y['records'][0]['metadata']['content_sha256']=__import__('hashlib').sha256(b'replacement').hexdigest()
  await es.write_verify(h,y)
  self.assertEqual(es.creates,1);self.assertEqual(es.writes,2)
  self.assertIn(record_digest(x['records'][0]),es.indices[h])
  self.assertIn(record_digest(y['records'][0]),es.indices[h])
 async def test_unlink_changes_logical_receipt_without_deleting_old_immutable_bytes(self):
  es=InMemoryES();x=fixture();h=version_handle('collection','elasticsearch');await es.write_verify(h,x)
  empty={**x,'records':[]};self.assertEqual(await es.write_verify(h,empty),(content_digest([]),0));self.assertEqual(es.writes,1)
 async def test_query_filters_exact_published_versions_before_knn_and_source_lookup(self):
  from application.query import RAGQuery
  from unittest.mock import AsyncMock
  q=object.__new__(RAGQuery);q._es_request=AsyncMock(return_value={'hits':{'hits':[]}})
  h=version_handle('collection','elasticsearch')
  await q._elasticsearch(h,[1.0],versions=['a'*64])
  body=q._es_request.await_args.args[2]
  self.assertEqual(body['knn']['filter'],{'terms':{'record_sha256':['a'*64]}})
  await q._elasticsearch(h,None,record_id='source1/page1',versions=['b'*64])
  body=q._es_request.await_args.args[2]
  self.assertIn({'terms':{'record_sha256':['b'*64]}},body['query']['bool']['filter'])
 async def test_empty_published_set_does_not_query_any_physical_rows(self):
  from application.query import RAGQuery
  from unittest.mock import AsyncMock
  q=object.__new__(RAGQuery);q._es_request=AsyncMock()
  self.assertEqual(await q._elasticsearch(version_handle('collection','elasticsearch'),[1.0],versions=[]),[])
  q._es_request.assert_not_awaited()

from tests.collection_control.test_repository import RepositoryTests as _RepositoryFixture
@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'Requires disposable loopback qualification PostgreSQL')
class IncrementalRepositoryTests(unittest.IsolatedAsyncioTestCase):
 asyncSetUp=_RepositoryFixture.asyncSetUp
 asyncTearDown=_RepositoryFixture.asyncTearDown
 intent=_RepositoryFixture.intent
 async def test_incremental_retries_keep_persistent_handles_but_fence_receipts(self):
  from pathlib import Path
  from collection_control.repository import ControlRepository,Fenced
  await self.pool.execute(Path('deployment/collection-control/006_incremental_projections.sql').read_text())
  repo=ControlRepository(self.pool,handle_prefix='rag_agentic',incremental=True)
  op=await self.intent();old=await repo.claim(op['operation_id'],ttl_seconds=60)
  self.assertEqual(old['storage_mode'],'record_versions')
  await self.pool.execute("UPDATE collection_control.operations SET lease_until=clock_timestamp()-interval '1 second' WHERE operation_id=$1",op['operation_id'])
  new=await repo.claim(op['operation_id'],ttl_seconds=60)
  self.assertEqual(old['handles'],new['handles'])
  with self.assertRaises(Fenced):await repo.receipt(old,'elasticsearch','c'*64,1)
  await repo.receipt(new,'elasticsearch','c'*64,1);await repo.publish(new)
  self.assertEqual((await repo.resolve('test'))['storage_mode'],'record_versions')

class PageBatchTests(unittest.IsolatedAsyncioTestCase):
 async def test_collection_reconciliation_does_not_lock_unrelated_collections(self):
  from application.worker import ApplicationWorker
  from unittest.mock import AsyncMock,MagicMock
  from types import SimpleNamespace
  c=AsyncMock();c.transaction=MagicMock();c.transaction.return_value.__aenter__=AsyncMock();c.transaction.return_value.__aexit__=AsyncMock()
  c.fetch.return_value=[]
  pool=MagicMock();pool.acquire.return_value.__aenter__=AsyncMock(return_value=c);pool.acquire.return_value.__aexit__=AsyncMock()
  w=object.__new__(ApplicationWorker);w.pool=pool
  await w._reconcile_collection('one')
  self.assertFalse(any('FOR UPDATE OF c' in call.args[0] for call in c.fetch.await_args_list))

from tests.collection_control.test_web_children_pg import WebChildrenTests as _WebFixture
@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'Requires disposable loopback qualification PostgreSQL')
class CoalescedPageTests(unittest.IsolatedAsyncioTestCase):
 asyncSetUp=_WebFixture.asyncSetUp
 asyncTearDown=_WebFixture.asyncTearDown
 fixture=_WebFixture.fixture
 finalized=_WebFixture.finalized
 publish=_WebFixture.publish
 group=_WebFixture.group
 materialize=_WebFixture.materialize
 candidate=_WebFixture.candidate
 retain=_WebFixture.retain
 page_group=_WebFixture.page_group
 association=_WebFixture.association
 async def test_ready_pages_share_one_dual_store_operation_with_each_original_proof(self):
  from pathlib import Path
  from collection_control.artifacts import load_artifact
  await self.pool.execute(Path('deployment/collection-control/006_incremental_projections.sql').read_text())
  g1,p1,_=await self.page_group(document='page-a');g2,p2,m2=await self.page_group(document='page-b')
  await self.candidate(g1,p1);await self.candidate(g2,p2)
  op=await self.group_repo.admit_group(g1,batch=True)
  self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.web_groups WHERE operation_id=$1',op['operation_id']),2)
  self.assertEqual(len(load_artifact(self.snapshots,op['artifact_sha256'])['records']),2)
  await self.publish(op['operation_id'])
  from collection_control.web_groups import resolve_graph
  async with self.pool.acquire() as c:
   col=await c.fetchrow("SELECT * FROM collection_control.collections WHERE collection_id='collection'")
   graph=await resolve_graph(c,col)
  self.assertEqual({p['document_id']:p['group_id'] for p in graph['pages']},{'page-a':str(g1),'page-b':str(g2)})
  binary=await self.fixture('binary-a');ref=await self.association(g2,m2,binary)
  e,lease,body,_=binary
  await self.group_repo.commit_child_candidate(e,lease_token=lease,artifact=self.finalized(e,body),acquisition=ref)
  child=await self.group_repo.admit_child(ref)
  _,snapshot=await self.publish(child['operation_id']);self.assertEqual(len(snapshot['records']),3)
 async def test_cancelled_ready_candidate_cannot_poison_another_pages_batch(self):
  from pathlib import Path
  await self.pool.execute(Path('deployment/collection-control/006_incremental_projections.sql').read_text())
  g1,p1,_=await self.page_group(document='page-a');g2,p2,_=await self.page_group(document='page-b')
  await self.candidate(g1,p1);await self.candidate(g2,p2)
  from uuid import UUID
  await self.pool.execute("UPDATE public.jobs SET status='cancelled' WHERE job_id=$1",UUID(p2[0].job_id))
  op=await self.group_repo.admit_group(g1,batch=True)
  self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.web_groups WHERE operation_id=$1',op['operation_id']),1)
  await self.publish(op['operation_id'])

class UnverifiedVersionTests(unittest.IsolatedAsyncioTestCase):
 async def test_existing_unverified_physical_key_with_wrong_bytes_is_rejected(self):
  from collection_control.repository import Conflict
  es=InMemoryES();x=fixture();h=version_handle('corrupt-version-test','elasticsearch')
  await es.write_verify(h,x)
  es.indices[h][record_digest(x['records'][0])]['text']='incomplete or corrupted prior attempt'
  with self.assertRaises(Conflict):await es.write_verify(h,x)

class PagePublicationPriorityTests(unittest.IsolatedAsyncioTestCase):
 async def test_ready_html_yields_background_child_publication_without_model_or_proof_work(self):
  from application.worker import ApplicationWorker
  from collection_control.repository import Conflict
  from unittest.mock import AsyncMock
  from types import SimpleNamespace
  w=object.__new__(ApplicationWorker);w.pool=SimpleNamespace(fetchval=AsyncMock(return_value=True),execute=AsyncMock())
  bridge=SimpleNamespace(admit_child=AsyncMock(return_value=None))
  w._deferred_bridge=AsyncMock(return_value=(bridge,{}))
  w.settings=SimpleNamespace(models={'models':{'embed':{'model':'test'}}},contract={'embedding_checkpoint_sha256':'e'*64})
  w._asset_parent_embedding=lambda text:[1.0]+[0.0]*2047
  w.reconcile_publications=AsyncMock();w.refresh_jobs=AsyncMock()
  with self.assertRaises(Conflict):await w._admit_deferred_asset({'collection_id':'c','source_revision_id':'r'},1)
  w._deferred_bridge.assert_not_awaited();bridge.admit_child.assert_not_awaited()

class VerifiedDeltaTests(unittest.IsolatedAsyncioTestCase):
 async def test_durable_verification_avoids_full_corpus_backend_reads(self):
  from types import SimpleNamespace
  from unittest.mock import AsyncMock
  es=InMemoryES();x=fixture();h=version_handle('verified-delta','elasticsearch');await es.write_verify(h,x)
  es.pool=SimpleNamespace(fetch=AsyncMock(return_value=[{'record_sha256':record_digest(x['records'][0])}]),execute=AsyncMock())
  requests=[];original=es._request
  async def tracked(method,path,**kw):
   requests.append(path);return await original(method,path,**kw)
  es._request=tracked
  await es.write_verify(h,x)
  self.assertNotIn(h+'/_mget',requests)
 async def test_missing_projection_bootstraps_despite_retained_verification_entries(self):
  from types import SimpleNamespace
  from unittest.mock import AsyncMock
  es=InMemoryES();x=fixture();h=version_handle('recreated-projection','elasticsearch')
  es.pool=SimpleNamespace(fetch=AsyncMock(return_value=[{'record_sha256':record_digest(x['records'][0])}]),execute=AsyncMock())
  await es.write_verify(h,x)
  self.assertEqual(es.writes,1);self.assertIn(record_digest(x['records'][0]),es.indices[h])

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'Requires disposable loopback qualification PostgreSQL')
class PendingPublicationFastGateTests(CoalescedPageTests):
 async def test_pending_publication_yields_before_expensive_group_validation(self):
  from unittest.mock import patch,AsyncMock
  from collection_control.repository import Conflict
  from collection_control.artifacts import canonical
  import hashlib
  g,item,_=await self.page_group(document='page-a');await self.candidate(g,item)
  await self.repo.prepare('collection','other-pending','snapshot',0,'b'*64,hashlib.sha256(canonical(self.contract)).hexdigest())
  with patch('collection_control.web_groups._verify_manifest',new=AsyncMock(side_effect=AssertionError('Heavy proof validation must not hold the writer lock for a rejected intent'))):
   with self.assertRaisesRegex(Conflict,'pending'):await self.group_repo.admit_group(g,batch=True)
