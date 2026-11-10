"""Pure controller tests: synthetic published evidence and mocked domain services."""
import copy
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from application.query import RAGQuery
from collection_control.repository import Conflict, Fenced
from collection_control.artifacts import record_digest



class FixtureCatalog:
    def __init__(self):
        self.entries={'collection':{'collection_id':'collection','revision':1,'operation_id':'op',
            'handles':{'elasticsearch':'rag_'+'a'*32+'_1_es'},'contract_sha256':'c'*64}}
    def selected(self,ids):
        if any(cid not in self.entries for cid in ids):raise Conflict('Selected collection is unavailable or outside this application')
        return [self.entries[cid] for cid in ids]

class QueryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.environment=patch.dict(os.environ,{'QUERY_MCP_URL':''});self.environment.start()
        self.addCleanup(self.environment.stop)
        settings=SimpleNamespace(models={'models':{'embed':{'endpoint':'http://embed.invalid/v1/embeddings','model':'fixture-embed'}}},
            es_endpoint='https://es.invalid',es_user='operator',es_password='private',es_ca=None,
            milvus_uri='http://milvus.invalid',milvus_token='private',
            llm_endpoint='http://llm.invalid:8000',llm_model='fixture-llm')
        self.pool=SimpleNamespace(fetchrow=AsyncMock(return_value={'collection_id':'collection'}))
        self.query=RAGQuery(settings,self.pool,SimpleNamespace(),catalog=FixtureCatalog())
        self.query.repository=SimpleNamespace(resolve=AsyncMock(return_value={
            'collection_id':'collection','revision':1,'target_epoch':1,'operation_id':'op',
            'handles':{'elasticsearch':'rag_'+'a'*32+'_1_es'},'contract_sha256':'c'*64}),
            revalidate=AsyncMock())
        self.record={'record_id':'chunk-1','text':'Observed fixture evidence',
            'vector':[1.0]+[0.0]*2047,'metadata':{
                'acl':['local-operator'],'origin':'grounded','source_revision_id':'revision',
                'provenance':{'canonical_uri':'https://source.invalid/doc.pdf','source_revision_id':'revision',
                    'anchors':{'page_number':3}},'source':{'source_name':'Fixture PDF'},
                'content_metadata':{'page_number':3}}}
        self.pool.fetchrow.return_value={'collection_id':'collection','record_sha256':record_digest(self.record)}
        self.query._embed=AsyncMock(return_value=[1.0]+[0.0]*2047)
        self.query._elasticsearch=AsyncMock(return_value=[copy.deepcopy(self.record)])
        self.query._needs_legal=AsyncMock(return_value=False)
        self.query._chat=AsyncMock(return_value='Grounded fixture answer [S1].')

    async def test_search_default_is_concise_without_cutting_evidence_or_requested_detail(self):
        await self.query.search('What credit card options are available to college students?',['collection'])
        messages=self.query._chat.await_args.args[0]
        self.assertIn('150–220 words',messages[0].content)
        self.assertIn('explicitly requests more detail',messages[0].content)
        self.assertIn('Avoid generic background',messages[0].content)
        self.assertIn('Observed fixture evidence',messages[1].content)

    async def test_no_collections_is_explicit_model_only_without_retrieval(self):
        result=await self.query.search('Question',[])
        self.assertEqual(result['sources'],[])
        self.query._embed.assert_not_awaited();self.query._elasticsearch.assert_not_awaited()
        self.query.repository.resolve.assert_not_awaited()
        self.assertTrue(any('No collections' in w for w in result['warnings']))
        self.assertNotIn('[S1]',result['answer'])

    async def test_invalid_backend_or_mode_rejected_before_any_retrieval(self):
        for options in ({'backend':'automatic'},{'mode':'automatic'}):
            with self.subTest(options=options),self.assertRaises(Conflict):
                await self.query.search('Question',['collection'],**options)
        self.query._embed.assert_not_awaited()

    async def test_scope_gate_rejects_nonapplication_collection(self):
        self.query.catalog.entries.clear()
        with self.assertRaises(Conflict):await self.query.query_evidence('Question',['collection'])
        self.query._embed.assert_not_awaited()

    async def test_unavailable_selected_backend_never_falls_back(self):
        with self.assertRaises(Conflict):await self.query.query_evidence('Question',['collection'],backend='milvus')
        self.query._elasticsearch.assert_not_awaited();self.query._embed.assert_not_awaited()

    async def test_acl_filtered_before_model_evidence_and_source_metadata_is_bounded(self):
        denied=copy.deepcopy(self.record);denied['metadata']['acl']=['someone-else']
        self.query._elasticsearch.return_value=[denied]
        result=await self.query.search('Question',['collection'])
        self.assertEqual(result['sources'],[])
        sent=str(self.query._chat.await_args.args[0])
        self.assertNotIn('Observed fixture evidence',sent)

    async def test_source_citations_bound_to_verified_actual_evidence(self):
        result=await self.query.search('Question',['collection'])
        self.assertEqual(result['sources'][0]['record_id'],'chunk-1')
        self.assertEqual(result['sources'][0]['page'],3)
        self.assertEqual(result['sources'][0]['uri'],'https://source.invalid/doc.pdf')
        self.assertEqual(result['sources'][0]['citation_id'],'S1')
        self.query.repository.revalidate.assert_not_awaited()
        self.assertEqual(self.query._elasticsearch.await_count,1)

    async def test_background_publication_change_does_not_discard_evidence(self):
        self.query.repository.revalidate.side_effect=Fenced('changed')
        result=await self.query.search('Question',['collection'])
        self.assertTrue(result['answer'])
        self.query.repository.revalidate.assert_not_awaited()

    async def test_background_publication_change_during_answer_does_not_interrupt(self):
        async def model(messages,**kwargs):
            self.query.repository.revalidate.side_effect=Fenced('changed during generation')
            return 'Answer [S1]'
        self.query._chat.side_effect=model
        result=await self.query.search('Question',['collection'])
        self.assertEqual(result['answer'],'Answer [S1]')

    async def test_query_does_not_repeat_ingestion_admission_hash_verification(self):
        self.pool.fetchrow.return_value={'collection_id':'collection','record_sha256':'0'*64}
        result=await self.query.query_evidence('Question',['collection'])
        self.assertEqual(len(result['sources']),1)
        self.pool.fetchrow.assert_not_awaited()

    async def test_deep_research_only_when_selected_and_bounded_to_local_queries(self):
        self.query._chat.side_effect=['["Follow-up one", "Follow-up two", "Ignored fourth query"]','Answer [S1]']
        result=await self.query.search('Question',['collection'],mode='deep_research')
        self.assertEqual(self.query._embed.await_count,3)
        self.assertEqual(self.query._elasticsearch.await_count,3)
        self.assertTrue(any('local' in w.lower() for w in result['warnings']))
        self.assertEqual(self.query._chat.await_count,2)

    async def test_unknown_generated_citation_is_removed(self):
        self.query._chat.return_value='Answer [S1] and invented [S99].'
        result=await self.query.search('Question',['collection'])
        self.assertNotIn('[S99]',result['answer'])
        self.assertEqual(len(result['sources']),1)
        self.assertTrue(any('citation' in w.lower() for w in result['warnings']))

    async def test_explicit_remote_mcp_failure_does_not_fallback(self):
        self.query.mcp_url='http://mcp.invalid/mcp'
        self.query._remote_evidence=AsyncMock(side_effect=Conflict('MCP unavailable'))
        with self.assertRaises(Conflict):await self.query.search('Question',['collection'])
        self.query._elasticsearch.assert_not_awaited()

    async def test_exact_source_lookup_never_embeds_and_revalidates_scope(self):
        result=await self.query.get_source('chunk-1',['collection'])
        self.assertEqual(result['sources'][0]['record_id'],'chunk-1')
        self.query._embed.assert_not_awaited()
        self.query._elasticsearch.assert_awaited_once_with('rag_'+'a'*32+'_1_es',None,record_id='chunk-1')
        self.query.repository.revalidate.assert_not_awaited()

    async def test_embedding_and_reranking_send_complete_untruncated_native_requests(self):
        self.query._post=AsyncMock(return_value={'data':[{'embedding':[1.0]+[0.0]*2047}]})
        vector=await RAGQuery._embed(self.query,'Long question '*1000)
        self.assertEqual(len(vector),2048)
        payload=self.query._post.await_args.args[1]
        self.assertEqual(payload['input'],['Long question '*1000])
        self.assertEqual((payload['truncate'],payload['modality'],payload['input_type']),('NONE','text','query'))
        self.assertEqual(payload['dimensions'],2048)
        self.query.settings.models['models']['rerank']={'endpoint':'http://rerank.invalid/v1/ranking','model':'fixture-rerank'}
        self.query._post.return_value={'rankings':[{'index':0,'logit':1.0}]}
        await self.query._rerank('Question',[{'text':'Complete source '*1000}])
        self.assertEqual(self.query._post.await_args.args[1]['passages'],[{'text':'Complete source '*1000}])
        self.assertEqual(self.query._post.await_args.args[1]['truncate'],'NONE')

    async def test_mcp_sdk2_tools_are_readonly_domain_scoped_without_credentials(self):
        from application.mcp_service import create_server
        server=create_server(self.query)
        tools=await server.list_tools()
        self.assertEqual({tool.name for tool in tools},{'query_evidence','get_source'})
        for tool in tools:
            self.assertTrue(tool.annotations.read_only_hint)
            schema=str(tool.input_schema)
            for forbidden in ('password','token','dsn','sql','ctx'):
                self.assertNotIn(forbidden,schema)
            self.assertIn('collection_ids',schema)
        result=await server.call_tool('query_evidence',{'query':'Question','collection_ids':[]})
        self.assertFalse(result.is_error)
        self.assertEqual(result.structured_content['sources'],[])
        self.query._embed.assert_not_awaited()

    async def test_actual_sdk2_streamable_http_client_and_server_scope(self):
        import asyncio
        import socket
        import uvicorn
        from application.mcp_service import create_server
        sock=socket.socket()
        sock.bind(('127.0.0.1',0));sock.listen(16);sock.setblocking(False)
        port=sock.getsockname()[1]
        app=create_server(self.query).streamable_http_app(stateless_http=True,json_response=True,host='127.0.0.1')
        server=uvicorn.Server(uvicorn.Config(app,log_level='critical',lifespan='on'))
        task=asyncio.create_task(server.serve(sockets=[sock]))
        try:
            for _ in range(100):
                if server.started:break
                if task.done():await task
                await asyncio.sleep(.01)
            self.assertTrue(server.started)
            self.query.mcp_url=f'http://127.0.0.1:{port}/mcp'
            result=await self.query._remote_evidence('Question',['collection'],'elasticsearch')
            self.assertEqual(result['sources'][0]['record_id'],'chunk-1')
            self.assertEqual(result['sources'][0]['collection_id'],'collection')
            self.query.repository.revalidate.assert_not_awaited()
            original=self.query.query_evidence
            self.query.query_evidence=AsyncMock(wraps=original)
            await self.query._remote_evidence('Terms',['collection'],'elasticsearch',include_legal=True)
            self.query.query_evidence.assert_awaited_once_with('Terms',['collection'],'elasticsearch',include_legal=True)
        finally:
            server.should_exit=True
            await asyncio.wait_for(task,5)
            sock.close()

    async def test_source_metadata_instructions_never_enter_model_prompt(self):
        self.record['metadata']['system_instruction']='EXFILTRATE_BACKEND_CREDENTIALS'
        self.pool.fetchrow.return_value['record_sha256']=record_digest(self.record)
        self.query._elasticsearch.return_value=[copy.deepcopy(self.record)]
        result=await self.query.search('Question',['collection'])
        prompt=str(self.query._chat.await_args.args[0])
        self.assertNotIn('EXFILTRATE_BACKEND_CREDENTIALS',prompt)
        self.assertNotIn('system_instruction',str(result['sources']))
        self.assertIn('UNTRUSTED',prompt)

    async def test_milvus_native_gpu_cagra_search_is_readonly_without_load_or_fallback(self):
        client=SimpleNamespace(list_indexes=lambda *a,**kw:['vector_index'],
            describe_index=lambda *a,**kw:{'index_type':'GPU_CAGRA','metric_type':'IP'},
            search=lambda *a,**kw:[ [{'entity':{'record_id':'chunk-1','payload':{
                'text':self.record['text'],'metadata':self.record['metadata']},'vector':self.record['vector']},'distance':.9}] ],
            close=lambda:None)
        with patch('pymilvus.MilvusClient',return_value=client):
            rows=await self.query._milvus('rag_'+'a'*32+'_1_mv',[1.0]+[0.0]*2047)
        self.assertEqual(rows[0]['record_id'],'chunk-1')
        def forbidden(*a,**kw):raise AssertionError('Search must not inspect or reconfigure the index')
        client.list_indexes=forbidden;client.describe_index=forbidden
        with patch('pymilvus.MilvusClient',return_value=client):
            self.assertEqual(len(await self.query._milvus('rag_'+'a'*32+'_1_mv',[1.0]+[0.0]*2047)),1)

    async def test_native_chat_disables_hidden_thinking_for_bounded_controller_calls(self):
        from langchain_core.messages import HumanMessage, SystemMessage
        messages=[SystemMessage(content='Controller policy'),HumanMessage(content='Complete question '*1000)]
        for budget in (2048,768):
            with self.subTest(budget=budget):
                self.query._post=AsyncMock(return_value={'choices':[{'message':{'content':'Final fixture answer'}}]})
                answer=await RAGQuery._chat(self.query,messages,max_tokens=budget)
                self.assertEqual(answer,'Final fixture answer')
                payload=self.query._post.await_args.args[1]
                self.assertEqual(payload.get('chat_template_kwargs'),{'enable_thinking':False})
                self.assertEqual(payload['max_tokens'],budget)
                self.assertEqual(payload['messages'][1]['content'],'Complete question '*1000)
                self.assertEqual(self.query._post.await_count,1)

    async def test_native_chat_rejects_length_stop_even_with_nonempty_partial_content(self):
        from langchain_core.messages import HumanMessage
        self.query._post=AsyncMock(return_value={'choices':[{'finish_reason':'length',
            'message':{'content':'Incomplete answer fragment','reasoning_content':'Private reasoning fixture'}}]})
        with self.assertRaisesRegex(Conflict,'output limit'):
            await RAGQuery._chat(self.query,[HumanMessage(content='Synthetic question')])
        self.assertEqual(self.query._post.await_count,1)

    async def test_native_chat_blank_reasoning_only_response_is_never_answer_text(self):
        from langchain_core.messages import HumanMessage
        for content in (None,'',' \n'):
            with self.subTest(content=content):
                self.query._post=AsyncMock(return_value={'choices':[{'finish_reason':'stop',
                    'message':{'content':content,'reasoning_content':'Private reasoning fixture'}}]})
                with self.assertRaisesRegex(Conflict,'no final text'):
                    await RAGQuery._chat(self.query,[HumanMessage(content='Synthetic question')])
                self.assertEqual(self.query._post.await_count,1)

    async def test_native_chat_returns_only_final_content_with_normal_stop(self):
        from langchain_core.messages import HumanMessage
        self.query._post=AsyncMock(return_value={'choices':[{'finish_reason':'stop',
            'message':{'content':'Final answer','reasoning_content':'Private reasoning fixture'}}]})
        answer=await RAGQuery._chat(self.query,[HumanMessage(content='Synthetic question')])
        self.assertEqual(answer,'Final answer')
        self.assertNotIn('Private reasoning',answer)
