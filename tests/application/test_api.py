"""ASGI lifecycle contracts with mocked control/intake, never external services."""
import unittest
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx2
from application.api import create_app


class Connection:
    def __init__(self):
        self.row={'collection_id':'collection-1','name':'Fixture','requested_milvus':False,
            'milvus_enabled':False,'deleted':False,'revision':0,'current_operation':None,
            'contract_sha256':'e'*64}
        self.statements=[]
        self.pending=False
    @asynccontextmanager
    async def transaction(self):yield self
    async def fetchrow(self,sql,*args):
        self.statements.append((sql,args))
        if 'FROM application.jobs' in sql:return None
        if 'application.collections' in sql:return self.row
        if 'collection_control.operations' in sql:return {'artifact_sha256':'a'*64}
        return None
    async def fetchval(self,sql,*args):
        if 'EXISTS' in sql:return self.pending
        return 1
    async def execute(self,sql,*args):self.statements.append((sql,args));return 'UPDATE 1'
    async def fetch(self,sql,*args):return []


class Pool:
    def __init__(self):self.connection=Connection()
    @asynccontextmanager
    async def acquire(self):yield self.connection
    async def fetchrow(self,sql,*args):return await self.connection.fetchrow(sql,*args)
    async def fetchval(self,sql,*args):return await self.connection.fetchval(sql,*args)
    async def execute(self,sql,*args):return await self.connection.execute(sql,*args)
    async def fetch(self,sql,*args):return await self.connection.fetch(sql,*args)


class APITests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.pool=Pool()
        settings=SimpleNamespace(contract={'dimensions':2048,'embedding_checkpoint_sha256':'e'*64})
        self.context=SimpleNamespace(pool=self.pool,settings=settings,control=SimpleNamespace(handle_prefix='rag_agentic'))
        self.control=SimpleNamespace(create_collection=AsyncMock(),delete_collection=AsyncMock(),
            prepare=AsyncMock(return_value={'operation_id':uuid4(),'status':'pending'}))
        self.repo_patch=patch('application.api.ProvenanceRepository',return_value=self.control)
        self.repo_patch.start();self.addCleanup(self.repo_patch.stop)
        self.submit=AsyncMock(return_value=str(uuid4()))
        self.query=SimpleNamespace(search=AsyncMock(return_value={'answer':'Fixture','sources':[],'warnings':[]}))
        self.app=create_app(context=self.context,query_service=self.query,submitter=self.submit)
        self.client=httpx2.AsyncClient(transport=httpx2.ASGITransport(app=self.app),base_url='http://fixture')
        self.addAsyncCleanup(self.client.aclose)

    async def test_gui_assets_are_content_versioned_and_index_never_cached(self):
        import hashlib
        from pathlib import Path
        import application.api as module
        response=await self.client.get('/')
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.headers.get('cache-control'),'no-store')
        for name in ('app.js','app.css'):
            sha=hashlib.sha256((Path(module.__file__).parent/'static'/name).read_bytes()).hexdigest()[:16]
            self.assertIn('/static/'+name+'?v='+sha,response.text)

    async def test_create_sets_initial_milvus_membership_without_vdb_writes(self):
        response=await self.client.post('/api/collections',json={'name':'  Fixture  ','also_write_milvus':True})
        self.assertEqual(response.status_code,201)
        self.assertEqual(response.json()['name'],'Fixture')
        self.assertTrue(response.json()['also_write_milvus'])
        self.control.create_collection.assert_awaited_once()
        self.assertTrue(any('milvus_enabled' in sql for sql,args in self.pool.connection.statements))

    async def test_boolean_optins_are_strict_and_names_must_be_nonempty(self):
        for body in ({'name':'  '},{'name':'Fixture','also_write_milvus':'true'}):
            with self.subTest(body=body):
                response=await self.client.post('/api/collections',json=body)
                self.assertEqual(response.status_code,422)
        self.control.create_collection.assert_not_awaited()

    async def test_upload_enqueues_real_submit_interface_default_caption_off(self):
        response=await self.client.post('/api/collections/collection-1/ingestions',
            files=[('files',('fixture.pdf',b'%PDF-generated-fixture','application/pdf'))])
        self.assertEqual(response.status_code,202)
        self.assertIn('job_id',response.json())
        self.submit.assert_awaited_once_with(self.context,'collection-1',
            [('fixture.pdf',b'%PDF-generated-fixture')],captions_enabled=False)

    async def test_upload_caption_optin_requires_exact_explicit_boolean_string(self):
        response=await self.client.post('/api/collections/collection-1/ingestions',
            files={'files':('fixture.pdf',b'%PDF-fixture','application/pdf')},data={'captions_enabled':'true'})
        self.assertEqual(response.status_code,202)
        self.assertTrue(self.submit.await_args.kwargs['captions_enabled'])
        response=await self.client.post('/api/collections/collection-1/ingestions',
            files={'files':('fixture.pdf',b'%PDF-fixture','application/pdf')},data={'captions_enabled':'yes'})
        self.assertEqual(response.status_code,422)
        self.assertEqual(self.submit.await_count,1)

    async def test_unsupported_traversal_and_oversize_rejected_before_submission(self):
        for name in ('fixture.exe','../fixture.pdf','directory\\fixture.pdf'):
            response=await self.client.post('/api/collections/collection-1/ingestions',files={'files':(name,b'file')})
            self.assertEqual(response.status_code,422)
        with patch('application.api.MAX_FILE_BYTES',8):
            response=await self.client.post('/api/collections/collection-1/ingestions',files={'files':('fixture.pdf',b'123456789')})
        self.assertEqual(response.status_code,413)
        self.submit.assert_not_awaited()

    async def test_each_supported_document_type_reaches_typed_intake(self):
        for suffix in ('docx','pptx','csv','xls','xlsx','md','markdown','txt'):
            with self.subTest(suffix=suffix):
                response=await self.client.post('/api/collections/collection-1/ingestions',files={'files':('fixture.'+suffix,b'fixture')})
                self.assertEqual(response.status_code,202)
        self.assertEqual(self.submit.await_count,8)

    async def test_aggregate_upload_bytes_are_bounded(self):
        with patch('application.api.MAX_TOTAL_BYTES',12):
            response=await self.client.post('/api/collections/collection-1/ingestions',files=[
                ('files',('a.pdf',b'12345678')),('files',('b.pdf',b'12345678'))])
        self.assertEqual(response.status_code,413);self.submit.assert_not_awaited()

    async def test_missing_or_deleted_collection_rejected_before_upload(self):
        for row in (None,{**self.pool.connection.row,'deleted':True}):
            self.pool.connection.row=row
            response=await self.client.post('/api/collections/collection-1/ingestions',files={'files':('fixture.pdf',b'file')})
            self.assertEqual(response.status_code,404)
        self.submit.assert_not_awaited()

    async def test_enroll_existing_publication_uses_control_prepare_only(self):
        self.pool.connection.row.update(current_operation=uuid4(),revision=2)
        response=await self.client.post('/api/collections/collection-1/milvus',json={'enabled':True})
        self.assertEqual(response.status_code,202)
        self.assertEqual(self.control.prepare.await_args.args[2],'enroll_milvus')
        self.assertEqual(self.control.prepare.await_args.args[4],'a'*64)

    async def test_empty_enrollment_is_deferred_and_pending_mutation_conflicts(self):
        response=await self.client.post('/api/collections/collection-1/milvus',json={'enabled':True})
        self.assertEqual(response.status_code,202)
        self.assertEqual(response.json()['status'],'waiting_for_content')
        self.control.prepare.assert_not_awaited()
        self.pool.connection.pending=True
        response=await self.client.post('/api/collections/collection-1/milvus',json={'enabled':True})
        self.assertEqual(response.status_code,409)

    async def test_delete_tombstones_and_cancels_parent_jobs_documents_and_pending_captions(self):
        response=await self.client.delete('/api/collections/collection-1')
        self.assertEqual(response.status_code,202)
        self.assertFalse(response.json()['physical_purge_complete'])
        self.control.delete_collection.assert_awaited_once_with('collection-1')
        sql='\n'.join(s for s,args in self.pool.connection.statements)
        for table in ('public.jobs','public.documents','application.jobs','caption_control.assets'):
            self.assertIn('UPDATE '+table,sql)

    async def test_search_passes_explicit_empty_scope_and_mode_unchanged(self):
        response=await self.client.post('/api/search',json={'query':'Question','collection_ids':[],
            'backend':'elasticsearch','mode':'search'})
        self.assertEqual(response.status_code,200)
        self.query.search.assert_awaited_once_with('Question',[],backend='elasticsearch',mode='search')

    async def test_unknown_job_is_404_and_malformed_job_id_422(self):
        response=await self.client.get('/api/jobs/'+str(uuid4()))
        self.assertEqual(response.status_code,404)
        self.assertEqual((await self.client.get('/api/jobs/not-a-uuid')).status_code,422)

    async def test_static_app_served_and_status_does_not_claim_model_readiness(self):
        self.assertEqual((await self.client.get('/')).status_code,200)
        response=await self.client.get('/api/status')
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.json()['components']['models'],'not_probed')

    async def test_job_progress_keeps_caption_publication_independent_of_source_completion(self):
        job_id=uuid4();operation_id=uuid4()
        job_row={'job_id':job_id,'collection_id':'collection-1','status':'completed',
            'stage':'complete','deleted':False,'errors':'[]'}
        self.pool.fetchrow=AsyncMock(return_value=job_row)
        async def rows(sql,*args):
            if 'filename,status,records' in sql:
                return [{'filename':'fixture.pdf','status':'published','records':2,
                    'document_id':'document-1','operation_id':operation_id}]
            if 'FROM caption_control.assets' in sql:
                return [dict(stage_key='caption-pending',filename='fixture.pdf',document_id='document-1',
                    status='publishing',terminal_reason=None,operation_id=None),
                    dict(stage_key='caption-failed',filename='fixture.pdf',document_id='document-1',
                    status='failed',terminal_reason='PermanentImageError',operation_id=None)]
            if 'FROM collection_control.operations' in sql:
                return [{'operation_id':operation_id,'status':'published','targets':['elasticsearch'],'fence':1}]
            if 'FROM collection_control.receipts' in sql:
                return [{'backend':'elasticsearch','fence':1,'row_count':2,'content_sha256':'a'*64,'visible_at':None}]
            return []
        self.pool.fetch=rows
        response=await self.client.get('/api/jobs/'+str(job_id))
        self.assertEqual(response.status_code,200)
        result=response.json()
        self.assertEqual(result['status'],'completed')
        self.assertEqual(result['captions']['pending'],1)
        self.assertEqual(result['captions']['failed'],1)
        self.assertEqual(len(result['captions']['images']),2)
        self.assertEqual(result['operations'][0]['receipts'][0]['row_count'],2)

    async def test_collection_list_reports_pending_milvus_separately_from_ready_publication(self):
        self.pool.fetch=AsyncMock(return_value=[dict(collection_id='collection-1',name='Fixture',
            requested_milvus=True,milvus_enabled=False,current_operation=uuid4(),revision=1,
            mutation_pending=True,source_pending=0,source_failed=0,documents=1,caption_pending=2,caption_failed=0)])
        response=await self.client.get('/api/collections')
        self.assertEqual(response.status_code,200)
        collection=response.json()['collections'][0]
        self.assertEqual(collection['status'],'ready')
        self.assertEqual(collection['milvus_status'],'pending')
        self.assertEqual(collection['caption_pending'],2)
        self.assertTrue(collection['also_write_milvus'])

    async def test_streaming_request_without_length_is_bounded_before_intake(self):
        async def content():
            yield b'--fixture\r\nContent-Disposition: form-data; name="files"; filename="a.pdf"\r\n\r\n'
            yield b'x'*1000
            yield b'\r\n--fixture--\r\n'
        with patch('application.api.MAX_REQUEST_BYTES',128):
            response=await self.client.post('/api/collections/collection-1/ingestions',
                headers={'Content-Type':'multipart/form-data; boundary=fixture'},content=content())
        self.assertEqual(response.status_code,413)
        self.submit.assert_not_awaited()

    async def test_validation_errors_do_not_echo_raw_request_input(self):
        response=await self.client.post('/api/search',json={'query':'PRIVATE_QUERY_TEXT','mode':'invalid'})
        self.assertEqual(response.status_code,422)
        self.assertNotIn('PRIVATE_QUERY_TEXT',response.text)

    async def test_module_entrypoint_starts_declared_api_listener(self):
        import runpy
        from pathlib import Path
        import application.api as api_module
        with patch('uvicorn.run') as run:
            runpy.run_path(str(Path(api_module.__file__)),run_name='__main__')
        run.assert_called_once_with('application.api:app',host='0.0.0.0',port=8080)

    async def test_deletion_preserves_asset_before_parent_lock_order(self):
        response=await self.client.delete('/api/collections/collection-1')
        self.assertEqual(response.status_code,202)
        sql=[s for s,args in self.pool.connection.statements]
        asset=next(i for i,s in enumerate(sql) if 'UPDATE caption_control.assets' in s)
        parent=next(i for i,s in enumerate(sql) if 'UPDATE public.jobs' in s)
        self.assertLess(asset,parent)


    async def test_pending_generation_excludes_generated_awaiting_publication(self):
        self.pool.connection.fetch=AsyncMock(return_value=[dict(self.pool.connection.row,
            mutation_pending=False,source_pending=0,source_failed=0,documents=1,
            caption_pending=3,caption_failed=0,caption_origin_rows=[
            {'stage_key':'a','asset_sha256':'a'*64,'status':'publishing','inference_status':'committed','origin':'pdf'},
            {'stage_key':'b','asset_sha256':'b'*64,'status':'queued','inference_status':'pending','origin':'pdf'},
            {'stage_key':'c','asset_sha256':'c'*64,'status':'processing','inference_status':'running','origin':'pdf'}])])
        response=await self.client.get('/api/collections')
        self.assertEqual(response.status_code,200)
        progress=response.json()['collections'][0]['processing']
        self.assertEqual(progress['captions_pending'],2)
        self.assertEqual(progress['captions_awaiting_publication'],1)
        self.assertEqual(progress['captions_generated'],1)
