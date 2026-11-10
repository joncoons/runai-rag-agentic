"""HTTP asset reads follow the published web graph, never intake cache paths."""
import hashlib
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx2
from application.api import create_app
from collection_control.repository import Conflict


class WebAssetAPITests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.raw = b'generated retained asset bytes'
        self.file = self.root / 'fixture.txt'
        self.file.write_bytes(self.raw)
        self.operation = uuid4()
        self.col = {'collection_id': 'c', 'deleted': False, 'current_operation': self.operation}
        self.pool = SimpleNamespace(fetchrow=AsyncMock(return_value=self.col), fetch=AsyncMock(return_value=[]), fetchval=AsyncMock(return_value=3))
        self.ctx = SimpleNamespace(pool=self.pool, settings=SimpleNamespace(raw=self.root))
        self.graph = {'pages': [{'assets': [{'document_id': 'asset-1', 'canonical_uri': 'https://docs.example/a%20file.txt', 'kind': 'binary', 'raw_uri': self.file.as_uri(), 'raw_sha256': 'sha256:' + hashlib.sha256(self.raw).hexdigest(), 'media_type': 'text/plain'}]}]}
        self.resolve = AsyncMock(return_value=self.graph)
        self.patch = patch('collection_control.web_groups.resolve_graph', self.resolve)
        self.patch.start(); self.addCleanup(self.patch.stop)
        self.client = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=create_app(context=self.ctx)), base_url='http://fixture')
        self.addAsyncCleanup(self.client.aclose)

    async def test_published_asset_serves_verified_bytes_and_safe_attachment(self):
        response = await self.client.get('/api/collections/c/assets/asset-1', params={'operation_id': str(self.operation)})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, self.raw)
        self.assertTrue(response.headers['content-type'].startswith('text/plain'))
        self.assertEqual(response.headers['cache-control'], 'no-store')
        self.assertEqual(response.headers['x-content-type-options'], 'nosniff')
        self.assertIn("filename*=UTF-8''a%20file.txt", response.headers['content-disposition'])
        self.resolve.assert_awaited_once_with(self.pool, self.col)

    async def test_old_operation_missing_and_deleted_scope_cannot_serve(self):
        response = await self.client.get('/api/collections/c/assets/asset-1', params={'operation_id': str(uuid4())})
        self.assertEqual(response.status_code, 409)
        self.resolve.assert_not_awaited()
        response = await self.client.get('/api/collections/c/assets/unknown')
        self.assertEqual(response.status_code, 404)
        self.col['deleted'] = True
        response = await self.client.get('/api/collections/c/assets/asset-1')
        self.assertEqual(response.status_code, 404)
        self.pool.fetchrow.return_value = None
        self.assertEqual((await self.client.get('/api/collections/c/assets/asset-1')).status_code, 404)

    async def test_unpublished_corrupt_and_outside_root_fail_without_raw_paths(self):
        self.resolve.side_effect = Conflict('unpublished private /raw/root')
        response = await self.client.get('/api/collections/c/assets/asset-1')
        self.assertEqual(response.status_code, 409)
        self.assertNotIn('/raw/root', response.text)
        self.resolve.side_effect = None
        self.file.write_bytes(b'changed')
        response = await self.client.get('/api/collections/c/assets/asset-1')
        self.assertEqual(response.status_code, 409)
        self.assertNotIn(self.temporary.name, response.text)
        self.graph['pages'][0]['assets'][0]['raw_uri'] = Path('/etc/passwd').as_uri()
        response = await self.client.get('/api/collections/c/assets/asset-1')
        self.assertEqual(response.status_code, 409)
        self.assertNotIn('/etc/passwd', response.text)

    async def test_job_separates_native_images_from_searchable_documents_and_captions(self):
        self.pool.fetchrow.return_value = {'deleted': False, 'status': 'running', 'stage': 'extract', 'collection_id': 'c', 'errors': []}
        self.pool.fetch.side_effect = [[
            {'filename':'hash.png','status':'captured','records':0,'document_id':'i','operation_id':None,'source_kind':'image','web_role':'image','canonical_uri':'https://docs.example/chart.png'},
            {'filename':'hash.pdf','status':'publishing','records':3,'document_id':'b','operation_id':None,'source_kind':'pdf','web_role':'binary','canonical_uri':'https://docs.example/guide.pdf'},
            {'filename':'hash.html','status':'running','records':0,'document_id':'p','operation_id':None,'source_kind':'html','web_role':'page','canonical_uri':'https://docs.example/page'}], []]
        response = await self.client.get('/api/jobs/' + str(uuid4()))
        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertEqual([d['name'] for d in result['documents']], ['https://docs.example/guide.pdf', 'https://docs.example/page'])
        self.assertEqual(result['assets'], [{'id':'i','url':'https://docs.example/chart.png','kind':'image','status':'processed','records':0}, {'id':'b','url':'https://docs.example/guide.pdf','kind':'binary','status':'publishing','records':3}])
        self.assertEqual(result['captions']['images'], [])
        self.assertIn('web_role', self.pool.fetch.call_args_list[0].args[0])

    async def test_generation_or_revocation_change_during_read_rejects_release(self):
        for changed in ({**self.col, 'current_operation': uuid4()}, {**self.col, 'deleted': True}):
            with self.subTest(changed=changed):
                self.pool.fetchrow.side_effect = [self.col, changed]
                response = await self.client.get('/api/collections/c/assets/asset-1')
                self.assertIn(response.status_code, (404, 409))
                self.assertNotEqual(response.content, self.raw)

    async def test_web_acceptance_reports_native_assets_and_rejects_unavailable_formats(self):
        from application.web_acquisition import CrawlCapture
        capture = CrawlCapture((SimpleNamespace(final_uri='https://docs.example/'),), (), True)
        body = {'seed_urls':['https://docs.example/'], 'allowed_hosts':['docs.example'], 'include_images':True}
        submit = AsyncMock(return_value='durable-job')
        app = create_app(context=self.ctx, web_submitter=submit)
        async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), base_url='http://fixture') as client:
            with patch('application.web_acquisition.collect', AsyncMock(return_value=capture)):
                response = await client.post('/api/collections/c/web-ingestions', json=body)
                self.assertEqual(response.status_code, 202)
                self.assertEqual(response.json()['asset_status'], 'native_ingestion')
                self.assertFalse(response.json()['scheduled_rechecks'])
                for change in ({'included_extensions':['.rst']}, {'captions_enabled':'true'}, {'include_images':'true'}):
                    response = await client.post('/api/collections/c/web-ingestions', json={**body, **change})
                    self.assertEqual(response.status_code, 422)
        submit.assert_awaited_once()

    async def test_private_fixture_seed_is_denied_by_default_before_capture(self):
        import os
        collect = AsyncMock()
        submit = AsyncMock()
        body = {'seed_urls': ['http://127.0.0.1:8089/'], 'allowed_hosts': ['127.0.0.1']}
        app = create_app(context=self.ctx, web_submitter=submit)
        with patch.dict(os.environ, {'WEB_ASSET_PRIVATE_HOSTS': '', 'WEB_ASSET_PRIVATE_PORTS': ''}):
            with patch('application.web_acquisition.collect', collect):
                async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), base_url='http://fixture') as client:
                    response = await client.post('/api/collections/c/web-ingestions', json=body)
        self.assertEqual(response.status_code, 422)
        collect.assert_not_awaited()
        submit.assert_not_awaited()

    async def test_operator_private_scope_preserves_port_and_cannot_be_widened_by_body(self):
        import os
        from application.web_acquisition import CrawlCapture, validate_url
        seed = 'http://127.0.0.1:8089/'
        body = {'seed_urls': [seed], 'allowed_hosts': ['127.0.0.1']}
        capture = CrawlCapture((SimpleNamespace(final_uri=seed),), (), True)
        collect = AsyncMock(return_value=capture)
        submit = AsyncMock(return_value='durable-private-fixture-job')
        app = create_app(context=self.ctx, web_submitter=submit)
        # The configured second host is deliberately absent from the request:
        # capture policy must contain the intersection, not the operator's union.
        operator = {'WEB_ASSET_PRIVATE_HOSTS': '127.0.0.1,10.9.8.7', 'WEB_ASSET_PRIVATE_PORTS': '8089'}
        with patch.dict(os.environ, operator):
            with patch('application.web_acquisition.collect', collect):
                async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), base_url='http://fixture') as client:
                    response = await client.post('/api/collections/c/web-ingestions', json=body)
                    self.assertEqual(response.status_code, 202)
                    self.assertEqual(response.json()['job_id'], 'durable-private-fixture-job')
                    seeds, policy = collect.await_args.args
                    self.assertEqual(seeds, [seed])
                    self.assertEqual(policy.allowed_hosts, ('127.0.0.1',))
                    self.assertEqual(policy.private_hosts, ('127.0.0.1',))
                    self.assertEqual(policy.private_ports, (8089,))
                    self.assertEqual(validate_url(seed, policy), seed)
                    self.assertIs(submit.await_args.args[3], policy)
                    for changed in (
                        {**body, 'private_hosts': ['127.0.0.1', '127.0.0.2']},
                        {**body, 'private_ports': [8089, 8090]},
                        {'seed_urls': ['http://127.0.0.2:8089/'], 'allowed_hosts': ['127.0.0.1', '127.0.0.2']},
                        {**body, 'seed_urls': ['http://127.0.0.1:8090/']},
                    ):
                        with self.subTest(body=changed):
                            response = await client.post('/api/collections/c/web-ingestions', json=changed)
                            self.assertEqual(response.status_code, 422)
        collect.assert_awaited_once()
        submit.assert_awaited_once()
