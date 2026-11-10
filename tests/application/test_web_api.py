"""Offline HTTP contracts for bounded website intake."""
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
import httpx2
from application.api import create_app
from application.web_acquisition import CrawlCapture


class WebAPITests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.context = SimpleNamespace(pool=SimpleNamespace(fetchrow=AsyncMock(return_value={'deleted': False})))
        self.submit = AsyncMock(return_value='durable-job')
        self.capture = CrawlCapture((SimpleNamespace(final_uri='https://docs.example/'),), (), True)
        self.collect = AsyncMock(return_value=self.capture)
        self.patch = patch('application.web_acquisition.collect', self.collect)
        self.patch.start(); self.addCleanup(self.patch.stop)
        self.app = create_app(context=self.context, web_submitter=self.submit)
        self.client = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=self.app), base_url='http://fixture')
        self.addAsyncCleanup(self.client.aclose)
        self.body = {'seed_urls': ['https://docs.example/'], 'allowed_hosts': ['docs.example'], 'max_depth': 1, 'max_pages': 3, 'include_images': True, 'included_extensions': ['.pdf'], 'ignored_extensions': []}

    async def test_complete_capture_precedes_durable_acceptance(self):
        async def submit(ctx, collection, capture, policy):
            self.assertEqual(self.collect.await_count, 1)
            self.assertIs(capture, self.capture)
            self.assertEqual(policy.allowed_hosts, ('docs.example',))
            return 'durable-job'
        self.submit.side_effect = submit
        response = await self.client.post('/api/collections/c/web-ingestions', json=self.body)
        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json()['job_id'], 'durable-job')
        self.submit.assert_awaited_once()

    async def test_partial_empty_and_failed_capture_never_submit(self):
        for capture in (CrawlCapture((), (), True), CrawlCapture(self.capture.pages, ({'error': 'failed'},), False)):
            self.collect.return_value = capture
            response = await self.client.post('/api/collections/c/web-ingestions', json=self.body)
            self.assertEqual(response.status_code, 422)
        self.collect.side_effect = ValueError('failed acquisition')
        response = await self.client.post('/api/collections/c/web-ingestions', json=self.body)
        self.assertEqual(response.status_code, 422)
        self.submit.assert_not_awaited()

    async def test_strict_bounds_and_discovery_options(self):
        invalid = [('seed_urls', []), ('seed_urls', ['https://docs.example/'] * 6), ('allowed_hosts', []), ('allowed_hosts', ['docs.example'] * 11), ('max_depth', -1), ('max_depth', '1'), ('max_depth', True), ('max_pages', -1), ('max_pages', 0), ('include_images', 'true'), ('included_extensions', ['.exe']), ('ignored_extensions', ['.zip']), ('captions_enabled', 'true')]
        for key, value in invalid:
            with self.subTest(key=key, value=value):
                response = await self.client.post('/api/collections/c/web-ingestions', json={**self.body, key: value})
                self.assertEqual(response.status_code, 422)
        self.collect.assert_not_awaited(); self.submit.assert_not_awaited()

    async def test_scope_escape_and_private_seed_rejected_before_capture(self):
        for seed in ('https://sub.docs.example/', 'https://docs.example.evil/', 'http://127.0.0.1/', 'https://name:pass@docs.example/'):
            response = await self.client.post('/api/collections/c/web-ingestions', json={**self.body, 'seed_urls': [seed]})
            self.assertEqual(response.status_code, 422)
        self.collect.assert_not_awaited(); self.submit.assert_not_awaited()

    async def test_missing_collection_stops_capture_and_durable_failure_never_accepts(self):
        self.context.pool.fetchrow.return_value = None
        response = await self.client.post('/api/collections/c/web-ingestions', json=self.body)
        self.assertEqual(response.status_code, 404)
        self.collect.assert_not_awaited()
        self.context.pool.fetchrow.return_value = {'deleted': False}
        self.submit.side_effect = ValueError('admission failed')
        response = await self.client.post('/api/collections/c/web-ingestions', json=self.body)
        self.assertEqual(response.status_code, 422)
