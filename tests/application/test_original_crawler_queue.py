"""Offline behavioral qualification of the actual local crawler queue bridge."""
import asyncio
import hashlib
import inspect
import tempfile
import unittest
from contextlib import ExitStack
from unittest.mock import patch

from application.web_acquisition import PageCapture
from ingestion.queued_crawler_runtime import crawl as runtime


class OriginalCrawlerQueueTests(unittest.IsolatedAsyncioTestCase):
    def require_hooks(self):
        parameters = inspect.signature(runtime.SimpleWebCrawler).parameters
        self.assertIn('capture_fetch', parameters, 'Original crawler needs an explicit acquisition hook')
        self.assertIn('capture_sink', parameters, 'Original crawler needs an explicit durable admission hook')

    def page(self, url, links=()):
        # Bytes intentionally cannot round-trip through UTF-8 decoding.
        raw = b'<html><main><p>Original \xe9 bytes</p></main></html>'
        return PageCapture(url, url, raw, 'sha256:' + hashlib.sha256(raw).hexdigest(),
                           'Original page text', 'Page', tuple(links), (), {})

    async def execute(self, graph, *, budget=None, depth=3, fail_sink=False, seed=None):
        self.require_hooks()
        fetched, admitted = [], []
        state = {'fetch': 0, 'fetch_peak': 0, 'sink': 0, 'sink_peak': 0}
        async def fetch(url):
            state['fetch'] += 1
            state['fetch_peak'] = max(state['fetch_peak'], state['fetch'])
            fetched.append(url)
            try:
                await asyncio.sleep(0.001)
                return graph[url]
            finally: state['fetch'] -= 1
        async def sink(pages):
            state['sink'] += 1
            state['sink_peak'] = max(state['sink_peak'], state['sink'])
            try:
                await asyncio.sleep(0.02)
                if fail_sink: raise RuntimeError('durable admission unavailable')
                admitted.extend(pages)
                return len(pages)
            finally: state['sink'] -= 1
        def forbidden(*args, **kwargs):
            raise AssertionError('Legacy extraction/model/VDB path must never run')
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            for module, name in ((runtime.es_client, 'ensure_index'), (runtime.es_client, 'bulk_write'),
                                 (runtime.es_client, 'delete_by_source_uris'), (runtime.embed_client, 'embed_texts'),
                                 (runtime.config, 'build_es_ssl_ctx'), (runtime, 'html_to_elements'),
                                 (runtime, 'split_by_semantic_elements')):
                stack.enter_context(patch.object(module, name, forbidden))
            crawler = runtime.SimpleWebCrawler(seed or 'https://fixture.invalid/section/', max_pages=budget,
                max_depth=depth, allowed_url_prefixes=['https://fixture.invalid/section/'],
                capture_fetch=fetch, capture_sink=sink, batch_size=1, max_concurrent_batches=2,
                registry_dir=directory, export_dir=directory, collection_name='fixed',
                # Even unsafe legacy defaults must be disabled by capture mode.
                use_selenium=True, extract_linked_files=True, use_product_url_map=True)
            stack.enter_context(patch.object(crawler, '_collect_binary_file', forbidden))
            stack.enter_context(patch.object(crawler, '_create_selenium_driver', forbidden))
            result = await crawler.crawl('fixed')
            self.assertFalse(crawler.use_selenium)
            self.assertFalse(crawler.extract_linked_files)
            self.assertFalse(crawler.use_product_url_map)
            registry = crawler._load_registry()
            self.assertTrue(all('last_ingested' not in entry for entry in registry.values()))
        return result, fetched, admitted, state

    def tree(self):
        root = 'https://fixture.invalid/section/'
        level1 = [root + str(i) for i in range(30)]
        level2 = [root + 'child/' + str(i) for i in range(30)]
        graph = {root: self.page(root, level1 + ['https://fixture.invalid/outside/'])}
        for first, second in zip(level1, level2):
            graph[first] = self.page(first, [second, level1[0]])
            graph[second] = self.page(second, [root + 'deep/' + second.rsplit('/', 1)[1]])
            third = graph[second].links[0]
            graph[third] = self.page(third, [third + '/too-deep'])
        return graph

    async def test_original_queue_exceeds_25_preserves_raw_pages_and_backpressure(self):
        graph = self.tree()
        result, fetched, admitted, state = await self.execute(graph)
        self.assertEqual(len(fetched), 91)
        self.assertEqual(len(set(fetched)), 91)
        self.assertEqual(len(admitted), 91)
        self.assertGreater(state['fetch_peak'], 1)
        self.assertLessEqual(state['fetch_peak'], runtime.SimpleWebCrawler._FETCH_CONCURRENCY)
        self.assertEqual(state['sink_peak'], 2)
        self.assertTrue(all(page is graph[page.requested_uri] for page in admitted))
        self.assertEqual(result['errors'], [])
        self.assertTrue(result['queue_exhausted'])
        self.assertEqual(result['remaining_count'], 0)
        self.assertEqual(result['pages_admitted'], 91)
        # Original BFS never fetches a deeper layer ahead of shallower URLs.
        self.assertTrue(all('/child/' not in u for u in fetched[:31]))

    async def test_finite_budget_reports_unvisited_frontier(self):
        result, fetched, admitted, _ = await self.execute(self.tree(), budget=11)
        self.assertLessEqual(len(fetched), 11)
        self.assertEqual(len(fetched), len(admitted))
        self.assertFalse(result['queue_exhausted'])
        self.assertGreater(result['remaining_count'], 0)

    async def test_sink_failure_is_reported_and_never_marks_ingested(self):
        root = 'https://fixture.invalid/section/'
        result, _, admitted, _ = await self.execute({root: self.page(root)}, fail_sink=True)
        self.assertEqual(admitted, [])
        self.assertTrue(result['errors'])
        self.assertIn('durable admission unavailable', str(result['errors']))
        self.assertEqual(result['pages_admitted'], 0)

    async def test_depth_zero_fetches_only_seed(self):
        result, fetched, admitted, _ = await self.execute(self.tree(), depth=0)
        self.assertEqual(len(fetched), 1)
        self.assertEqual(len(admitted), 1)
        self.assertTrue(result['queue_exhausted'])

    async def test_capture_preserves_exact_slashless_endpoint(self):
        url = 'https://fixture.invalid/section/page?kind=exact'
        result, fetched, admitted, _ = await self.execute({url: self.page(url)}, seed=url)
        self.assertEqual(fetched, [url])
        self.assertEqual(admitted[0].requested_uri, url)
        self.assertTrue(result['complete'])

    def test_capture_hooks_must_be_paired(self):
        self.require_hooks()
        async def hook(value): return value
        with self.assertRaisesRegex(ValueError, 'paired|both'):
            runtime.SimpleWebCrawler('https://fixture.invalid/', capture_fetch=hook)
        with self.assertRaisesRegex(ValueError, 'paired|both'):
            runtime.SimpleWebCrawler('https://fixture.invalid/', capture_sink=hook)


if __name__ == '__main__': unittest.main()
