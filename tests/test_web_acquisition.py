"""Offline acquisition contracts; no site or corpus traffic."""
import hashlib
import unittest
from application import web_acquisition as web


class Content:
    def __init__(self, chunks): self.chunks = chunks
    async def iter_chunked(self, size):
        for chunk in self.chunks: yield chunk


class Response:
    def __init__(self, raw=b'', status=200, headers=None, chunks=None):
        self.status = status
        self.headers = headers or {'Content-Type': 'text/html; charset=utf-8'}
        self.content = Content(chunks if chunks is not None else [raw])
    async def __aenter__(self): return self
    async def __aexit__(self, *args): pass


class Session:
    def __init__(self, routes): self.routes, self.calls = routes, []
    def get(self, url, **kwargs):
        assert kwargs['allow_redirects'] is False
        self.calls.append(url)
        value = self.routes[url]
        if isinstance(value, Exception): raise value
        return value


class AcquisitionTests(unittest.IsolatedAsyncioTestCase):
    def policy(self, **kw): return web.AcquisitionPolicy(allowed_hosts=('docs.example',), **kw)

    async def test_whole_page_retains_bytes_and_distinct_parented_assets(self):
        raw = b'<html><title>Guide</title><nav>menu</nav><main><h1>Guide</h1><p>All text here.</p><a href="next#part">Next</a><a href="manual.pdf">PDF</a><img src="chart.png" alt="Sales"><picture><source srcset="wide.webp 2x, narrow.webp 1x"></picture></main></html>'
        session = Session({'https://docs.example/': Response(raw)})
        page = await web.acquire_page('https://docs.example/', self.policy(), session=session)
        self.assertEqual(page.raw_bytes, raw)
        self.assertEqual(page.raw_sha256, 'sha256:' + hashlib.sha256(raw).hexdigest())
        self.assertEqual(page.title, 'Guide')
        self.assertIn('All text here.', page.text)
        self.assertNotIn('menu', page.text)
        self.assertEqual(page.links, ('https://docs.example/next',))
        self.assertEqual({a.url for a in page.assets}, {'https://docs.example/manual.pdf', 'https://docs.example/chart.png', 'https://docs.example/wide.webp', 'https://docs.example/narrow.webp'})
        self.assertTrue(all(a.parent_url == page.final_uri for a in page.assets))
        self.assertEqual(next(a.kind for a in page.assets if a.url.endswith('.pdf')), 'binary')

    async def test_redirect_cannot_escape_exact_hosts(self):
        session = Session({'https://docs.example/': Response(status=302, headers={'Location': 'https://docs.example.evil/a'})})
        with self.assertRaises(web.AcquisitionError):
            await web.acquire_page('https://docs.example/', self.policy(), session=session)
        self.assertEqual(len(session.calls), 1)

    async def test_private_literal_and_credentials_rejected_without_request(self):
        session = Session({})
        for url, host in [('http://127.0.0.1/', '127.0.0.1'), ('https://user:pass@docs.example/', 'docs.example')]:
            with self.assertRaises(web.AcquisitionError):
                await web.acquire_page(url, web.AcquisitionPolicy(allowed_hosts=(host,)), session=session)
        self.assertFalse(session.calls)

    async def test_streamed_limit_enforced_without_content_length(self):
        session = Session({'https://docs.example/': Response(chunks=[b'abcd', b'efgh'])})
        with self.assertRaises(web.AcquisitionError):
            await web.acquire_page('https://docs.example/', self.policy(max_response_bytes=6), session=session)

    async def test_asset_filters_and_unapproved_subdomains(self):
        raw = b'<main>Body<a href="a.pdf">A</a><a href="b.docx">B</a><img src="https://sub.docs.example/x.png"><img src="x.png"><a href="https://elsewhere.example/">out</a></main>'
        page = await web.acquire_page('https://docs.example/', self.policy(included_extensions=('.pdf',), include_images=False), session=Session({'https://docs.example/': Response(raw)}))
        self.assertEqual([a.url for a in page.assets], ['https://docs.example/a.pdf'])
        self.assertFalse(page.links)

    async def test_bfs_depth_and_partial_failure_never_assert_deletion(self):
        routes = {'https://docs.example/': Response(b'<main>Root<a href="b">B</a><a href="a">A</a></main>'), 'https://docs.example/a': Response(b'<main>A<a href="c">C</a></main>'), 'https://docs.example/b': Response(status=503)}
        session = Session(routes)
        result = await web.collect(['https://docs.example/'], self.policy(max_depth=1), session=session)
        self.assertEqual(session.calls, ['https://docs.example/', 'https://docs.example/a', 'https://docs.example/b'])
        self.assertFalse(result.complete)
        self.assertFalse(result.missing_means_deleted)
        self.assertTrue(result.errors)

    async def test_complete_bounded_scan_and_page_cap(self):
        routes = {'https://docs.example/': Response(b'<main>Root<a href="a">A</a></main>'), 'https://docs.example/a': Response(b'<main>A</main>')}
        complete = await web.collect(['https://docs.example/'], self.policy(max_depth=1), session=Session(routes))
        self.assertTrue(complete.complete)
        capped = await web.collect(['https://docs.example/'], self.policy(max_pages=1), session=Session(routes))
        self.assertFalse(capped.complete)

    async def test_redirect_limit_and_non_html_fail_closed(self):
        routes = {'https://docs.example/': Response(status=302, headers={'Location': '/a'}), 'https://docs.example/a': Response(status=302, headers={'Location': '/b'})}
        with self.assertRaises(web.AcquisitionError):
            await web.acquire_page('https://docs.example/', self.policy(max_redirects=1), session=Session(routes))
        with self.assertRaises(web.AcquisitionError):
            await web.acquire_page('https://docs.example/', self.policy(), session=Session({'https://docs.example/': Response(b'%PDF', headers={'Content-Type': 'application/pdf'})}))

    async def test_public_resolver_rejects_private_dns_answer(self):
        from unittest.mock import AsyncMock, patch
        with patch('aiohttp.resolver.DefaultResolver.resolve', new=AsyncMock(return_value=[{'host': '10.1.2.3'}])):
            resolver = web.PublicResolver()
            try:
                with self.assertRaises(web.AcquisitionError): await resolver.resolve('docs.example')
            finally: await resolver.close()

    async def test_redirect_alias_does_not_consume_an_extra_page_slot(self):
        routes = {'https://docs.example/': Response(status=301, headers={'Location': '/home'}), 'https://docs.example/home': Response(b'<main>Home<a href="next">Next</a></main>'), 'https://docs.example/next': Response(b'<main>Next</main>')}
        result = await web.collect(['https://docs.example/'], self.policy(max_pages=2), session=Session(routes))
        self.assertTrue(result.complete)
        self.assertEqual(len(result.pages), 2)

    async def test_overall_timeout_rejects_slow_stream(self):
        import asyncio
        class SlowContent:
            async def iter_chunked(self, size):
                await asyncio.sleep(0.1)
                yield b'<main>late</main>'
        response = Response()
        response.content = SlowContent()
        with self.assertRaises(web.AcquisitionError):
            await web.acquire_page('https://docs.example/', self.policy(timeout_seconds=0.001), session=Session({'https://docs.example/': response}))

    async def test_existing_crawler_table_normalizer_preserves_cells(self):
        raw = b'<main><h1>Prices</h1><table><tr><th>Name</th><th>Value</th></tr><tr><td>A|B</td><td>2</td></tr></table><p>After table</p></main>'
        page = await web.acquire_page('https://docs.example/', self.policy(), session=Session({'https://docs.example/': Response(raw)}))
        self.assertIn('| Name | Value |', page.text)
        self.assertIn('| A\\|B | 2 |', page.text)
        self.assertIn('After table', page.text)
