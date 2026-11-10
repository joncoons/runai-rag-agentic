"""Offline bounded asset transport tests; no models, storage or network."""
import asyncio
import hashlib
import io
import unittest
import zipfile
from types import SimpleNamespace
from application import web_asset_acquisition as assets
from application.web_acquisition import AcquisitionPolicy, Asset


class Content:
    def __init__(self, chunks): self.chunks = chunks
    async def iter_chunked(self, size):
        for chunk in self.chunks: yield chunk


class Response:
    def __init__(self, raw=b'', media='application/pdf', status=200, headers=None, chunks=None):
        self.status = status
        self.headers = {'Content-Type': media, **(headers or {})}
        self.content = Content([raw] if chunks is None else chunks)
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


class AssetTests(unittest.IsolatedAsyncioTestCase):
    def policy(self, **kw): return AcquisitionPolicy(('docs.example',), **kw)
    def asset(self, path='/manual.pdf', kind='binary', parent='https://docs.example/page', locator='a[0]'):
        return Asset('https://docs.example' + path, kind, parent, 'Author alt', locator)
    async def fetch(self, response, path='/manual.pdf', kind='binary', **kw):
        asset = self.asset(path, kind)
        return await assets.acquire_asset(asset, self.policy(**kw), session=Session({asset.url: response}))

    async def test_retains_raw_bytes_digest_and_parent_context(self):
        raw = b'%PDF-1.7\nbody\n%%EOF'
        result = await self.fetch(Response(raw))
        self.assertEqual(result.raw_bytes, raw)
        self.assertEqual(result.raw_sha256, 'sha256:' + hashlib.sha256(raw).hexdigest())
        self.assertEqual(result.filename, 'manual.pdf')
        self.assertEqual((result.parent_url, result.kind, result.locator, result.alt), ('https://docs.example/page', 'binary', 'a[0]', 'Author alt'))
        self.assertEqual(result.evidence['byte_size'], len(raw))
        self.assertFalse(result.evidence['missing_means_deleted'])

    async def test_same_url_recheck_hashes_new_body(self):
        first = await self.fetch(Response(b'%PDF-1.7\none\n%%EOF'))
        second = await self.fetch(Response(b'%PDF-1.7\ntwo\n%%EOF'))
        self.assertNotEqual(first.raw_sha256, second.raw_sha256)

    async def test_rejects_wrong_mime_signature_and_unsupported_extensions(self):
        for response, path, kind in [(Response(b'%PDF-1.7', 'text/html'), '/manual.pdf', 'binary'), (Response(b'<html>login</html>'), '/manual.pdf', 'binary'), (Response(b'GIF89a', 'image/gif'), '/a.gif', 'image'), (Response(b'<svg xmlns="http://www.w3.org/2000/svg"><script>unsafe</script></svg>', 'image/svg+xml'), '/a.svg', 'image'), (Response(b'text', 'text/plain'), '/a.rst', 'binary'), (Response(b'%PDF-1.7'), '/a.png', 'image')]:
            with self.subTest(path=path, media=response.headers['Content-Type']):
                with self.assertRaises(assets.AssetAcquisitionError): await self.fetch(response, path, kind)

    async def test_extensionless_uses_validated_mime_and_query_not_filename(self):
        result = await self.fetch(Response(b'%PDF-1.7\n%%EOF'), '/download?id=a.txt')
        self.assertEqual(result.filename, 'download.pdf')

    async def test_text_and_ooxml_validation(self):
        result = await self.fetch(Response(b'# Heading\nbody', 'text/markdown; charset=utf-8'), '/guide.md')
        self.assertEqual(result.filename, 'guide.md')
        for raw in (b'\x00binary', b'\xff\xfe', b'<html>login</html>'):
            with self.assertRaises(assets.AssetAcquisitionError): await self.fetch(Response(raw, 'text/plain'), '/a.txt')
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w') as archive:
            archive.writestr('[Content_Types].xml', '<Types/>')
            archive.writestr('word/document.xml', '<document/>')
        media = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        self.assertEqual((await self.fetch(Response(buffer.getvalue(), media), '/a.docx')).filename, 'a.docx')
        with self.assertRaises(assets.AssetAcquisitionError): await self.fetch(Response(b'PK\x03\x04bad', media), '/a.docx')

    async def test_supported_image_signatures(self):
        from PIL import Image
        for extension, media, format in [('.png', 'image/png', 'PNG'), ('.jpg', 'image/jpeg', 'JPEG'),
                ('.webp', 'image/webp', 'WEBP'), ('.tif', 'image/tiff', 'TIFF'), ('.bmp', 'image/bmp', 'BMP')]:
            stream = io.BytesIO()
            Image.new('RGB', (8, 6), 'blue').save(stream, format=format)
            raw = stream.getvalue()
            path = '/a' + extension
            with self.subTest(path=path):
                result = await self.fetch(Response(raw, media), path, 'image')
                self.assertEqual(result.raw_bytes, raw)
                self.assertEqual(result.evidence['inspected_image']['detected_format'], format)

    async def test_scope_redirect_chain_downgrade_and_limit(self):
        source = self.asset().url
        for location in ('https://docs.example.evil/a.pdf', 'http://docs.example/a.pdf', 'http://127.0.0.1/a.pdf'):
            session = Session({source: Response(status=302, headers={'Location': location})})
            with self.assertRaises(assets.AssetAcquisitionError): await assets.acquire_asset(self.asset(), self.policy(), session=session)
            self.assertEqual(session.calls, [source])
        routes = {source: Response(status=302, headers={'Location': '/b.pdf'}), 'https://docs.example/b.pdf': Response(status=307, headers={'Location': '/c.pdf'}), 'https://docs.example/c.pdf': Response(b'%PDF-1.7')}
        result = await assets.acquire_asset(self.asset(), self.policy(), session=Session(routes))
        self.assertEqual(result.final_uri, 'https://docs.example/c.pdf')
        self.assertEqual(len(result.evidence['redirect_chain']), 3)
        with self.assertRaises(assets.AssetAcquisitionError): await assets.acquire_asset(self.asset(), self.policy(max_redirects=1), session=Session(routes))

    async def test_stream_bound_declared_length_and_truncation_fail(self):
        for response in (Response(chunks=[b'%PDF-', b'123456789']), Response(b'%PDF-', headers={'Content-Length': '99'}), Response(b'%PDF-', headers={'Content-Length': '8'}), Response(b'%PDF-', headers={'Content-Length': '-1'}), Response(b'%PDF-', headers={'Content-Length': 'bad'})):
            with self.subTest(headers=response.headers):
                with self.assertRaises(assets.AssetAcquisitionError): await self.fetch(response, max_response_bytes=10)

    async def test_absence_is_observation_never_deletion(self):
        for status in (404, 410, 503, 304):
            with self.assertRaises(assets.AssetAcquisitionError) as caught: await self.fetch(Response(status=status))
            self.assertEqual(caught.exception.observation, 'absent' if status in (404, 410) else 'unavailable')
            self.assertEqual(caught.exception.http_status, status)
            self.assertFalse(caught.exception.missing_means_deleted)

    async def test_timeout(self):
        class Slow:
            async def iter_chunked(self, size):
                await asyncio.sleep(.1)
                yield b'%PDF-1.7'
        response = Response()
        response.content = Slow()
        with self.assertRaises(assets.AssetAcquisitionError): await self.fetch(response, timeout_seconds=.001)

    async def test_collection_deduplicates_fetch_preserving_parent_relationships_and_bounds(self):
        one = self.asset()
        two = self.asset(parent='https://docs.example/other', locator='a[9]')
        capture = SimpleNamespace(pages=(SimpleNamespace(assets=(one,)), SimpleNamespace(assets=(two,))))
        session = Session({one.url: Response(b'%PDF-1.7')})
        result = await assets.acquire_assets(capture, self.policy(), session=session, max_assets=1, max_aggregate_bytes=8)
        self.assertEqual(len(result), 2)
        self.assertEqual([r.parent_url for r in result], [one.parent_url, two.parent_url])
        self.assertEqual(session.calls, [one.url])
        with self.assertRaises(assets.AssetAcquisitionError): await assets.acquire_assets(capture, self.policy(), session=session, max_aggregate_bytes=7)
        third = self.asset('/other.pdf')
        capture = SimpleNamespace(pages=(SimpleNamespace(assets=(one, third)),))
        with self.assertRaises(assets.AssetAcquisitionError): await assets.acquire_assets(capture, self.policy(), session=session, max_assets=1)
