"""Qualified raster bytes, MIME variants and cache revalidation; no network."""
from dataclasses import replace
import hashlib
import io
import struct
import zlib
import unittest

from PIL import Image
from application.web_acquisition import AcquisitionPolicy, Asset
from application import web_asset_acquisition as acquisition


class Content:
    def __init__(self, raw): self.raw = raw
    async def iter_chunked(self, size): yield self.raw


class Response:
    status = 200
    def __init__(self, raw, media):
        self.headers = {'Content-Length': str(len(raw))}
        if media is not None: self.headers['Content-Type'] = media
        self.content = Content(raw)
    async def __aenter__(self): return self
    async def __aexit__(self, *args): pass


class Session:
    def __init__(self, response): self.response = response
    def get(self, url, **kwargs): return self.response


class ImageMediaValidationTests(unittest.IsolatedAsyncioTestCase):
    def raster(self, format):
        stream = io.BytesIO(); Image.new('RGB', (8, 6), 'blue').save(stream, format=format)
        return stream.getvalue()

    def asset(self, suffix='.webp'):
        return Asset('https://docs.example/original'+suffix, 'image', 'https://docs.example/page', 'Original alt', 'img[0]')

    def policy(self, **kwargs): return AcquisitionPolicy(('docs.example',), **kwargs)

    async def fetch(self, raw, media, suffix='.webp', **policy):
        return await acquisition.acquire_asset(self.asset(suffix), self.policy(**policy), session=Session(Response(raw, media)))

    def cache(self, capture, suffix='.webp', **policy):
        asset = self.asset(suffix)
        return acquisition._validate_cached_asset(capture, asset.url, asset, self.policy(**policy))

    async def test_jpeg_at_webp_url_preserves_original_filename_bytes_and_evidence(self):
        raw = self.raster('JPEG')
        result = await self.fetch(raw, 'image/jpeg')
        self.assertEqual(result.filename, 'original.webp')
        self.assertEqual(result.requested_uri, self.asset().url)
        self.assertEqual(result.raw_bytes, raw)
        self.assertEqual(result.raw_sha256, 'sha256:'+hashlib.sha256(raw).hexdigest())
        details = result.evidence['inspected_image']
        self.assertEqual(details['url_extension'], '.webp')
        self.assertEqual(details['detected_format'], 'JPEG')
        self.assertEqual(details['validation_extension'], '.jpg')
        self.assertTrue(details['url_extension_mismatch'])
        self.assertEqual(self.cache(result).evidence['inspected_image'], details)

    async def test_wrong_or_missing_raster_mime_is_preserved_and_corrected_by_verified_body(self):
        for format, declared, suffix, effective in [('JPEG', 'image/png', '.png', 'image/jpeg'),
                ('WEBP', 'image/jpeg', '.jpg', 'image/webp'), ('WEBP', None, '.webp', 'image/webp'),
                ('PNG', 'application/octet-stream', '.png', 'image/png')]:
            with self.subTest(format=format, declared=declared):
                result = await self.fetch(self.raster(format), declared, suffix)
                self.assertEqual(result.media_type, effective)
                self.assertEqual(result.evidence['content_type'], declared)
                self.assertEqual(result.filename, 'original'+suffix)
                inspected = result.evidence['inspected_image']
                self.assertTrue(inspected['declared_type_mismatch'])
                self.assertEqual(inspected['declared_type_missing'], declared is None)
                self.assertEqual(self.cache(result, suffix), result)

    async def test_valid_png_and_jpeg_accept_trailing_bytes_including_cache(self):
        for format, media, extension in [('PNG', 'image/png', '.png'), ('JPEG', 'image/jpeg', '.jpg')]:
            with self.subTest(format=format):
                raw = self.raster(format)+b'original trailing metadata'
                result = await self.fetch(raw, media, extension)
                self.assertEqual(result.raw_bytes, raw)
                self.assertEqual(result.evidence['inspected_image']['detected_format'], format)
                self.assertFalse(result.evidence['inspected_image']['url_extension_mismatch'])
                self.assertEqual(self.cache(result, extension).raw_bytes, raw)

    async def test_truncated_corrupt_and_signature_only_bodies_fail(self):
        for raw, media, extension in [(self.raster('PNG')[:40], 'image/png', '.png'),
                                     (self.raster('JPEG')[:80], 'image/jpeg', '.jpg'),
                                     (b'\xff\xd8\xffgarbage\xff\xd9', 'image/jpeg', '.jpg'),
                                     (b'<html>Login page</html>', 'image/png', '.png')]:
            with self.subTest(media=media, raw=raw[:12]):
                with self.assertRaises(acquisition.AssetAcquisitionError): await self.fetch(raw, media, extension)

    async def test_cache_does_not_trust_claimed_mime_or_validation_evidence(self):
        good = await self.fetch(self.raster('PNG'), 'image/png', '.png')
        metadata = dict(good.evidence); metadata['content_type'] = 'image/jpeg'
        mislabeled = replace(good, media_type='image/jpeg', evidence=metadata)
        with self.assertRaises(acquisition.AssetAcquisitionError): self.cache(mislabeled, '.png')
        corrupt = b'\x89PNG\r\n\x1a\ncorrupt'
        metadata = dict(good.evidence); metadata['byte_size'] = len(corrupt)
        with self.assertRaises(acquisition.AssetAcquisitionError):
            self.cache(replace(good, raw_bytes=corrupt, raw_sha256='sha256:'+hashlib.sha256(corrupt).hexdigest(), evidence=metadata), '.png')
        metadata = dict(good.evidence); metadata['inspected_image'] = {'detected_format': 'FORGED'}
        with self.assertRaises(acquisition.AssetAcquisitionError):
            self.cache(replace(good, evidence=metadata), '.png')

    async def test_cache_backward_compatibility_requires_correct_original_header(self):
        good = await self.fetch(self.raster('PNG'), 'image/png', '.png')
        metadata = dict(good.evidence); metadata.pop('inspected_image')
        self.assertEqual(self.cache(replace(good, evidence=metadata), '.png').raw_bytes, good.raw_bytes)
        overridden = await self.fetch(self.raster('JPEG'), 'image/png', '.png')
        metadata = dict(overridden.evidence); metadata.pop('inspected_image')
        with self.assertRaises(acquisition.AssetAcquisitionError): self.cache(replace(overridden, evidence=metadata), '.png')

    async def test_gif_avif_svg_and_document_boundaries_are_unchanged(self):
        for extension, media in [('.gif', 'image/gif'), ('.avif', 'image/avif'), ('.svg', 'image/jpeg'), ('.pdf', 'image/jpeg')]:
            with self.subTest(extension=extension):
                with self.assertRaises(acquisition.AssetAcquisitionError): await self.fetch(self.raster('JPEG'), media, extension)
        with self.assertRaises(acquisition.AssetAcquisitionError): await self.fetch(self.raster('JPEG'), 'text/html', '.jpg')
        with self.assertRaises(acquisition.AssetAcquisitionError): await self.fetch(self.raster('JPEG'), 'image/svg+xml', '.jpg')

    async def test_all_qualified_rasters_fully_decode(self):
        for format, media, suffix in [('PNG', 'image/png', '.png'), ('JPEG', 'image/jpeg', '.jpeg'),
                ('WEBP', 'image/webp', '.webp'), ('TIFF', 'image/tiff', '.tif'), ('BMP', 'image/bmp', '.bmp')]:
            with self.subTest(format=format):
                result = await self.fetch(self.raster(format), media, suffix)
                self.assertEqual(result.evidence['inspected_image']['detected_format'], format)
                self.assertEqual(self.cache(result, suffix).raw_bytes, result.raw_bytes)

    async def test_multiframe_and_excessive_pixel_rasters_fail_before_native_processing(self):
        animation = io.BytesIO()
        Image.new('RGB', (8, 6), 'red').save(animation, format='WEBP', save_all=True,
            append_images=[Image.new('RGB', (8, 6), 'blue')], duration=50, loop=0)
        with self.assertRaises(acquisition.AssetAcquisitionError): await self.fetch(animation.getvalue(), 'image/webp')
        # A valid PNG header with an oversized canvas must fail before allocation.
        huge = bytearray(self.raster('PNG'))
        huge[16:24] = struct.pack('>II', 8001, 4000)
        huge[29:33] = struct.pack('>I', zlib.crc32(huge[12:29]))
        with self.assertRaises(acquisition.AssetAcquisitionError): await self.fetch(bytes(huge), 'image/png', '.png')
        with self.assertRaises(acquisition.AssetAcquisitionError): await self.fetch(self.raster('GIF'), 'image/png', '.png')

    async def test_variation_does_not_bypass_ignored_format(self):
        with self.assertRaises(acquisition.AssetAcquisitionError):
            await self.fetch(self.raster('JPEG'), 'image/jpeg', ignored_extensions=('.jpg',))
        with self.assertRaises(acquisition.AssetAcquisitionError):
            await self.fetch(self.raster('JPEG'), 'image/jpeg', ignored_extensions=('.webp',))


if __name__ == '__main__': unittest.main()
