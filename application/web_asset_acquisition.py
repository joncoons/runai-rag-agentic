"""Bounded asset acquisition only: raw bytes and relationship evidence.

No extraction, storage, publication or deletion decisions occur here. Injected
sessions are trusted offline-test boundaries, as in web_acquisition.acquire_page.
Without an explicit capture store, every invocation fetches full bodies.
An explicit store reuses captures only after current-policy revalidation.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import hashlib
import io
from pathlib import PurePosixPath
from urllib.parse import unquote, urljoin, urlsplit
import zipfile

import aiohttp

from application.web_acquisition import AcquisitionError, AcquisitionPolicy, Asset, PublicResolver, validate_url


class AssetAcquisitionError(AcquisitionError):
    """Failed observation; even confirmed HTTP absence never establishes deletion."""
    missing_means_deleted = False

    def __init__(self, message, *, http_status=None):
        super().__init__(message)
        self.http_status = http_status
        self.observation = 'absent' if http_status in (404, 410) else 'unavailable'


@dataclass(frozen=True)
class AssetCapture:
    requested_uri: str
    final_uri: str
    raw_bytes: bytes
    raw_sha256: str
    media_type: str
    filename: str
    evidence: dict
    parent_url: str
    kind: str
    locator: str
    alt: str


_TYPES = {
    '.svg': ('image/svg+xml',),
    '.png': ('image/png',), '.jpg': ('image/jpeg',), '.jpeg': ('image/jpeg',),
    '.webp': ('image/webp',), '.tif': ('image/tiff',), '.tiff': ('image/tiff',),
    '.bmp': ('image/bmp',), '.pdf': ('application/pdf',),
    '.docx': ('application/vnd.openxmlformats-officedocument.wordprocessingml.document',),
    '.pptx': ('application/vnd.openxmlformats-officedocument.presentationml.presentation',),
    '.xlsx': ('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',),
    '.xls': ('application/vnd.ms-excel',), '.csv': ('text/csv', 'application/csv'),
    '.md': ('text/markdown', 'text/x-markdown', 'text/plain'),
    '.markdown': ('text/markdown', 'text/x-markdown', 'text/plain'), '.txt': ('text/plain',),
}
_IMAGE_EXTENSIONS = {'.svg','.png', '.jpg', '.jpeg', '.webp', '.tif', '.tiff', '.bmp'}
_RASTER_EXTENSIONS = _IMAGE_EXTENSIONS - {'.svg'}
_RASTER_FORMATS = {'PNG': ('image/png', '.png'), 'JPEG': ('image/jpeg', '.jpg'),
                   'WEBP': ('image/webp', '.webp'), 'TIFF': ('image/tiff', '.tiff'),
                   'BMP': ('image/bmp', '.bmp')}
_RASTER_MEDIA = {media for media, extension in _RASTER_FORMATS.values()}
_DEFAULT_EXTENSIONS = {media: ext for ext, media in (
    ('.png', 'image/png'), ('.jpg', 'image/jpeg'), ('.webp', 'image/webp'),
    ('.tiff', 'image/tiff'), ('.bmp', 'image/bmp'), ('.pdf', 'application/pdf'),
    ('.docx', _TYPES['.docx'][0]), ('.pptx', _TYPES['.pptx'][0]),
    ('.xlsx', _TYPES['.xlsx'][0]), ('.xls', 'application/vnd.ms-excel'),
    ('.csv', 'text/csv'), ('.csv', 'application/csv'), ('.md', 'text/markdown'),
    ('.md', 'text/x-markdown'), ('.txt', 'text/plain'))}


def _metadata(uri, media, asset, policy):
    name = PurePosixPath(unquote(urlsplit(uri).path)).name
    # Decoded path separators and controls cannot become filesystem names.
    if '/' in name or '\\' in name or any(ord(c) < 32 or ord(c) == 127 for c in name):
        raise AssetAcquisitionError('Unsafe asset filename')
    extension = PurePosixPath(name).suffix.lower()
    if not extension:
        extension = _DEFAULT_EXTENSIONS.get(media, '')
        name = (name or 'asset') + extension
    if extension not in _TYPES:
        raise AssetAcquisitionError(f'Unsupported asset extension: {extension or "missing"}')
    raster = asset.kind == 'image' and extension in _RASTER_EXTENSIONS
    if media not in _TYPES[extension] and not (
            raster and media in _RASTER_MEDIA | {'', 'application/octet-stream'}):
        raise AssetAcquisitionError('Asset extension/content type mismatch')
    if extension in {e.lower() for e in policy.ignored_extensions}:
        raise AssetAcquisitionError('Asset extension excluded by policy')
    if asset.kind == 'image':
        if not policy.include_images or extension not in _IMAGE_EXTENSIONS:
            raise AssetAcquisitionError('Image is outside selected format policy')
    elif asset.kind == 'binary':
        if extension in _IMAGE_EXTENSIONS or extension not in {e.lower() for e in policy.included_extensions}:
            raise AssetAcquisitionError('Binary is outside selected format policy')
    else:
        raise AssetAcquisitionError('Unsupported asset kind')
    return name, extension


def _validate_body(raw, extension):
    valid = False
    if extension == '.svg':
        from .svg_images import validate_svg
        try:validate_svg(raw);valid=True
        except ValueError:valid=False
    elif extension in _RASTER_EXTENSIONS:
        # Actual raster content governs admission, including servers that omit
        # or mislabel raster MIME. Verify structure and fully decode every pixel.
        from PIL import Image
        try:
            with Image.open(io.BytesIO(raw)) as image:
                if image.format not in _RASTER_FORMATS:
                    raise ValueError('Unqualified raster format')
                if image.width * image.height > 32000000 or getattr(image, 'n_frames', 1) != 1:
                    raise ValueError('Unqualified raster pixel/frame count')
                details = {'detected_format': image.format, 'width': image.width,
                           'height': image.height, 'frames': 1}
                image.verify()
            with Image.open(io.BytesIO(raw)) as image:
                image.load()
            return details
        except (OSError, SyntaxError, ValueError, EOFError, Image.DecompressionBombError) as error:
            raise AssetAcquisitionError('Asset body does not match admitted raster format') from error
    elif extension == '.pdf': valid = raw.startswith(b'%PDF-')
    elif extension == '.xls': valid = raw.startswith(b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1')
    elif extension in ('.docx', '.pptx', '.xlsx'):
        required = {'.docx': 'word/document.xml', '.pptx': 'ppt/presentation.xml', '.xlsx': 'xl/workbook.xml'}[extension]
        try:
            # Inspect the bounded central directory, never decompress remote members.
            with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                names = set(archive.namelist())
                valid = '[Content_Types].xml' in names and required in names
        except (zipfile.BadZipFile, ValueError, OSError):
            pass
    elif extension in ('.txt', '.md', '.markdown', '.csv'):
        try:
            text = raw.decode('utf-8-sig')
            valid = bool(text.strip()) and not any(ord(c) < 32 and c not in '\t\n\r' for c in text)
            prefix = text.lstrip().lower()
            valid = valid and not prefix.startswith(('<!doctype html', '<html', '<script', '<?xml', '<svg'))
        except UnicodeError:
            pass
    if not valid:
        raise AssetAcquisitionError('Asset body does not match admitted format signature')
    return None


def _inspected_image(uri, declared, details, policy):
    media, extension = _RASTER_FORMATS[details['detected_format']]
    ignored = {value.lower() for value in policy.ignored_extensions}
    if any(candidate in ignored and media in _TYPES[candidate] for candidate in _RASTER_EXTENSIONS):
        raise AssetAcquisitionError('Detected raster format excluded by policy')
    original = PurePosixPath(urlsplit(uri).path).suffix.lower()
    return media, {'schema_version': 'inspected-raster.v1', **details,
        'media_type': media, 'validation_extension': extension, 'url_extension': original,
        'declared_media_type': declared, 'declared_type_missing': not bool(declared),
        'declared_type_mismatch': declared != media,
        'url_extension_mismatch': media not in _TYPES.get(original, ()),
        'validation': 'pillow-verify-load-v1', 'normalization': 'native-pillow-image'}


async def _acquire(asset, policy, session):
    requested = current = validate_url(asset.url, policy)
    validate_url(asset.parent_url, policy)
    hops = []
    for hop in range(policy.max_redirects + 1):
        async with session.get(current, allow_redirects=False) as response:
            hops.append({'uri': current, 'status': response.status})
            if response.status in (301, 302, 303, 307, 308):
                location = response.headers.get('Location')
                if not location or hop == policy.max_redirects:
                    raise AssetAcquisitionError('Missing redirect location or redirect limit exceeded')
                destination = validate_url(urljoin(current, location), policy)
                if urlsplit(current).scheme == 'https' and urlsplit(destination).scheme != 'https':
                    raise AssetAcquisitionError('HTTPS downgrade redirect')
                current = destination
                continue
            if response.status != 200:
                raise AssetAcquisitionError(f'HTTP status {response.status}', http_status=response.status)
            media = response.headers.get('Content-Type', '').split(';')[0].strip().lower()
            filename, extension = _metadata(current, media, asset, policy)
            declared = None
            if response.headers.get('Content-Length') is not None:
                try: declared = int(response.headers['Content-Length'])
                except (ValueError, TypeError) as exc: raise AssetAcquisitionError('Invalid response length') from exc
                if declared < 0 or declared > policy.max_response_bytes:
                    raise AssetAcquisitionError('Asset exceeds byte bound')
            chunks, size = [], 0
            async for chunk in response.content.iter_chunked(65536):
                size += len(chunk)
                if size > policy.max_response_bytes:
                    raise AssetAcquisitionError('Asset exceeds byte bound')
                chunks.append(chunk)
            # Length is the transferred representation; reject compressed responses
            # rather than compare decoded bytes to an encoded Content-Length.
            if response.headers.get('Content-Encoding', 'identity').lower() != 'identity':
                raise AssetAcquisitionError('Encoded asset responses unsupported')
            if declared is not None and declared != size:
                raise AssetAcquisitionError('Asset response length mismatch/truncation')
            raw = b''.join(chunks)
            details = _validate_body(raw, extension)
            inspected = None
            if details is not None:
                media, inspected = _inspected_image(requested, media, details, policy)
            return AssetCapture(requested, current, raw, 'sha256:' + hashlib.sha256(raw).hexdigest(), media, filename, {
                'retrieved_at': datetime.now(timezone.utc).isoformat(), 'http_status': response.status,
                'content_type': response.headers.get('Content-Type'), 'etag': response.headers.get('ETag'),
                'last_modified': response.headers.get('Last-Modified'), 'redirect_chain': hops,
                'byte_size': size, 'validation': 'asset-signature-v1', 'missing_means_deleted': False,
                **({'inspected_image': inspected} if inspected is not None else {}),
            }, asset.parent_url, asset.kind, asset.locator, asset.alt)
    raise AssetAcquisitionError('Redirect limit exceeded')


async def acquire_asset(asset: Asset, policy: AcquisitionPolicy, *, session=None) -> AssetCapture:
    """Fetch and validate a complete selected asset, always hashing the new body."""
    try:
        async with asyncio.timeout(policy.timeout_seconds):
            if session is not None:
                return await _acquire(asset, policy, session)
            connector = aiohttp.TCPConnector(resolver=PublicResolver(private_hosts=policy.private_hosts), use_dns_cache=False)
            async with aiohttp.ClientSession(connector=connector, trust_env=False, cookie_jar=aiohttp.DummyCookieJar(), timeout=aiohttp.ClientTimeout(total=policy.timeout_seconds), headers={'User-Agent': 'runai-rag-agentic-acquisition/1', 'Accept-Encoding': 'identity'}) as owned:
                return await _acquire(asset, policy, owned)
    except AssetAcquisitionError:
        raise
    except AcquisitionError as exc:
        raise AssetAcquisitionError(str(exc)) from exc
    except (aiohttp.ClientError, TimeoutError, OSError) as exc:
        raise AssetAcquisitionError(f'Asset acquisition failed: {type(exc).__name__}') from exc


def _validate_cached_asset(result, url, asset, policy):
    """Treat persisted captures as untrusted observations under the current policy."""
    if (not isinstance(result, AssetCapture) or not isinstance(result.raw_bytes, bytes)
            or any(not isinstance(getattr(result, field), str) for field in
                   ('requested_uri', 'final_uri', 'parent_url', 'media_type', 'filename', 'raw_sha256'))):
        raise AssetAcquisitionError('Invalid cached asset capture')
    requested = validate_url(result.requested_uri, policy)
    final = validate_url(result.final_uri, policy)
    validate_url(result.parent_url, policy)
    if requested != url:
        raise AssetAcquisitionError('Cached asset requested URL mismatch')
    if len(result.raw_bytes) > policy.max_response_bytes:
        raise AssetAcquisitionError('Cached asset exceeds response byte bound')
    evidence = result.evidence
    if not isinstance(evidence, dict) or evidence.get('http_status') != 200:
        raise AssetAcquisitionError('Invalid cached HTTP observation')
    content_type = evidence.get('content_type')
    if content_type is not None and not isinstance(content_type, str):
        raise AssetAcquisitionError('Invalid cached content type')
    declared = (content_type or '').split(';')[0].strip().lower()
    filename, extension = _metadata(final, declared, asset, policy)
    if filename != result.filename:
        raise AssetAcquisitionError('Cached asset filename mismatch')
    details = _validate_body(result.raw_bytes, extension)
    inspected = None
    if details is not None:
        effective, inspected = _inspected_image(requested, declared, details, policy)
        if result.media_type != effective:
            raise AssetAcquisitionError('Cached effective image type differs from inspected body')
        saved = evidence.get('inspected_image')
        if (saved is not None and saved != inspected) or (declared != effective and saved is None):
            raise AssetAcquisitionError('Cached image inspection evidence mismatch')
    elif declared != result.media_type:
        raise AssetAcquisitionError('Cached content type mismatch')
    if result.raw_sha256 != 'sha256:' + hashlib.sha256(result.raw_bytes).hexdigest():
        raise AssetAcquisitionError('Cached asset body hash mismatch')
    if type(evidence.get('byte_size')) is not int or evidence['byte_size'] != len(result.raw_bytes):
        raise AssetAcquisitionError('Cached byte size mismatch')
    hops = evidence.get('redirect_chain')
    if not isinstance(hops, (list, tuple)) or not 1 <= len(hops) <= policy.max_redirects + 1:
        raise AssetAcquisitionError('Invalid cached redirect chain')
    previous = None
    for index, hop in enumerate(hops):
        if not isinstance(hop, dict) or not isinstance(hop.get('uri'), str):
            raise AssetAcquisitionError('Invalid cached redirect hop')
        destination = validate_url(hop['uri'], policy)
        if previous is not None and urlsplit(previous).scheme == 'https' and urlsplit(destination).scheme != 'https':
            raise AssetAcquisitionError('Cached HTTPS downgrade redirect')
        if index == 0 and destination != requested:
            raise AssetAcquisitionError('Cached redirect source mismatch')
        if index == len(hops) - 1:
            if destination != final or hop.get('status') != 200:
                raise AssetAcquisitionError('Cached redirect destination mismatch')
        elif hop.get('status') not in (301, 302, 303, 307, 308):
            raise AssetAcquisitionError('Invalid cached redirect status')
        previous = destination
    return replace(result, evidence={**evidence, 'inspected_image': inspected}) if inspected is not None else result


async def acquire_assets(capture, policy: AcquisitionPolicy, *, session=None, max_assets=None,
                         max_aggregate_bytes=None, capture_store=None) -> tuple[AssetCapture, ...]:
    """Acquire unique URLs with four workers and preserve all relationship order.

    Corpus budgets are optional positive operator limits; overflow rejects the
    whole call explicitly. Per-response bounds remain enforced independently.
    An explicitly supplied capture store may reuse fully revalidated observations.
    """
    for bound in (max_assets, max_aggregate_bytes):
        if bound is not None and (isinstance(bound, bool) or not isinstance(bound, int) or bound < 1):
            raise AssetAcquisitionError('Invalid asset batch bounds')
    relations = tuple(asset for page in capture.pages for asset in page.assets)
    unique = {}
    relation_urls = []
    try:
        # Check every parent before deduplication, even if another occurrence of
        # that URL has already been admitted. This also avoids needless fetches.
        for asset in relations:
            url = validate_url(asset.url, policy)
            validate_url(asset.parent_url, policy)
            relation_urls.append(url)
            unique.setdefault(url, asset)
        if max_assets is not None and len(unique) > max_assets:
            raise AssetAcquisitionError('Asset count bound exceeded')
        jobs = iter(unique.items())
        fetched, total = {}, 0

        async def worker():
            nonlocal total
            for url, asset in jobs:
                result = await capture_store.get_asset(url) if capture_store is not None else None
                if result is None:
                    result = await acquire_asset(asset, policy, session=session)
                    if capture_store is not None:
                        await capture_store.put_asset(url, result)
                else:
                    result = _validate_cached_asset(result, url, asset, policy)
                # No await separates counter update and overflow detection.
                total += len(result.raw_bytes)
                if max_aggregate_bytes is not None and total > max_aggregate_bytes:
                    raise AssetAcquisitionError('Aggregate asset byte bound exceeded')
                fetched[url] = result

        tasks = [asyncio.create_task(worker()) for _ in range(min(4, len(unique)))]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        results = []
        for asset, url in zip(relations, relation_urls):
            result = fetched[url]
            _metadata(result.final_uri, result.media_type, asset, policy)
            results.append(replace(result, parent_url=asset.parent_url, kind=asset.kind,
                                   locator=asset.locator, alt=asset.alt))
        return tuple(results)
    except AssetAcquisitionError:
        raise
    except AcquisitionError as exc:
        raise AssetAcquisitionError(str(exc)) from exc
