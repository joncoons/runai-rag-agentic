"""Bounded HTML acquisition for the durable ingestion boundary.

Acquisition semantics follow the local upstream queued crawler/external/rag-crawler
origin assessment. The pure table normalizer is adapted from that origin; its exact source is
locked in deployment/application/crawler-source-lock.json. No mutable external
import, legacy embedding or ES writes occur. Returned bytes require durable retention
and provenance admission by the caller. Assets are discoveries, not downloads or
successful extraction receipts. Completion is relative to the configured scope;
it never establishes deletion. Sparse JavaScript pages use the original Selenium renderer; sitemap seeding remains disabled.
"""
from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import ipaddress
from pathlib import PurePosixPath
import socket
from urllib.parse import urljoin, urlsplit, urlunsplit, unquote
import posixpath

import aiohttp
from bs4 import BeautifulSoup


class AcquisitionError(ValueError):
    """Scope, response or acquisition failure; no partial page is returned."""


@dataclass(frozen=True)
class AcquisitionPolicy:
    allowed_hosts: tuple[str, ...]
    max_depth: int | None = 2
    max_pages: int | None = None
    page_prefixes: tuple[str, ...] = ()
    max_response_bytes: int = 8 * 1024 * 1024
    max_asset_response_bytes: int = 128 * 1024 * 1024
    timeout_seconds: float = 30
    max_redirects: int = 5
    private_hosts: tuple[str, ...] = ()
    private_ports: tuple[int, ...] = ()
    prefer_https: bool = False
    include_images: bool = True
    included_extensions: tuple[str, ...] = ('.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx', '.txt', '.md', '.rst', '.csv')
    ignored_extensions: tuple[str, ...] = ()

    def __post_init__(self):
        if not self.allowed_hosts or any(not h or '/' in h or '@' in h for h in self.allowed_hosts):
            raise ValueError('Explicit hostname allowlist required')
        if not {h.lower() for h in self.private_hosts}.issubset({h.lower() for h in self.allowed_hosts}):raise ValueError('Private acquisition scope must be an explicit allowed hostname')
        if any(isinstance(p,bool) or not isinstance(p,int) or not 1<=p<=65535 for p in self.private_ports):raise ValueError('Invalid operator private port scope')
        for prefix in self.page_prefixes:
            parts=urlsplit(validate_url(prefix,self))
            if parts.query or parts.fragment:raise ValueError('HTML prefixes cannot contain query or fragment')
        if (self.max_depth is not None and (isinstance(self.max_depth,bool) or not isinstance(self.max_depth,int) or self.max_depth < 0)) or (self.max_pages is not None and (isinstance(self.max_pages,bool) or self.max_pages < 1)) or self.max_response_bytes < 1 or self.max_asset_response_bytes < 1 or self.timeout_seconds <= 0 or self.max_redirects < 0:
            raise ValueError('Invalid acquisition bounds')


@dataclass(frozen=True)
class Asset:
    url: str
    kind: str
    parent_url: str
    alt: str = ''
    locator: str = ''


@dataclass(frozen=True)
class PageCapture:
    requested_uri: str
    final_uri: str
    raw_bytes: bytes
    raw_sha256: str
    text: str
    title: str
    links: tuple[str, ...]
    assets: tuple[Asset, ...]
    evidence: dict
    http_raw_bytes: bytes | None = None


@dataclass(frozen=True)
class CrawlCapture:
    pages: tuple[PageCapture, ...]
    errors: tuple[dict, ...]
    complete: bool
    missing_means_deleted: bool = False


def _public_address(value):
    address = ipaddress.ip_address(value)
    if not address.is_global or (address.version == 6 and address.ipv4_mapped and not address.ipv4_mapped.is_global):
        raise AcquisitionError('Non-public destination is outside acquisition scope')


class PublicResolver(aiohttp.resolver.DefaultResolver):
    """Validate addresses used by the connector, not a separate DNS precheck."""
    def __init__(self, *, private_hosts=()):
        super().__init__();self.private_hosts={h.lower() for h in private_hosts}

    async def resolve(self, host, port=0, family=socket.AF_INET):
        answers = await super().resolve(host, port, family)
        if not answers:
            raise AcquisitionError('Destination has no DNS answers')
        for answer in answers:
            if host.lower() not in self.private_hosts:_public_address(answer['host'])
        return answers


def validate_url(url: str, policy: AcquisitionPolicy) -> str:
    try:
        parts = urlsplit(url)
        host = (parts.hostname or '').encode('idna').decode('ascii').lower()
        allowed = {h.encode('idna').decode('ascii').lower() for h in policy.allowed_hosts}
        if parts.scheme not in ('http', 'https') or host not in allowed or parts.username is not None or parts.password is not None:
            raise AcquisitionError('URL outside explicit host/scheme scope')
        if any(ord(c) < 33 for c in url) or '\\' in url or '%' in host:
            raise AcquisitionError('Invalid URL characters')
        private=host in {h.lower() for h in policy.private_hosts}
        if parts.port not in (None, 80 if parts.scheme == 'http' else 443) and not (private and parts.port in policy.private_ports):
            raise AcquisitionError('Nonstandard destination port')
        try:
            ipaddress.ip_address(host)
        except ValueError:
            if not private and (host == 'localhost' or host.endswith(('.localhost', '.local', '.internal'))):
                raise AcquisitionError('Non-public hostname')
        else:
            if not private:_public_address(host)
        authority = f'[{host}]' if ':' in host else host
        if parts.port is not None and parts.port!=(80 if parts.scheme=='http' else 443):authority+=':'+str(parts.port)
        return urlunsplit((parts.scheme, authority, parts.path or '/', parts.query, ''))
    except (ValueError, UnicodeError) as exc:
        if isinstance(exc, AcquisitionError): raise
        raise AcquisitionError('Invalid URL') from exc


def html_in_scope(url,policy):
    if not policy.page_prefixes:return True
    def normalized_path(value):
        path=urlsplit(value).path
        for _ in range(6):
            decoded=unquote(path)
            if decoded==path:break
            path=decoded
        if '%' in path or '\\' in path:return None
        return posixpath.normpath('/'+path.lstrip('/')).rstrip('/') or '/'
    value=urlsplit(url);path=normalized_path(url)
    if path is None:return False
    for prefix in policy.page_prefixes:
        p=urlsplit(prefix);base=normalized_path(prefix)
        if base is not None and (value.scheme,value.netloc)==(p.scheme,p.netloc) and (base=='/' or path==base or path.startswith(base+'/')):return True
    return False


_IMAGES = {'.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg', '.bmp', '.tif', '.tiff', '.avif'}
_NON_HTML = _IMAGES | {'.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx', '.txt', '.md', '.rst', '.csv', '.zip', '.gz', '.mp4', '.mp3', '.json', '.yaml', '.yml', '.xml', '.js', '.css'}


# Adapted only to remove typing.Any; see crawler-source-lock.json.
def html_table_to_markdown(table_tag) -> str:
    """Convert a BeautifulSoup <table> tag to GitHub-Flavored Markdown."""
    rows: list[str] = []
    for tr in table_tag.find_all("tr"):
        cells = [
            cell.get_text(separator=" ", strip=True).replace("|", "\\|")
            for cell in tr.find_all(["th", "td"])
        ]
        if cells:
            rows.append("| " + " | ".join(cells) + " |")
    if not rows:
        return ""
    header = rows[0]
    col_count = max(1, header.count("|") - 1)
    separator = "| " + " | ".join(["---"] * col_count) + " |"
    return "\n".join([header, separator] + rows[1:])


def _normalize(raw, final, policy):
    soup = BeautifulSoup(raw, 'html.parser')
    title = soup.title.get_text(' ', strip=True) if soup.title else ''
    links, assets = set(), {}
    included = {e.lower() for e in policy.included_extensions}
    ignored = {e.lower() for e in policy.ignored_extensions}

    def discover(value, kind=None, node=None, locator=''):
        if not value: return
        try:
            url = validate_url(urljoin(final, value), policy)
        except AcquisitionError:
            return
        if policy.prefer_https and urlsplit(url).scheme=='http':
            parts=urlsplit(url);url=validate_url(urlunsplit(('https',parts.netloc,parts.path,parts.query,'')),policy)
        suffix = PurePosixPath(urlsplit(url).path).suffix.lower()
        if kind == 'image' and suffix in (_NON_HTML - _IMAGES):return
        if suffix in ignored: return
        if kind == 'image' or suffix in _IMAGES:
            if policy.include_images:
                assets.setdefault((url, locator), Asset(url, 'image', final, node.get('alt', '') if node else '', locator))
        elif suffix in included:
            assets.setdefault((url, locator), Asset(url, 'binary', final, '', locator))
        elif suffix not in _NON_HTML and kind is None and html_in_scope(url,policy):
            links.add(url)

    for index, node in enumerate(soup.find_all(['a', 'img', 'source'])):
        locator = f'{node.name}[{index}]'
        if node.name == 'a':
            discover(node.get('href'), node=node, locator=locator)
        else:
            # Audio/video source nodes are not image candidates.
            if node.name == 'source' and not node.find_parent('picture'): continue
            for attr in ('src', 'data-src', 'data-lazy-src'):
                discover(node.get(attr), 'image', node, locator)
            for attr in ('srcset', 'data-srcset'):
                for candidate in node.get(attr, '').split(','):
                    fields = candidate.strip().split()
                    if fields: discover(fields[0], 'image', node, locator)
    return normalize_html_text(raw), title, tuple(sorted(links)), tuple(sorted(assets.values(), key=lambda a: (a.url, a.locator)))


def normalize_html_text_legacy(raw: bytes) -> str:
    """Shared wholepage representation used by file inspection and acquisition."""
    soup = BeautifulSoup(raw, 'html.parser')
    for node in soup.find_all(['script', 'style', 'nav', 'header', 'footer', 'noscript', 'template']):
        node.decompose()
    for table in list(soup.find_all('table')):
        if table.parent is not None:
            table.replace_with(soup.new_string(html_table_to_markdown(table)))
    root = soup.find('main') or soup.find('article') or soup.body or soup
    text = '\n'.join(line for line in (' '.join(v.split()) for v in root.get_text('\n', strip=True).splitlines()) if line)
    if not text: raise AcquisitionError('HTML has no normalized text')
    return text


def normalize_html_text(raw: bytes) -> str:
    from .html_disclosures import html_units
    return html_units(raw)[0]['text']


async def _acquire(url, policy, session, renderer=None):
    requested = current = validate_url(url, policy)
    if not html_in_scope(current,policy):raise AcquisitionError('HTML URL outside selected section scope')
    hops = []
    for hop in range(policy.max_redirects + 1):
        async with session.get(current, allow_redirects=False) as response:
            hops.append({'uri': current, 'status': response.status})
            if response.status in (301, 302, 303, 307, 308):
                location = response.headers.get('Location')
                if not location or hop == policy.max_redirects:
                    raise AcquisitionError('Missing redirect location or redirect limit exceeded')
                destination = validate_url(urljoin(current, location), policy)
                if not html_in_scope(destination,policy):raise AcquisitionError('HTML redirect outside selected section scope')
                if urlsplit(current).scheme == 'https' and urlsplit(destination).scheme != 'https':
                    raise AcquisitionError('HTTPS downgrade redirect')
                current = destination
                continue
            if response.status != 200:
                raise AcquisitionError(f'HTTP status {response.status}')
            media_type = response.headers.get('Content-Type', '').split(';')[0].strip().lower()
            if media_type not in ('text/html', 'application/xhtml+xml'):
                raise AcquisitionError('HTML content type required')
            length = response.headers.get('Content-Length')
            if length is not None:
                try: declared = int(length)
                except ValueError as exc: raise AcquisitionError('Invalid response length') from exc
                if declared < 0 or declared > policy.max_response_bytes:
                    raise AcquisitionError('Response exceeds byte bound')
            chunks, size = [], 0
            async for chunk in response.content.iter_chunked(65536):
                size += len(chunk)
                if size > policy.max_response_bytes:
                    raise AcquisitionError('Response exceeds byte bound')
                chunks.append(chunk)
            raw = b''.join(chunks)
            render_evidence = None
            http_original = None
            from .browser_acquisition import is_js_sparse
            if renderer is not None and is_js_sparse(raw):
                http_sha = 'sha256:' + hashlib.sha256(raw).hexdigest()
                # Retain the original response as evidence alongside the rendered DOM.
                http_original = raw
                rendered = await renderer(current)
                current = validate_url(rendered.final_uri, policy)
                if not html_in_scope(current, policy): raise AcquisitionError('Rendered page outside section scope')
                raw = rendered.raw_bytes
                if len(raw) > policy.max_response_bytes: raise AcquisitionError('Rendered DOM exceeds acquisition bound')
                render_evidence = {**rendered.evidence, 'http_html_sha256': http_sha}
            text, title, links, assets = _normalize(raw, current, policy)
            return PageCapture(requested, current, raw, 'sha256:' + hashlib.sha256(raw).hexdigest(), text, title, links, assets, {
                'retrieved_at': datetime.now(timezone.utc).isoformat(), 'http_status': response.status,
                'content_type': response.headers.get('Content-Type'), 'etag': response.headers.get('ETag'),
                'last_modified': response.headers.get('Last-Modified'), 'redirect_chain': hops,
                'parser': 'beautifulsoup-html.parser', 'normalization': 'html-disclosures-v4',
                'asset_acquisition_complete': False,
                **({'rendering': render_evidence} if render_evidence else {}),
            }, http_raw_bytes=http_original)
    raise AcquisitionError('Redirect limit exceeded')


async def acquire_page(url: str, policy: AcquisitionPolicy, *, session=None, renderer=None) -> PageCapture:
    """Acquire one HTML page; injected sessions are trusted offline-test boundaries."""
    owned_renderer = None
    if renderer is None and session is None:
        from .browser_acquisition import BrowserRenderer
        owned_renderer = renderer = BrowserRenderer(policy)
    try:
        async with asyncio.timeout(policy.timeout_seconds + (75 if renderer is not None else 0)):
            if session is not None:
                return await _acquire(url, policy, session, renderer)
            connector = aiohttp.TCPConnector(resolver=PublicResolver(private_hosts=policy.private_hosts), use_dns_cache=False)
            async with aiohttp.ClientSession(connector=connector, trust_env=False, cookie_jar=aiohttp.DummyCookieJar(), timeout=aiohttp.ClientTimeout(total=policy.timeout_seconds), headers={'User-Agent': 'runai-rag-agentic-acquisition/1'}) as owned:
                return await _acquire(url, policy, owned, renderer)
    except (aiohttp.ClientError, TimeoutError, OSError) as exc:
        raise AcquisitionError(f'Acquisition failed: {type(exc).__name__}') from exc
    finally:
        if owned_renderer is not None: await owned_renderer.close()


async def collect(seed_urls, policy: AcquisitionPolicy, *, session=None, capture_store=None, page_sink=None, renderer=None) -> CrawlCapture:
    """Use the original local queued crawler with capture-only durable boundaries."""
    from ingestion.queued_crawler_runtime.crawl import SimpleWebCrawler
    urls=sorted(set(seed_urls));pages=[];finals=set()
    if not urls:return CrawlCapture((),({'uri':'','error':'No seed URLs'},),False)
    try:
        for url in urls:
            validate_url(url,policy)
            if not html_in_scope(url,policy):raise AcquisitionError('Seed outside selected HTML section')
    except AcquisitionError as error:return CrawlCapture((),({'uri':url,'error':str(error)},),False)
    owned_renderer = None
    if renderer is None and session is None:
        from .browser_acquisition import BrowserRenderer
        owned_renderer = renderer = BrowserRenderer(policy)
    async def fetch(url):
        if capture_store is not None:
            cached=await capture_store.get_page(url)
            if cached is not None:return cached
        page=await acquire_page(url,policy,session=session,renderer=renderer)
        if capture_store is not None:await capture_store.put_page(url,page)
        return page
    sink_lock=asyncio.Lock()
    async def sink(batch):
        async with sink_lock:
            unique={p.final_uri:p for p in reversed(batch) if p.final_uri not in finals}
            admitted=list(unique.values())
            if admitted and page_sink is not None:
                await page_sink(tuple(admitted))
            for page in admitted:
                finals.add(page.final_uri);pages.append(page)
        return len(batch)
    crawler=SimpleWebCrawler(urls[0],seed_urls=urls[1:],max_depth=policy.max_depth,max_pages=policy.max_pages,
        allowed_url_prefixes=list(policy.page_prefixes) or None,use_product_url_map=False,use_sitemap=False,
        use_selenium=False,extract_linked_files=False,batch_size=(1 if page_sink is not None else 8),max_concurrent_batches=4,
        capture_fetch=fetch,capture_sink=sink)
    try:
        result=await crawler.crawl('durable-acquisition')
    finally:
        if owned_renderer is not None: await owned_renderer.close()
    errors=tuple({'uri':item.get('url',''),'error':item.get('error') or item.get('error_type') or 'Acquisition failed'} for item in result['errors'])
    if not result.get('queue_exhausted',False):errors+=({'uri':'','error':'Page budget reached with pending frontier'},)
    if not pages and not errors:errors=({'uri':'','error':'No captured pages'},)
    return CrawlCapture(tuple(pages),errors,bool(result.get('complete')) and not errors)
