"""Conservative, bounded SVG-to-PNG normalization for untrusted acquisitions.

Original bytes belong to the acquisition layer. This module never rewrites them.
Unsupported active content, filters and external resources fail closed.
Only bounded, verified, inline base64 PNG images are admitted.
"""
from __future__ import annotations

import math
import base64
import hashlib
import io
import re
import subprocess
import sys
from collections import defaultdict

from defusedxml import ElementTree
import tinycss2

MAX_BYTES = 2_000_000
MAX_NODES = 10_000
MAX_EXPANDED_NODES = 50_000
MAX_DEPTH = 64
MAX_PATH_CHARS = 250_000
RENDER_TIMEOUT = 12
RENDER_MEMORY_BYTES = 1_073_741_824
SVG_NS = 'http://www.w3.org/2000/svg'
_ALLOWED = set('svg image g defs symbol use title desc metadata path rect circle ellipse line polyline polygon text tspan textPath linearGradient radialGradient stop clipPath mask marker pattern switch a style'.split())
_FRAGMENT = re.compile(r'#[A-Za-z_][A-Za-z0-9_.:-]*\Z')
_LENGTH = re.compile(r'([+]?(?:\d+(?:\.\d*)?|\.\d+))(px|pt|pc|mm|cm|in|q)?\Z', re.I)
_UNITS = {'px': 1, 'pt': 96 / 72, 'pc': 16, 'mm': 96 / 25.4, 'cm': 96 / 2.54, 'in': 96, 'q': 96 / 101.6}


class SVGValidationError(ValueError):
    """SVG is unsafe, unsupported, malformed, or exceeds normalization limits."""


def _fragment(value):
    value = value.strip()
    if not _FRAGMENT.fullmatch(value):
        raise SVGValidationError('Only local fragment resources are supported')
    return value[1:]


def _css_tokens(tokens, depth=0):
    if depth > MAX_DEPTH:
        raise SVGValidationError('SVG CSS depth limit exceeded')
    refs = []
    for token in tokens:
        if token.type in ('error', 'at-keyword'):
            raise SVGValidationError('Invalid CSS or unsupported CSS at-rule')
        if token.type == 'url':
            refs.append(_fragment(token.value))
        elif token.type == 'function':
            if token.lower_name == 'url':
                args = [t for t in token.arguments if t.type not in ('whitespace', 'comment')]
                if len(args) == 1 and args[0].type == 'string':
                    value = args[0].value
                else:
                    value = tinycss2.serialize(args)
                refs.append(_fragment(value))
            else:
                refs.extend(_css_tokens(token.arguments, depth + 1))
        elif hasattr(token, 'content'):
            refs.extend(_css_tokens(token.content, depth + 1))
    return refs


def _embedded_png(value, max_pixels):
    """Verify an inline raster without permitting any URI resolution."""
    from PIL import Image

    prefix = 'data:image/png;base64,'
    if not value.startswith(prefix):
        raise SVGValidationError('SVG image requires an inline base64 PNG')
    try:
        payload = base64.b64decode(re.sub(r'[ \t\r\n]', '', value[len(prefix):]), validate=True)
        if not payload.startswith(b'\x89PNG\r\n\x1a\n'):
            raise ValueError('PNG signature mismatch')
        with Image.open(io.BytesIO(payload)) as image:
            width, height = image.size
            if (image.format != 'PNG' or min(width, height) <= 0
                    or max(width, height) > 32768 or width * height > max_pixels):
                raise ValueError('Embedded PNG pixel limit exceeded')
            if getattr(image, 'n_frames', 1) != 1:
                raise ValueError('Animated PNG is unsupported')
            image.verify()
    except Exception as exc:
        raise SVGValidationError('Invalid embedded PNG or decoded pixel limit exceeded') from exc
    return {'media_type': 'image/png', 'width': width, 'height': height, 'pixels': width * height,
            'byte_size': len(payload), 'raw_sha256': 'sha256:' + hashlib.sha256(payload).hexdigest()}


def _dimension(value, fallback):
    if value is None or value.endswith('%'):
        return fallback
    match = _LENGTH.fullmatch(value.strip())
    if not match:
        raise SVGValidationError('Unsupported SVG dimensions')
    return float(match[1]) * _UNITS.get((match[2] or 'px').lower(), 1)


def validate_svg(raw: bytes, max_pixels: int = 32_000_000) -> dict:
    """Return JSON-compatible dimensions/render settings after fail-closed checks.

    Dimensions resolve at 96 dpi; missing/percentage sizes use viewBox, then
    the SVG default viewport (300 x 150). Pixel dimensions round up.
    """
    if not isinstance(raw, bytes) or not raw or len(raw) > MAX_BYTES:
        raise SVGValidationError('SVG byte limit exceeded or invalid byte input')
    if not isinstance(max_pixels, int) or max_pixels <= 0:
        raise SVGValidationError('max_pixels must be a positive integer')
    # Decode explicitly so UTF-16 cannot hide processing instructions/declarations.
    try:
        source = raw.decode('utf-8-sig')
    except UnicodeError as exc:
        raise SVGValidationError('Only UTF-8 SVG is supported') from exc
    if re.search(r'<!\s*(?:DOCTYPE|ENTITY)|<\?(?!xml\s)', source, re.I):
        raise SVGValidationError('DTD, entities and processing instructions are forbidden')
    try:
        root = ElementTree.fromstring(raw, forbid_dtd=True, forbid_entities=True, forbid_external=True)
    except Exception as exc:
        raise SVGValidationError('Invalid or unsafe XML') from exc
    if root.tag not in ('svg', f'{{{SVG_NS}}}svg'):
        raise SVGValidationError('Root element must be SVG')
    nodes = list(root.iter())
    if len(nodes) > MAX_NODES:
        raise SVGValidationError('SVG node limit exceeded')
    ids = {}
    duplicate_ids = set()
    references = defaultdict(list)
    path_chars = 0
    embedded_images = []
    embedded_pixels = 0
    for node in nodes:
        tag = node.tag
        if tag.startswith('{'):
            namespace, tag = tag[1:].split('}', 1)
            if namespace != SVG_NS:
                raise SVGValidationError('Non-SVG namespace is unsupported')
        if tag not in _ALLOWED:
            raise SVGValidationError(f'Unsupported SVG element: {tag}')
        identifier = node.get('id')
        if identifier:
            if identifier in ids:
                duplicate_ids.add(identifier)
            else:
                ids[identifier] = node
        for key, value in node.attrib.items():
            local = key.rsplit('}', 1)[-1]
            if local.lower().startswith('on') or local in ('base', 'src'):
                raise SVGValidationError('Active content and base URLs are forbidden')
            if local == 'href':
                if tag == 'image':
                    embedded = _embedded_png(value, max_pixels)
                    embedded_pixels += embedded['pixels']
                    if embedded_pixels > max_pixels:
                        raise SVGValidationError('Aggregate embedded PNG pixel limit exceeded')
                    embedded_images.append(embedded)
                else:
                    references[node].append(_fragment(value))
            elif local == 'd':
                path_chars += len(value)
            else:
                references[node].extend(_css_tokens(tinycss2.parse_component_value_list(value)))
        if tag == 'style':
            references[node].extend(_css_tokens(tinycss2.parse_component_value_list(node.text or '')))
    if path_chars > MAX_PATH_CHARS:
        raise SVGValidationError('SVG path complexity limit exceeded')
    for refs in references.values():
        if any(ref in duplicate_ids for ref in refs):
            raise SVGValidationError('Ambiguous duplicated SVG resource id')
        if any(ref not in ids for ref in refs):
            raise SVGValidationError('Unresolved local SVG reference')
    # Include both containment and reference edges. This catches references to
    # ancestors, cyclic gradients and exponential use/pattern expansion.
    memo = {}
    active = set()

    def cost(node, depth=0):
        if depth > MAX_DEPTH or node in active:
            raise SVGValidationError('SVG cycle or depth limit exceeded')
        if node in memo:
            size, height = memo[node]
            if depth + height > MAX_DEPTH:
                raise SVGValidationError('SVG depth limit exceeded')
            return size, height
        active.add(node)
        size, height = 1, 0
        for child in list(node) + [ids[ref] for ref in references[node]]:
            child_size, child_height = cost(child, depth + 1)
            size += child_size
            height = max(height, child_height + 1)
            if size > MAX_EXPANDED_NODES:
                raise SVGValidationError('Expanded SVG complexity limit exceeded')
        active.remove(node)
        memo[node] = size, height
        return size, height

    expanded_nodes, _ = cost(root)
    viewbox = root.get('viewBox')
    default_width, default_height = 300., 150.
    if viewbox:
        try:
            values = [float(v) for v in re.split(r'[\s,]+', viewbox.strip())]
            if len(values) != 4 or not all(math.isfinite(v) for v in values) or min(values[2:]) <= 0:
                raise ValueError
            default_width, default_height = values[2:]
        except ValueError as exc:
            raise SVGValidationError('Invalid SVG viewBox') from exc
    width = _dimension(root.get('width'), default_width)
    height = _dimension(root.get('height'), default_height)
    if not all(math.isfinite(v) and 0 < v <= 32768 for v in (width, height)):
        raise SVGValidationError('SVG dimensions exceed limits')
    width, height = math.ceil(width), math.ceil(height)
    if width * height > max_pixels:
        raise SVGValidationError('SVG pixel limit exceeded')
    return {
        'width': width, 'height': height, 'node_count': len(nodes), 'expanded_node_count': expanded_nodes,
        'duplicate_id_count': len(duplicate_ids), 'embedded_images': embedded_images,
        'original_dimensions': {'width': root.get('width'), 'height': root.get('height'), 'viewBox': viewbox},
        'settings': {'dpi': 96, 'background_color': '#ffffff', 'output_width': width, 'output_height': height,
                     'parent_width': width, 'parent_height': height, 'unsafe': False, 'external_resources': 'deny', 'inline_images': 'verified-base64-png-only',
                     'timeout_seconds': RENDER_TIMEOUT, 'memory_limit_bytes': RENDER_MEMORY_BYTES,
                     'cpu_limit_seconds': 8, 'max_pixels': max_pixels, 'normalization_policy': 'safe-svg-v2',
                     'validation_limits': {'max_bytes': MAX_BYTES, 'max_nodes': MAX_NODES,
                         'max_expanded_nodes': MAX_EXPANDED_NODES, 'max_depth': MAX_DEPTH,
                         'max_path_chars': MAX_PATH_CHARS, 'max_dimension': 32768}},
    }


_RENDER_SCRIPT = r'''
import base64, io, json, re, resource, sys
resource.setrlimit(resource.RLIMIT_AS, (1073741824, 1073741824))
resource.setrlimit(resource.RLIMIT_CPU, (8, 8))
import cairosvg
from cairosvg.surface import PNGSurface
settings = json.loads(sys.argv[1])
from defusedxml import ElementTree
raw = sys.stdin.buffer.read()
# This allow-map is derived only from the already validated input bytes. No URL
# implementation is delegated to CairoSVG's general-purpose resource fetcher.
inline = {}
for node in ElementTree.fromstring(raw, forbid_dtd=True, forbid_entities=True, forbid_external=True).iter():
    if node.tag.rsplit('}', 1)[-1] == 'image':
        for key, value in node.attrib.items():
            if key.rsplit('}', 1)[-1] == 'href' and value.startswith('data:image/png;base64,'):
                inline[value] = base64.b64decode(re.sub(r'[ \t\r\n]', '', value.split(',', 1)[1]), validate=True)
def deny_url(url, *args, **kwargs):
    if url in inline:
        return inline[url]
    raise ValueError('SVG external resource access denied')
png = PNGSurface.convert(bytestring=raw, url_fetcher=deny_url,
    dpi=96, background_color='#ffffff', unsafe=False,
    output_width=settings['output_width'], output_height=settings['output_height'],
    parent_width=settings['parent_width'], parent_height=settings['parent_height'])
sys.stdout.buffer.write(png)
'''


def rasterize_svg(raw: bytes, max_pixels: int = 32_000_000) -> tuple[bytes, dict]:
    """Validate then render in a child with CPU/memory/time bounds; return PNG, metadata.

    Requires Linux resource limits and installed CairoSVG. Network/file/data URL
    fetching is denied independently of validation by the renderer's URL fetcher.
    Verified inline PNGs are supplied from an exact-match, in-memory allow-map.
    """
    import json
    from importlib.metadata import version

    metadata = validate_svg(raw, max_pixels=max_pixels)
    try:
        result = subprocess.run([sys.executable, '-c', _RENDER_SCRIPT, json.dumps(metadata['settings'])],
                                input=raw, capture_output=True, timeout=RENDER_TIMEOUT, check=True)
    except (OSError, subprocess.SubprocessError) as exc:
        raise SVGValidationError('SVG renderer failed or exceeded resource limits') from exc
    if not result.stdout.startswith(b'\x89PNG\r\n\x1a\n'):
        raise SVGValidationError('SVG renderer did not return PNG')
    metadata.update({'renderer': 'cairosvg', 'renderer_version': version('CairoSVG'), 'media_type': 'image/png'})
    return result.stdout, metadata
