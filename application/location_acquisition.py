"""Bounded, full-byte location observations, without persistence or deletion.

Operator input alone configures RootRegistry; browser input selects a registered
alias and relative URI. HTTP(S) acquires one document via the existing hardened
asset transport. No cloud credentials, models, databases or indexes are used.
"""
from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import PurePosixPath
import re
import stat
from typing import Mapping
from urllib.parse import quote, unquote, urlsplit

from application.web_acquisition import AcquisitionPolicy, Asset
from application.web_asset_acquisition import _TYPES, _validate_body, acquire_asset

SUPPORTED_EXTENSIONS = frozenset(('.pdf', '.docx', '.pptx', '.csv', '.xls', '.xlsx', '.md', '.markdown', '.txt'))
SPEC_KEYS = frozenset(('uri', 'recursive', 'max_depth', 'max_files', 'max_entries', 'max_file_bytes', 'max_total_bytes', 'allowed_hosts'))
_ALIAS = re.compile(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z')


@dataclass(frozen=True)
class LocationLimits:
    max_depth: int = 16
    max_files: int = 1000
    max_entries: int = 10000
    max_file_bytes: int = 32 * 1024 * 1024
    max_total_bytes: int = 256 * 1024 * 1024

    def __post_init__(self):
        for name, value in asdict(self).items():
            if isinstance(value, bool) or not isinstance(value, int) or value < (0 if name == 'max_depth' else 1):
                raise ValueError('Invalid location acquisition bounds')


@dataclass(frozen=True)
class LocationFile:
    canonical_uri: str
    filename: str
    raw_bytes: bytes
    raw_sha256: str
    media_type: str
    evidence: dict


@dataclass(frozen=True)
class LocationCapture:
    files: tuple[LocationFile, ...]
    complete: bool
    errors: tuple[dict, ...]
    inventory_manifest: dict


@dataclass(frozen=True)
class RootRegistry:
    """Private operator configuration. Never serialize paths into public evidence."""
    roots: Mapping[str, str]
    identities: Mapping[str, tuple[int, int]]
    map_digest: str


def _identity(info):
    return info.st_dev, info.st_ino


def _stable(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _proof(info):
    return dict(zip(('device', 'inode', 'size', 'mtime_ns', 'ctime_ns'), _stable(info)))


def _open_absolute(path):
    """Open every absolute component without following any symlink."""
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for name in path.split('/')[1:]:
            if not name:
                continue
            child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def read_root_registry(raw_json: str | Mapping[str, str]) -> RootRegistry:
    """Validate an operator JSON alias→approved absolute directory mapping.

    The operator chooses approved read-only mount paths; callers cannot create
    registry entries through capture_location's source specification. Digest
    replay requires this same private operator configuration and inode identity.
    """
    try:
        config = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
        if not isinstance(config, Mapping):
            raise ValueError('Root registry must be an object')
        roots, identities = {}, {}
        for alias, path in config.items():
            if not isinstance(alias, str) or not _ALIAS.fullmatch(alias):
                raise ValueError('Invalid registered root alias')
            if not isinstance(path, str) or not path.startswith('/') or '\\' in path or '\0' in path:
                raise ValueError('Registered roots require absolute operator paths')
            if any(part in ('.', '..') for part in path.split('/')):
                raise ValueError('Noncanonical registered root path')
            path = '/' + '/'.join(p for p in path.split('/') if p)
            fd = _open_absolute(path)
            try:
                identities[alias] = _identity(os.fstat(fd))
            finally:
                os.close(fd)
            roots[alias] = path
        payload = {alias: {'path': roots[alias], 'device': identities[alias][0], 'inode': identities[alias][1]} for alias in sorted(roots)}
        digest = 'sha256:' + hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        from types import MappingProxyType
        return RootRegistry(MappingProxyType(roots), MappingProxyType(identities), digest)
    except (OSError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError('Invalid or inaccessible operator root registry') from exc


def _parse_spec(spec, limits):
    if isinstance(spec, str):
        spec = {'uri': spec}
    if not isinstance(spec, Mapping) or set(spec) - SPEC_KEYS:
        raise ValueError('Unsupported location specification fields')
    uri = spec.get('uri')
    if not isinstance(uri, str) or not uri or any(ord(c) < 32 or ord(c) == 127 for c in uri) or '\\' in uri:
        raise ValueError('Invalid location URI')
    recursive = spec.get('recursive', True)
    if not isinstance(recursive, bool):
        raise ValueError('recursive must be boolean')
    selected = replace(limits, **{key: spec[key] for key in asdict(limits) if key in spec})
    # Source options may tighten the server ceiling, never raise it.
    if any(getattr(selected, key) > value for key, value in asdict(limits).items()):
        raise ValueError('Source bounds exceed operator limits')
    return dict(spec), uri, recursive, selected


def _location_parts(uri):
    parts = urlsplit(uri)
    if parts.scheme != 'location' or not _ALIAS.fullmatch(parts.netloc) or parts.query or parts.fragment:
        raise ValueError('Invalid registered location URI')
    names = []
    for encoded in parts.path.split('/'):
        if not encoded:
            continue
        if re.search(r'%(?![0-9a-fA-F]{2})', encoded):
            raise ValueError('Invalid location path encoding')
        name = unquote(encoded, errors='strict')
        if name in ('.', '..') or '/' in name or '\\' in name or any(ord(c) < 32 or ord(c) == 127 for c in name):
            raise ValueError('Location path escapes registered root')
        names.append(name)
    return parts.netloc, tuple(names)


def _canonical(alias, names):
    return 'location://' + alias + '/' + '/'.join(quote(n, safe='') for n in names)


def _directory_capture(uri, registry, recursive, limits):
    alias, names = _location_parts(uri)
    if alias not in registry.roots:
        raise ValueError('Unregistered location alias')
    manifest = {'version': 'location-inventory-v1', 'uri': _canonical(alias, names), 'root_alias': alias,
                'root_map_digest': registry.map_digest, 'root_identity': dict(zip(('device', 'inode'), registry.identities[alias])),
                'limits': asdict(limits), 'recursive': recursive, 'entries': [], 'excluded': [],
                'retrieved_at': datetime.now(timezone.utc).isoformat(), 'missing_means_deleted': False}
    files, errors, opened, links = [], [], [], []
    counts = {'entries': 0, 'files': 0, 'bytes': 0}
    def error(locator, reason):
        errors.append({'uri': locator, 'error': reason, 'observation': 'unavailable', 'missing_means_deleted': False})
    def read_file(parent, name, relative):
        locator = _canonical(alias, relative)
        extension = PurePosixPath(name).suffix.lower()
        if extension not in SUPPORTED_EXTENSIONS:
            manifest['excluded'].append({'uri': locator, 'reason': 'unsupported_extension'})
            return
        counts['files'] += 1
        if counts['files'] > limits.max_files:
            raise ValueError('File count bound exceeded')
        before = os.stat(name, dir_fd=parent, follow_symlinks=False)
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        try:
            start = os.fstat(fd)
            if not stat.S_ISREG(start.st_mode) or _stable(start) != _stable(before):
                raise ValueError('File changed before stable read')
            remaining = min(limits.max_file_bytes, limits.max_total_bytes - counts['bytes'])
            if start.st_size > remaining:
                raise ValueError('File or total byte bound exceeded')
            chunks, size = [], 0
            while True:
                chunk = os.read(fd, min(65536, max(1, remaining - size + 1)))
                if not chunk:
                    break
                size += len(chunk)
                if size > remaining:
                    raise ValueError('File or total byte bound exceeded')
                chunks.append(chunk)
            after = os.fstat(fd)
            linked = os.stat(name, dir_fd=parent, follow_symlinks=False)
            if _stable(start) != _stable(after) or _stable(after) != _stable(linked) or size != start.st_size:
                raise ValueError('File changed during stable read')
            raw = b''.join(chunks)
            _validate_body(raw, extension)
            digest = 'sha256:' + hashlib.sha256(raw).hexdigest()
            evidence = {'validation': 'stable-openat-nofollow-v1', 'before': _proof(start), 'after': _proof(after),
                        'byte_size': size, 'root_map_digest': registry.map_digest, 'missing_means_deleted': False}
            files.append(LocationFile(locator, name, raw, digest, _TYPES[extension][0], evidence))
            manifest['entries'].append({'canonical_uri': locator, 'filename': name, 'raw_sha256': digest, 'byte_size': size,
                                        'media_type': _TYPES[extension][0], 'evidence': evidence})
            counts['bytes'] += size
        finally:
            os.close(fd)
    def walk(fd, relative, depth):
        before = os.fstat(fd)
        entries = []
        # Enumerate into a bounded list; never silently truncate discovery.
        with os.scandir(fd) as scan:
            for entry in scan:
                counts['entries'] += 1
                if counts['entries'] > limits.max_entries:
                    raise ValueError('Directory entry count bound exceeded')
                entries.append(entry.name)
        for name in sorted(entries):
            child_relative = relative + (name,)
            locator = _canonical(alias, child_relative)
            try:
                info = os.stat(name, dir_fd=fd, follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode):
                    raise ValueError('Symlink entries are outside registered scope')
                if stat.S_ISDIR(info.st_mode):
                    if not recursive or depth >= limits.max_depth:
                        raise ValueError('Directory depth/recursive bound excludes subtree')
                    child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                    try:
                        if _stable(info) != _stable(os.fstat(child)):
                            raise ValueError('Directory changed before enumeration')
                        walk(child, child_relative, depth + 1)
                        if _stable(os.fstat(child)) != _stable(os.stat(name, dir_fd=fd, follow_symlinks=False)):
                            raise ValueError('Directory replaced during enumeration')
                    finally:
                        os.close(child)
                elif stat.S_ISREG(info.st_mode):
                    read_file(fd, name, child_relative)
                else:
                    raise ValueError('Nonregular entry is outside supported scope')
            except (OSError, ValueError) as exc:
                error(locator, str(exc) if isinstance(exc, ValueError) else 'Entry unavailable or changed')
        if _stable(before) != _stable(os.fstat(fd)):
            raise ValueError('Directory changed during enumeration')
    try:
        root = _open_absolute(registry.roots[alias]); opened.append(root)
        if _identity(os.fstat(root)) != registry.identities[alias]:
            raise ValueError('Registered root identity changed')
        current = root
        for name in names[:-1]:
            child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
            links.append((current, name, child, _identity(os.fstat(child))))
            opened.append(child); current = child
        if names:
            info = os.stat(names[-1], dir_fd=current, follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                child = os.open(names[-1], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
                opened.append(child); links.append((current, names[-1], child, _identity(os.fstat(child))))
                walk(child, names, 0)
            else:
                read_file(current, names[-1], names)
        else:
            walk(root, (), 0)
        for parent, name, child, identity in links:
            if _identity(os.stat(name, dir_fd=parent, follow_symlinks=False)) != identity:
                raise ValueError('Location ancestor replaced during capture')
        verified_root = _open_absolute(registry.roots[alias])
        try:
            if _identity(os.fstat(verified_root)) != registry.identities[alias]:
                raise ValueError('Registered root replaced during capture')
        finally:
            os.close(verified_root)
    except (OSError, ValueError) as exc:
        error(manifest['uri'], str(exc) if isinstance(exc, ValueError) else 'Location unavailable or changed')
    finally:
        for fd in reversed(opened):
            os.close(fd)
    manifest['complete'] = not errors
    manifest['observed_bytes'] = counts['bytes']
    files.sort(key=lambda item: item.canonical_uri)
    manifest['entries'].sort(key=lambda item: item['canonical_uri'])
    return LocationCapture(tuple(files), not errors, tuple(errors), manifest)


async def capture_location(spec, registered_roots=None, *, limits=LocationLimits(), network_scope=None, session=None) -> LocationCapture:
    """Observe a registered directory/file or fetch exactly one HTTP(S) file.

    Only the trusted network_scope argument can grant private host/port scope.
    Injected sessions are trusted offline-test boundaries like acquire_asset.
    All incomplete captures retain explicit errors and never establish deletion.
    """
    try:
        selected, uri, recursive, bounds = _parse_spec(spec, limits)
        scheme = urlsplit(uri).scheme
        if scheme == 'location':
            if 'allowed_hosts' in selected:
                raise ValueError('HTTP scope is invalid for registered locations')
            registry = registered_roots if isinstance(registered_roots, RootRegistry) else await asyncio.to_thread(read_root_registry, registered_roots or {})
            return await asyncio.to_thread(_directory_capture, uri, registry, recursive, bounds)
        if scheme not in ('http', 'https'):
            raise ValueError('Unsupported location connector; use registered location URI or HTTP(S) file')
        if set(selected) & {'recursive', 'max_depth'}:
            raise ValueError('HTTP location connector acquires a single file')
        host = urlsplit(uri).hostname
        allowed = selected.get('allowed_hosts', [host])
        if not isinstance(allowed, (list, tuple)) or not allowed or any(not isinstance(h, str) for h in allowed):
            raise ValueError('Invalid HTTP allowed host selection')
        policy = network_scope or AcquisitionPolicy(allowed_hosts=tuple(allowed))
        if not {h.lower() for h in allowed}.issubset({h.lower() for h in policy.allowed_hosts}):
            raise ValueError('HTTP hosts exceed operator scope')
        private = tuple(h for h in policy.private_hosts if h.lower() in {a.lower() for a in allowed})
        policy = replace(policy, allowed_hosts=tuple(allowed), private_hosts=private,
                         max_response_bytes=min(policy.max_response_bytes, bounds.max_file_bytes, bounds.max_total_bytes),
                         included_extensions=tuple(SUPPORTED_EXTENSIONS), include_images=False)
        result = await acquire_asset(Asset(uri, 'binary', uri), policy, session=session)
        item = LocationFile(result.final_uri, result.filename, result.raw_bytes, result.raw_sha256, result.media_type, result.evidence)
        manifest = {'version': 'location-inventory-v1', 'uri': result.requested_uri, 'connector': 'http-single-file', 'complete': True,
                    'entries': [{'canonical_uri': item.canonical_uri, 'filename': item.filename, 'raw_sha256': item.raw_sha256,
                                 'byte_size': len(item.raw_bytes), 'media_type': item.media_type, 'evidence': item.evidence}],
                    'limits': asdict(bounds), 'missing_means_deleted': False}
        return LocationCapture((item,), True, (), manifest)
    except (ValueError, OSError, UnicodeError) as exc:
        # Avoid leaking an operator filesystem path from an OS exception.
        reason = str(exc) if isinstance(exc, ValueError) and not isinstance(exc, OSError) else 'Location unavailable'
        errors = ({'error': reason, 'observation': 'unavailable', 'missing_means_deleted': False},)
        return LocationCapture((), False, errors, {'version': 'location-inventory-v1', 'complete': False, 'entries': [], 'missing_means_deleted': False})
