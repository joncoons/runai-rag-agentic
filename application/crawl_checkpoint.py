"""Fenced, immutable acquisition checkpoints; the existing PG run owns the pointer."""
from __future__ import annotations

import asyncio
from dataclasses import asdict
import hashlib
import json
from pathlib import Path

from collection_control.artifacts import canonical
from collection_control.provenance import verified_file
from collection_control.repository import Conflict
from nv_ingest_durable.deferred_caption import retain

from .web_acquisition import Asset, PageCapture
from .web_asset_acquisition import AssetCapture

ROOT_SCHEMA = 'queued-crawl-checkpoint.v1'


def _decoded(value):
    return json.loads(value) if isinstance(value, str) else value


class CrawlCheckpoint:
    """Cache only successful full-byte captures within one immutable run spec.

    Artifacts may precede a transaction commit; unreferenced files confer no
    authority. Every read and write consults the current fenced PG row. No job,
    publication receipt, or deletion decision is made here.
    """
    def __init__(self, context, registry, run):
        self.context, self.registry = context, registry
        self.run = dict(run)
        self.raw_root = context.settings.raw
        self.lock = asyncio.Lock()
        self._reference = None
        self.identity = self._identity(self.run)
        if self.run.get('manifest_uri'):
            self._root(self.run)

    @staticmethod
    def _identity(run):
        return {'run_id': str(run['run_id']), 'source_id': str(run['source_id']),
                'collection_id': run['collection_id'],
                'spec_sha256': hashlib.sha256(canonical(_decoded(run['spec']))).hexdigest()}

    @property
    def reference(self):
        """Latest verified checkpoint reference, retained for the final observation."""
        return dict(self._reference) if self._reference else None

    def _read(self, reference):
        try:
            raw = verified_file(self.raw_root, reference['uri'], reference['sha256'])
            result = json.loads(raw)
            if not isinstance(result, dict): raise ValueError('Expected manifest object')
            return result
        except (KeyError, TypeError, ValueError, UnicodeError) as error:
            raise Conflict('Invalid retained crawl checkpoint') from error

    def _root(self, run):
        if self._identity(run) != self.identity:
            raise Conflict('Crawl checkpoint run/spec identity differs')
        empty = {'schema_version': ROOT_SCHEMA, **self.identity, 'coverage': 'in_progress', 'pages': {}, 'assets': {}}
        if not run.get('manifest_uri'):
            self._reference = None
            return empty
        reference = {'uri': run['manifest_uri'], 'sha256': run['manifest_sha256']}
        root = self._read(reference)
        if root.get('schema_version') != ROOT_SCHEMA:
            self._reference = None
            return empty
        if any(root.get(key) != value for key, value in self.identity.items()):
            raise Conflict('Retained crawl checkpoint identity differs')
        if root.get('coverage') != 'in_progress' or any(not isinstance(root.get(key), dict) for key in ('pages', 'assets')):
            raise Conflict('Invalid crawl checkpoint root schema')
        self._reference = reference
        return root

    async def _leased(self, connection):
        _, run, _ = await self.registry._leased(connection, self.run['run_id'], self.run['fence'], require_deadline=True)
        return run

    async def get_page(self, url):
        return await self._get(url, 'pages')

    async def get_asset(self, url):
        return await self._get(url, 'assets')

    async def _get(self, url, group):
        async with self.lock:
            async with self.context.pool.acquire() as connection, connection.transaction():
                root = self._root(await self._leased(connection))
                reference = root[group].get(url)
                if reference is None: return None
                item = self._read(reference)
                expected = 'queued-crawl-page.v1' if group == 'pages' else 'queued-crawl-asset.v1'
                if item.get('schema_version') != expected or item.get('requested_uri') != url:
                    raise Conflict('Invalid crawl checkpoint item schema or URI')
                try:
                    metadata = dict(item['capture'])
                    if metadata['requested_uri'] != url: raise ValueError('Capture URI differs')
                    checksum = metadata['raw_sha256'].removeprefix('sha256:')
                    metadata['raw_bytes'] = verified_file(self.raw_root, item['raw_uri'], checksum)
                    if group == 'pages':
                        metadata['links'] = tuple(metadata['links'])
                        metadata['assets'] = tuple(Asset(**asset) for asset in metadata['assets'])
                        rendering=metadata['evidence'].get('rendering')
                        if rendering:
                            metadata['http_raw_bytes']=verified_file(self.raw_root,rendering['http_html_artifact_uri'],rendering['http_html_sha256'].removeprefix('sha256:'))
                        return PageCapture(**metadata)
                    return AssetCapture(**metadata)
                except (KeyError, TypeError, ValueError) as error:
                    raise Conflict('Invalid crawl checkpoint capture metadata') from error

    async def put_page(self, url, capture: PageCapture):
        await self._put(url, capture, 'pages', '.html')

    async def put_asset(self, url, capture: AssetCapture):
        await self._put(url, capture, 'assets', Path(capture.filename).suffix.lower())

    async def _put(self, url, capture, group, suffix):
        if capture.requested_uri != url:
            raise Conflict('Checkpoint key differs from captured requested URI')
        checksum = hashlib.sha256(capture.raw_bytes).hexdigest()
        if capture.raw_sha256.removeprefix('sha256:') != checksum:
            raise Conflict('Captured original byte checksum differs')
        async with self.lock:
            # Fencing and PG locks precede any new pointer/retention decision.
            async with self.context.pool.acquire() as connection, connection.transaction():
                run = await self._leased(connection)
                root = self._root(run)
                raw_uri, raw_sha = retain(self.raw_root, capture.raw_bytes, suffix)
                metadata = asdict(capture); metadata.pop('raw_bytes')
                if group == 'pages':
                    metadata.pop('http_raw_bytes', None)
                    from .intake import acquisition_evidence
                    metadata['evidence']=acquisition_evidence(capture,self.raw_root)
                item = {'schema_version': 'queued-crawl-page.v1' if group == 'pages' else 'queued-crawl-asset.v1',
                        'requested_uri': url, 'raw_uri': raw_uri, 'capture': metadata}
                item_uri, item_sha = retain(self.raw_root, canonical(item), '.crawl-item.json')
                root[group][url] = {'uri': item_uri, 'sha256': item_sha}
                root_uri, root_sha = retain(self.raw_root, canonical(root), '.crawl-checkpoint.json')
                counts = {'pages': len(root['pages']), 'assets': len(root['assets'])}
                # Compact evidence only; normalized text/raw bodies stay in artifacts.
                observations = _decoded(run.get('observations') or []) if self._reference else []
                kind = 'page' if group == 'pages' else capture.kind
                observations = [entry for entry in observations
                                if (entry.get('kind'), entry.get('requested_uri')) != (kind, url)]
                observations.append({'kind': kind, 'requested_uri': url, 'canonical_uri': capture.final_uri,
                                     'raw_uri': raw_uri, 'raw_sha256': raw_sha, 'status': 'captured'})
                await connection.execute('''UPDATE application.website_source_runs
                    SET manifest_uri=$2,manifest_sha256=$3,counts=$4::jsonb,observations=$5::jsonb,
                        updated_at=clock_timestamp() WHERE run_id=$1''',
                    run['run_id'], root_uri, root_sha, json.dumps(counts), json.dumps(observations))
            # Publish the local view only after PG commit succeeds.
            self._reference = {'uri': root_uri, 'sha256': root_sha}
