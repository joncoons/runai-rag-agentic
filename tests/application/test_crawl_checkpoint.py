"""Immutable capture checkpoints with PostgreSQL lease authority; no network/models."""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from urllib.parse import unquote, urlsplit
from uuid import uuid4

from application.web_acquisition import Asset, PageCapture
from application.web_asset_acquisition import AssetCapture
from collection_control.repository import Fenced, Conflict


def page(url='https://qualification.invalid/page'):
    raw = b'<html><p>Original \xe9 byte</p></html>'
    return PageCapture(url, url + '/canonical', raw, 'sha256:' + hashlib.sha256(raw).hexdigest(),
        'Original normalized text', 'Exact title', ('https://qualification.invalid/next?x=1',),
        (Asset('https://qualification.invalid/chart.png', 'image', url + '/canonical', 'Caption', 'img[2]'),),
        {'http_status': 200, 'etag': 'original', 'redirect_chain': [{'uri': url, 'status': 301}]})


def asset():
    raw = b'original-native-png-bytes'
    return AssetCapture('https://qualification.invalid/chart.png', 'https://qualification.invalid/chart.png',
        raw, 'sha256:' + hashlib.sha256(raw).hexdigest(), 'image/png', 'chart.png',
        {'http_status': 200, 'byte_size': len(raw)}, 'https://qualification.invalid/page/canonical', 'image', 'img[2]', 'Caption')


class MemoryPool:
    def __init__(self, row): self.row = row
    @asynccontextmanager
    async def acquire(self): yield self
    @asynccontextmanager
    async def transaction(self): yield self
    async def execute(self, query, run_id, uri, sha, counts, observations):
        self.row.update(manifest_uri=uri, manifest_sha256=sha, counts=json.loads(counts), observations=json.loads(observations))


class MemoryRegistry:
    async def _leased(self, c, run_id, fence, *, require_deadline):
        if fence != c.row['fence']: raise Fenced('stale qualification lease')
        return {}, dict(c.row), None


class CaptureCheckpointTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(importlib.util.find_spec('application.crawl_checkpoint'), 'Capture checkpoints need a durable adapter')
        from application.crawl_checkpoint import CrawlCheckpoint
        self.adapter = CrawlCheckpoint
        self.temporary = tempfile.TemporaryDirectory(); self.addCleanup(self.temporary.cleanup)
        self.run = {'run_id': uuid4(), 'source_id': uuid4(), 'collection_id': 'collection', 'fence': 1,
                    'spec': {'seed_urls': ['https://qualification.invalid/page'], 'policy': {'max_depth': 3}},
                    'manifest_uri': None, 'manifest_sha256': None, 'counts': {}, 'observations': []}
        self.pool = MemoryPool(dict(self.run)); self.registry = MemoryRegistry()
        self.context = SimpleNamespace(pool=self.pool, settings=SimpleNamespace(raw=Path(self.temporary.name)))

    def checkpoint(self, run=None): return self.adapter(self.context, self.registry, run or self.run)

    async def test_restart_restores_exact_page_and_asset_without_refetch(self):
        original_page, original_asset = page(), asset()
        checkpoint = self.checkpoint()
        self.assertIsNone(checkpoint.reference)
        await checkpoint.put_page(original_page.requested_uri, original_page)
        await checkpoint.put_asset(original_asset.requested_uri, original_asset)
        restarted = self.checkpoint(dict(self.pool.row))
        self.assertEqual(await restarted.get_page(original_page.requested_uri), original_page)
        self.assertEqual(await restarted.get_asset(original_asset.requested_uri), original_asset)
        self.assertEqual(restarted.reference, checkpoint.reference)
        self.assertEqual(self.pool.row['counts']['pages'], 1)
        self.assertEqual(self.pool.row['counts']['assets'], 1)
        self.assertNotIn(original_page.text, json.dumps(self.pool.row['observations']))

    async def test_concurrent_adapter_instances_merge_authoritative_pointer(self):
        first, second = self.checkpoint(), self.checkpoint()
        await first.put_page(page().requested_uri, page())
        await second.put_asset(asset().requested_uri, asset())
        self.assertEqual(await first.get_asset(asset().requested_uri), asset())
        self.assertEqual(await second.get_page(page().requested_uri), page())

    async def test_one_adapter_serializes_concurrent_capture_writes(self):
        checkpoint = self.checkpoint()
        await asyncio.gather(checkpoint.put_page(page().requested_uri, page()),
                             checkpoint.put_asset(asset().requested_uri, asset()))
        self.assertEqual(await checkpoint.get_page(page().requested_uri), page())
        self.assertEqual(await checkpoint.get_asset(asset().requested_uri), asset())
        self.assertEqual(self.pool.row['counts'], {'pages': 1, 'assets': 1})

    async def test_stale_lease_cannot_advance_pointer(self):
        checkpoint = self.checkpoint(); await checkpoint.put_page(page().requested_uri, page())
        before = dict(self.pool.row); self.pool.row['fence'] = 2
        with self.assertRaises(Fenced): await checkpoint.put_asset(asset().requested_uri, asset())
        self.assertEqual(self.pool.row['manifest_uri'], before['manifest_uri'])
        self.assertEqual(self.pool.row['manifest_sha256'], before['manifest_sha256'])

    async def test_tampered_root_and_raw_are_rejected(self):
        checkpoint = self.checkpoint(); await checkpoint.put_page(page().requested_uri, page())
        reference = checkpoint.reference
        root_file = Path(unquote(urlsplit(reference['uri']).path)); original = root_file.read_bytes()
        root_file.write_bytes(original + b' ')
        with self.assertRaises(Conflict): await checkpoint.get_page(page().requested_uri)
        root_file.write_bytes(original)
        root = json.loads(original); item_ref = root['pages'][page().requested_uri]
        item = json.loads(Path(unquote(urlsplit(item_ref['uri']).path)).read_bytes())
        Path(unquote(urlsplit(item['raw_uri']).path)).write_bytes(b'tampered')
        with self.assertRaises(Conflict): await checkpoint.get_page(page().requested_uri)

    async def test_forged_capture_hash_never_updates_pg(self):
        with self.assertRaises((ValueError, Conflict)):
            await self.checkpoint().put_page(page().requested_uri, replace(page(), raw_sha256='sha256:'+'0'*64))
        self.assertIsNone(self.pool.row['manifest_uri'])

    async def test_checkpoint_identity_rejects_changed_spec(self):
        checkpoint = self.checkpoint(); await checkpoint.put_page(page().requested_uri, page())
        changed = dict(self.pool.row); changed['spec'] = {'seed_urls': ['https://qualification.invalid/other']}
        with self.assertRaises(Conflict): await self.checkpoint(changed).get_page(page().requested_uri)

    async def test_other_manifest_is_fresh_and_cache_miss_stays_missing(self):
        from nv_ingest_durable.deferred_caption import retain
        uri, sha = retain(self.context.settings.raw, b'{"schema_version":"website-observation.v1"}', '.json')
        self.pool.row.update(manifest_uri=uri, manifest_sha256=sha)
        checkpoint = self.checkpoint(dict(self.pool.row))
        self.assertIsNone(await checkpoint.get_page(page().requested_uri))
        self.assertIsNone(checkpoint.reference)
        self.assertEqual(self.pool.row['manifest_uri'], uri)


@unittest.skipUnless(os.environ.get('SOURCE_RECHECK_TEST_DSN'), 'requires isolated source refresh PostgreSQL')
class CaptureCheckpointPostgresTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Composition avoids inheriting or rediscovering RegistryTests methods.
        spec = importlib.util.spec_from_file_location('checkpoint_registry_fixture', Path(__file__).with_name('test_source_registry.py'))
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        self.fixture = module.RegistryTests(methodName='runTest')
        await self.fixture.asyncSetUp(); self.addAsyncCleanup(self.fixture.pool.close)
        self.source, self.run = await self.fixture.claimed()
        self.pool, self.registry = self.fixture.pool, self.fixture.registry
        self.temporary = tempfile.TemporaryDirectory(); self.addCleanup(self.temporary.cleanup)
        self.context = SimpleNamespace(pool=self.pool, settings=SimpleNamespace(raw=Path(self.temporary.name)))
        from application.crawl_checkpoint import CrawlCheckpoint
        self.adapter = CrawlCheckpoint

    async def test_takeover_restores_captures_and_rejects_old_fence(self):
        checkpoint = self.adapter(self.context, self.registry, self.run)
        await checkpoint.put_page(page().requested_uri, page())
        await checkpoint.put_asset(asset().requested_uri, asset())
        await self.pool.execute("UPDATE application.website_source_runs SET lease_until=clock_timestamp()-interval '1 second' WHERE run_id=$1", self.run['run_id'])
        reclaimed = await self.registry.claim('checkpoint-reclaimer')
        resumed = self.adapter(self.context, self.registry, reclaimed)
        self.assertEqual(await resumed.get_page(page().requested_uri), page())
        self.assertEqual(await resumed.get_asset(asset().requested_uri), asset())
        before = resumed.reference
        with self.assertRaises(Fenced): await checkpoint.put_page(page().requested_uri, page())
        row = await self.pool.fetchrow('SELECT * FROM application.website_source_runs WHERE run_id=$1', self.run['run_id'])
        self.assertEqual(row['manifest_uri'], before['uri'])
        self.assertEqual(row['manifest_sha256'], before['sha256'])
        self.assertEqual(row['state'], 'scanning')
        self.assertIsNone(row['job_id'])

    async def test_pg_serializes_independent_checkpoint_instances(self):
        first = self.adapter(self.context, self.registry, self.run)
        second = self.adapter(self.context, self.registry, self.run)
        await asyncio.gather(first.put_page(page().requested_uri, page()),
                             second.put_asset(asset().requested_uri, asset()))
        self.assertEqual(await first.get_asset(asset().requested_uri), asset())
        self.assertEqual(await second.get_page(page().requested_uri), page())
        counts = await self.pool.fetchval('SELECT counts FROM application.website_source_runs WHERE run_id=$1', self.run['run_id'])
        self.assertEqual(json.loads(counts), {'pages': 1, 'assets': 1})

    async def test_pg_authority_rejects_tampered_immutable_payload(self):
        checkpoint = self.adapter(self.context, self.registry, self.run)
        await checkpoint.put_page(page().requested_uri, page())
        path = Path(unquote(urlsplit(checkpoint.reference['uri']).path)); path.write_bytes(b'{}')
        with self.assertRaises(Conflict): await checkpoint.get_page(page().requested_uri)


if __name__ == '__main__': unittest.main()
