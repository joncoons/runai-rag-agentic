"""Real PostgreSQL intake/run atomicity using generated public page/image/CSV bytes.

Only the dedicated loopback :55443 qualification database may be reset. Remote
acquisition and extraction/embedding services are never contacted.
"""
from dataclasses import asdict
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import unittest
from unittest.mock import AsyncMock, patch
from urllib.parse import urlsplit
from uuid import UUID


def load_asset_fixture():
    path = Path(__file__).with_name('test_web_asset_intake.py')
    spec = importlib.util.spec_from_file_location('refresh_intake_asset_fixture', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.WebAssetIntakeTests


@unittest.skipUnless(os.environ.get('SOURCE_REFRESH_INTAKE_TEST_DSN'),
                     'requires dedicated source refresh intake PostgreSQL')
class SourceRefreshIntakeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        dsn = os.environ['SOURCE_REFRESH_INTAKE_TEST_DSN']
        target = urlsplit(dsn)
        self.assertIn(target.hostname, ('localhost', '127.0.0.1'))
        self.assertEqual(target.port, 55443)
        self.assertEqual((target.path, target.username), ('/qualification', 'qualification'))
        # Reuse setup and generated captures without inheriting its test methods.
        self.fixture = load_asset_fixture()
        with patch.dict(os.environ, {'COLLECTION_TEST_DSN': dsn}):
            await self.fixture.asyncSetUp(self)
        await self.pool.execute(Path('deployment/application/004_source_rechecks.sql').read_text())
        await self.pool.execute(Path('deployment/application/009_streamed_website_runs.sql').read_text())
        from application.source_registry import SourceRegistry
        self.registry = SourceRegistry(self.pool)
        self.capture, self.policy, self.assets = self.fixture.capture(self)
        self.source = await self.registry.create('collection', name='Generated refresh intake',
            seed_urls=[page.requested_uri for page in self.capture.pages],
            policy=asdict(self.policy), schedule=None)
        await self.registry.request(self.source['source_id'], 'collection')
        self.run = await self.registry.claim('refresh-intake-qualification')
        self.assertIsNotNone(self.run)
        observations = [{'uri': page.final_uri, 'raw_sha256': page.raw_sha256, 'state': 'present'}
                        for page in self.capture.pages]
        observations += [{'uri': asset.requested_uri, 'raw_sha256': asset.raw_sha256,
                          'parent_uri': asset.parent_url, 'state': 'present'} for asset in self.assets]
        raw = json.dumps(observations, sort_keys=True).encode()
        manifest = self.h.raw_root / 'generated-refresh-observations.json'
        manifest.write_bytes(raw)
        self.refresh = dict(run_id=self.run['run_id'], fence=self.run['fence'],
            fingerprint=hashlib.sha256(raw).hexdigest(), manifest_uri=manifest.as_uri(),
            manifest_sha256=hashlib.sha256(raw).hexdigest(), observations=observations,
            counts={'pages': 2, 'assets': 2})

    async def ledger(self):
        source = await self.pool.fetchrow('SELECT * FROM application.website_sources WHERE source_id=$1',
                                          self.source['source_id'])
        run = await self.pool.fetchrow('SELECT * FROM application.website_source_runs WHERE run_id=$1',
                                       self.run['run_id'])
        return dict(source), dict(run)

    async def assert_rolled_back(self, before):
        await self.fixture.assert_no_acceptance(self)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.events'), 0)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.sources'), 0)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.website_sources'), 1)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.website_source_runs'), 1)
        self.assertEqual(await self.ledger(), before)

    async def submit(self):
        from application.intake import submit_web
        with patch('application.web_asset_acquisition.acquire_assets',
                   AsyncMock(side_effect=AssertionError('Preacquired assets must not be fetched twice'))) as acquire:
            result = await submit_web(self.context, 'collection', self.capture, self.policy,
                                      _acquired_assets=self.assets, _refresh=self.refresh)
        acquire.assert_not_awaited()
        return UUID(result)

    async def test_run_job_group_and_outbox_commit_together_without_reacquisition(self):
        from application.source_registry import SourceRegistry
        original = SourceRegistry.accept
        calls = []
        async def inspect_accept(registry, connection, **kwargs):
            # All child work already exists on the intake transaction connection.
            self.assertEqual(await connection.fetchval('SELECT count(*) FROM public.jobs'), 1)
            self.assertEqual(await connection.fetchval('SELECT count(*) FROM collection_control.web_groups'), 1)
            self.assertEqual(await connection.fetchval('SELECT count(*) FROM collection_control.web_group_members'), 4)
            self.assertEqual(await connection.fetchval('SELECT count(*) FROM public.outbox_events'), 4)
            # Independent readers cannot see a partial intake before run acceptance.
            self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM public.jobs'), 0)
            self.assertEqual((await self.ledger())[1]['state'], 'scanning')
            await original(registry, connection, **kwargs)
            calls.append(kwargs)
        with patch.object(SourceRegistry, 'accept', inspect_accept):
            job = await self.submit()
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0], dict(self.refresh, job_id=job))
        source, run = await self.ledger()
        self.assertEqual((run['state'], run['job_id']), ('accepted', job))
        self.assertEqual((run['fingerprint'], run['manifest_uri'], run['manifest_sha256']),
                         tuple(self.refresh[key] for key in ('fingerprint', 'manifest_uri', 'manifest_sha256')))
        self.assertEqual(json.loads(run['observations']), self.refresh['observations'])
        self.assertEqual(json.loads(run['counts']), self.refresh['counts'])
        self.assertIsNone(source['last_successful_run_id'])
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM public.jobs WHERE job_id=$1', job), 1)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.jobs WHERE job_id=$1', job), 1)
        group = await self.pool.fetchrow('SELECT * FROM collection_control.web_groups WHERE job_id=$1', job)
        self.assertEqual(group['state'], 'pending')
        members = await self.pool.fetch('SELECT * FROM collection_control.web_group_members WHERE group_id=$1', group['group_id'])
        revisions = await self.pool.fetch('SELECT * FROM application.source_revisions WHERE job_id=$1', job)
        self.assertEqual(sorted(row['web_role'] for row in revisions), ['binary', 'image', 'page', 'page'])
        self.assertEqual({row['document_id'] for row in members}, {row['document_id'] for row in revisions})
        stages = await self.pool.fetch('SELECT * FROM public.stage_units WHERE job_id=$1', job)
        outbox = await self.pool.fetch('SELECT * FROM public.outbox_events')
        self.assertEqual((len(stages), len(outbox)), (4, 4))
        self.assertEqual({row['idempotency_key'] for row in stages}, {row['idempotency_key'] for row in outbox})
        self.assertTrue(all(row['target_stream']=='rag.agentic.extraction.pending' and row['published_at'] is None
                            for row in outbox))
        self.assertEqual(await self.pool.fetchval("SELECT count(*) FROM application.events WHERE job_id=$1 AND event_type='intake_committed'", job), 1)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'), 0)

    async def test_error_after_acceptance_sql_rolls_back_run_and_all_intake_rows(self):
        from application.source_registry import SourceRegistry
        original = SourceRegistry.accept
        before = await self.ledger()
        async def fail_after_accept(registry, connection, **kwargs):
            await original(registry, connection, **kwargs)
            self.assertEqual(await connection.fetchval('SELECT state FROM application.website_source_runs'), 'accepted')
            self.assertEqual(await connection.fetchval('SELECT count(*) FROM collection_control.web_groups'), 1)
            self.assertEqual(await connection.fetchval('SELECT count(*) FROM public.outbox_events'), 4)
            raise RuntimeError('generated late acceptance failure')
        with patch.object(SourceRegistry, 'accept', fail_after_accept):
            with self.assertRaisesRegex(RuntimeError, 'generated late acceptance failure'):
                await self.submit()
        await self.assert_rolled_back(before)
        # The same lease remains usable after a transaction rollback.
        job = await self.submit()
        self.assertEqual((await self.ledger())[1]['job_id'], job)

    async def test_edited_source_cannot_accept_new_intake(self):
        from collection_control.repository import Fenced
        await self.registry.update(self.source['source_id'], 'collection', paused=True)
        before = await self.ledger()
        with self.assertRaises(Fenced):
            await self.submit()
        await self.assert_rolled_back(before)

    async def test_old_fence_cannot_accept_after_lease_takeover(self):
        from collection_control.repository import Fenced
        await self.pool.execute("UPDATE application.website_source_runs SET lease_until=clock_timestamp()-interval '1 second' WHERE run_id=$1", self.run['run_id'])
        successor = await self.registry.claim('refresh-intake-takeover')
        self.assertEqual(successor['run_id'], self.run['run_id'])
        self.assertGreater(successor['fence'], self.run['fence'])
        before = await self.ledger()
        with self.assertRaises(Fenced):
            await self.submit()
        await self.assert_rolled_back(before)
        self.refresh['fence'] = successor['fence']
        job = await self.submit()
        self.assertEqual((await self.ledger())[1]['job_id'], job)

    async def test_expired_lease_cannot_accept_new_intake(self):
        from collection_control.repository import Fenced
        await self.pool.execute("UPDATE application.website_source_runs SET lease_until=clock_timestamp()-interval '1 second' WHERE run_id=$1", self.run['run_id'])
        before = await self.ledger()
        with self.assertRaises(Fenced):
            await self.submit()
        await self.assert_rolled_back(before)

    async def test_source_version_mismatch_rejects_even_with_live_run_lease(self):
        from collection_control.repository import Fenced
        # Isolate the version check from update()'s additional run cancellation.
        await self.pool.execute('UPDATE application.website_sources SET version=version+1 WHERE source_id=$1', self.source['source_id'])
        before = await self.ledger()
        self.assertEqual(before[1]['state'], 'scanning')
        with self.assertRaises(Fenced):
            await self.submit()
        await self.assert_rolled_back(before)

    async def test_accepted_run_cannot_acknowledge_a_second_job(self):
        from collection_control.repository import Fenced
        job = await self.submit()
        before = await self.ledger()
        tables = ('public.jobs', 'application.jobs', 'public.documents', 'application.sources',
                  'application.source_revisions', 'application.events', 'public.stage_units',
                  'public.outbox_events', 'collection_control.web_groups',
                  'collection_control.web_group_members', 'collection_control.operations')
        counts = {table: await self.pool.fetchval('SELECT count(*) FROM '+table) for table in tables}
        with self.assertRaises(Fenced):
            await self.submit()
        self.assertEqual(await self.ledger(), before)
        self.assertEqual((await self.ledger())[1]['job_id'], job)
        for table, count in counts.items():
            self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM '+table), count, table)
