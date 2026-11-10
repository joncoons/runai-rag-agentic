"""Website intake against a disposable qualification database; no site traffic.

The parent suite runs these sequentially with other destructive qualification
fixtures. No production database, models, Redis or VDB service is contacted.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock
from urllib.parse import unquote, urlsplit
from uuid import UUID


def decoded(value):
    return json.loads(value) if isinstance(value, str) else value


def retained_bytes(uri):
    parsed = urlsplit(uri)
    if parsed.scheme != 'file' or parsed.netloc:
        raise AssertionError('Qualification artifacts must be local files')
    return Path(unquote(parsed.path)).read_bytes()


@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'), 'requires disposable qualification database')
class WebIntakeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Import by filename: discovery with tests/application on sys.path must
        # not accidentally resolve application or tests to another package.
        fixture_path = Path(__file__).resolve().parents[1] / 'collection_control' / 'test_stage_bridge.py'
        spec = importlib.util.spec_from_file_location('web_intake_stage_bridge_fixture', fixture_path)
        fixtures = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(fixtures)
        self.h = fixtures.StageBridgeTests()
        # This validates loopback DSN and current_database/current_user are
        # qualification/qualification before migrations or TRUNCATE.
        await self.h.asyncSetUp()
        self.addAsyncCleanup(self.h.asyncTearDown)
        self.pool = self.h.pool
        for migration in (
            'deployment/ingestion/captioning/001_caption_jobs.sql',
            'deployment/collection-control/003_caption_admissions.sql',
            'deployment/application/001_application.sql',
            'deployment/application/002_typed_sources.sql',
            'deployment/application/003_web_assets.sql',
        ):
            await self.pool.execute(Path(migration).read_text())
        await self.pool.execute("INSERT INTO application.collections(collection_id,name) VALUES('collection','Offline website qualification')")
        self.writer = SimpleNamespace(run=AsyncMock(side_effect=AssertionError('Intake must not publish')))
        settings = SimpleNamespace(raw=self.h.raw_root, stages=self.h.stage_root,
            snapshots=self.h.snapshots, contract=self.h.contract,
            models={'models': {'embed': {'model': 'qualification-embedding-model'}}})
        self.context = SimpleNamespace(pool=self.pool, settings=settings, writer=self.writer)

    def capture(self):
        from application.web_acquisition import AcquisitionPolicy, Asset, CrawlCapture, PageCapture, normalize_html_text
        raw = b'<html><title>Offline guide</title><main><h1>Guide</h1><p>One complete page.</p><img src="chart.png" alt="Counts"><a href="manual.pdf">Manual</a></main></html>'
        sha = 'sha256:' + hashlib.sha256(raw).hexdigest()
        pages = []
        for suffix in ('first', 'second'):
            uri = 'https://qualification.invalid/' + suffix
            assets = ()
            pages.append(PageCapture(uri, uri, raw, sha, normalize_html_text(raw), 'Offline guide', (), assets,
                {'retrieved_at': '2026-11-08T00:00:00+00:00', 'http_status': 200, 'content_type': 'text/html',
                 'redirect_chain': [{'uri': uri, 'status': 200}], 'asset_acquisition_complete': False}))
        return CrawlCapture(tuple(pages), (), True), AcquisitionPolicy(allowed_hosts=('qualification.invalid',), max_depth=0, max_pages=2, include_images=False, included_extensions=())

    async def assert_nothing_accepted(self):
        for table in ('public.jobs', 'application.jobs', 'public.documents', 'application.source_revisions',
                      'public.stage_units', 'public.outbox_events', 'collection_control.operations'):
            self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM ' + table), 0, table)
        self.writer.run.assert_not_awaited()

    async def test_identical_html_at_distinct_urls_has_two_durable_sources_and_extraction_inputs(self):
        from application.intake import submit_web
        capture, policy = self.capture()
        job_id = UUID(await submit_web(self.context, 'collection', capture, policy))
        revisions = await self.pool.fetch('SELECT * FROM application.source_revisions WHERE job_id=$1 ORDER BY canonical_uri', job_id)
        self.assertEqual(len(revisions), 2)
        self.assertEqual(len({r['document_id'] for r in revisions}), 2)
        self.assertEqual(len({r['source_id'] for r in revisions}), 2)
        self.assertEqual({r['canonical_uri'] for r in revisions}, {p.final_uri for p in capture.pages})
        self.assertEqual(len({r['raw_sha256'] for r in revisions}), 1)
        # Content-addressed raw retention may deduplicate the bytes; source
        # identities and parent associations must remain separate.
        for revision, page in zip(revisions, capture.pages):
            self.assertEqual(revision['source_kind'], 'html')
            self.assertEqual(revision['media_type'], 'text/html')
            self.assertIsNone(revision['page_count'])
            self.assertEqual(revision['status'], 'queued')
            self.assertIsNone(revision['operation_id'])
            self.assertEqual(retained_bytes(revision['raw_uri']), page.raw_bytes)
            self.assertEqual(hashlib.sha256(page.raw_bytes).hexdigest(), revision['raw_sha256'])
            units = decoded(revision['source_units'])
            self.assertEqual(units['schema_version'], 'native-source-units.v1')
            self.assertEqual(len(units['units']), 1)
            unit = units['units'][0]
            self.assertEqual(unit['unit_id'], 'page:1')
            self.assertEqual(unit['anchors'], {'section': 'page', 'heading_path': ['page']})
            self.assertEqual(unit['text'], page.text)
            self.assertEqual(unit['status'], 'complete')
            proof = decoded(revision['acquisition'])
            proof_raw = retained_bytes(proof['uri'])
            self.assertEqual(hashlib.sha256(proof_raw).hexdigest(), proof['sha256'].removeprefix('sha256:'))
            manifest = json.loads(proof_raw)
            self.assertEqual(manifest['schema_version'], 'web-acquisition.v1')
            self.assertEqual(manifest['requested_uri'], page.requested_uri)
            self.assertEqual(manifest['final_uri'], page.final_uri)
            self.assertEqual(manifest['raw_sha256'], revision['raw_sha256'])
            self.assertEqual(manifest['policy']['allowed_hosts'], ['qualification.invalid'])
            self.assertEqual(manifest['http']['http_status'], 200)
            self.assertTrue(manifest['capture_complete'])
            self.assertFalse(manifest['deletion_authorized'])
            self.assertEqual(manifest['asset_processing'], 'native_ingestion')
            self.assertEqual(manifest['assets'], [])
            self.assertTrue(manifest['asset_acquisition_complete'])

        stages = await self.pool.fetch('SELECT * FROM public.stage_units WHERE job_id=$1', job_id)
        self.assertEqual(len(stages), 2)
        by_document = {r['document_id']: r for r in revisions}
        for stage in stages:
            self.assertEqual(stage['status'], 'pending')
            self.assertEqual(stage['stage'], 'extraction')
            envelope = decoded(stage['input_envelope'])
            self.assertEqual(envelope['document_id'], stage['document_id'])
            self.assertEqual(envelope['artifact_uri'], by_document[stage['document_id']]['raw_uri'])
            self.assertEqual(envelope['job_id'], str(job_id))
        outbox = await self.pool.fetch('SELECT * FROM public.outbox_events')
        self.assertEqual(len(outbox), 2)
        self.assertEqual({o['target_stream'] for o in outbox}, {'rag.agentic.extraction.pending'})
        self.assertEqual({o['idempotency_key'] for o in outbox}, {s['idempotency_key'] for s in stages})
        for event in outbox:
            envelope = decoded(event['envelope'])
            self.assertEqual(envelope['artifact_uri'], by_document[envelope['document_id']]['raw_uri'])
        self.assertEqual(await self.pool.fetchval('SELECT status FROM public.jobs WHERE job_id=$1', job_id), 'accepted')
        self.assertEqual(await self.pool.fetchval('SELECT status FROM application.jobs WHERE job_id=$1', job_id), 'queued')
        self.assertEqual(await self.pool.fetchval("SELECT count(*) FROM public.documents WHERE job_id=$1 AND status='accepted'", job_id), 2)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'), 0)
        self.assertIsNone(await self.pool.fetchval("SELECT current_operation FROM collection_control.collections WHERE collection_id='collection'"))
        self.writer.run.assert_not_awaited()

    async def test_partial_or_empty_scan_cannot_create_an_accepted_job(self):
        from application.intake import submit_web
        from application.web_acquisition import CrawlCapture
        capture, policy = self.capture()
        for invalid in (CrawlCapture(capture.pages, ({'uri': 'https://qualification.invalid/failed', 'error': 'unavailable'},), False),
                        CrawlCapture((), (), True)):
            with self.assertRaises(ValueError):
                await submit_web(self.context, 'collection', invalid, policy)
            await self.assert_nothing_accepted()

    async def test_changed_last_page_checksum_rejects_entire_capture_before_acceptance(self):
        from dataclasses import replace
        from application.intake import submit_web
        capture, policy = self.capture()
        broken = replace(capture, pages=(capture.pages[0], replace(capture.pages[1], raw_sha256='sha256:' + '0' * 64)))
        with self.assertRaises(ValueError):
            await submit_web(self.context, 'collection', broken, policy)
        await self.assert_nothing_accepted()

    async def test_malformed_last_upload_leaves_no_partial_job_or_extraction_work(self):
        from application.intake import submit
        with self.assertRaises(ValueError):
            await submit(self.context, 'collection', [('valid.txt', b'First valid document'), ('invalid.exe', b'Unsupported document')])
        await self.assert_nothing_accepted()

    async def test_same_final_url_with_conflicting_bytes_rejects_entire_capture(self):
        from dataclasses import replace
        from application.intake import submit_web
        from application.web_acquisition import normalize_html_text
        capture, policy = self.capture()
        first = capture.pages[0]
        changed_raw = b'<html><main>A conflicting version of the same final page.</main></html>'
        conflicting = replace(first, requested_uri='https://qualification.invalid/redirect',
            raw_bytes=changed_raw, raw_sha256='sha256:' + hashlib.sha256(changed_raw).hexdigest(),
            text=normalize_html_text(changed_raw), assets=())
        with self.assertRaises(ValueError):
            await submit_web(self.context, 'collection', replace(capture, pages=(first, conflicting)), policy)
        await self.assert_nothing_accepted()

    async def test_same_final_url_with_identical_bytes_deduplicates_and_preserves_first_observation(self):
        from dataclasses import replace
        from application.intake import submit_web
        capture, policy = self.capture()
        first = capture.pages[0]
        duplicate = replace(first, requested_uri='https://qualification.invalid/alias',
            evidence={'http_status': 200, 'observation': 'later alias'}, assets=())
        job_id = UUID(await submit_web(self.context, 'collection', replace(capture, pages=(first, duplicate)), policy))
        revisions = await self.pool.fetch('SELECT * FROM application.source_revisions WHERE job_id=$1', job_id)
        self.assertEqual(len(revisions), 1)
        revision = revisions[0]
        self.assertEqual(revision['canonical_uri'], first.final_uri)
        self.assertEqual(retained_bytes(revision['raw_uri']), first.raw_bytes)
        proof = decoded(revision['acquisition'])
        proof_raw = retained_bytes(proof['uri'])
        self.assertEqual(hashlib.sha256(proof_raw).hexdigest(), proof['sha256'].removeprefix('sha256:'))
        manifest = json.loads(proof_raw)
        self.assertEqual(manifest['requested_uri'], first.requested_uri)
        self.assertEqual(manifest['http'], first.evidence)
        self.assertEqual(len(manifest['assets']), len(first.assets))
        self.assertTrue(all(asset['parent_url'] == first.final_uri for asset in manifest['assets']))
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM public.documents WHERE job_id=$1', job_id), 1)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM public.stage_units WHERE job_id=$1', job_id), 1)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM public.outbox_events'), 1)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'), 0)
        self.writer.run.assert_not_awaited()
