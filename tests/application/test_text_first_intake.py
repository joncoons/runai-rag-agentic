"""Real PG: HTML intake and asset intents commit without any asset bytes."""
import json
import os
from pathlib import Path
import unittest
from unittest.mock import AsyncMock,patch
from tests.application.test_web_asset_intake import WebAssetIntakeTests

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'requires disposable qualification PG')
class TextFirstIntakeTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=WebAssetIntakeTests.asyncSetUp
    capture=WebAssetIntakeTests.capture
    async def test_html_and_deferred_intents_commit_before_download_or_extraction(self):
        from application.intake import submit_web_text
        for path in sorted(Path('deployment/application').glob('*.sql')):await self.pool.execute(path.read_text())
        for path in sorted(Path('deployment/collection-control').glob('*.sql')):await self.pool.execute(path.read_text())
        capture,policy,assets=self.capture()
        with patch('application.web_asset_acquisition.acquire_asset',new=AsyncMock(side_effect=AssertionError('Asset download on HTML path'))):
            jobs=await submit_web_text(self.context,'collection',capture,policy,observation_id='qualification:scan-1',captions_enabled=True)
        self.assertEqual(len(jobs),2)
        rows=await self.pool.fetch('SELECT source_kind,web_role FROM application.source_revisions')
        self.assertEqual([(r['source_kind'],r['web_role']) for r in rows],[('html','page'),('html','page')])
        events=await self.pool.fetch('SELECT target_stream FROM public.outbox_events')
        self.assertEqual({r['target_stream'] for r in events},{'rag.agentic.pages.extraction.pending'})
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.web_page_admissions'),2)
        replay=await submit_web_text(self.context,'collection',capture,policy,observation_id='qualification:scan-1',captions_enabled=True)
        self.assertEqual(jobs,replay)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.source_revisions'),2)

    async def test_checksum_bound_preflight_is_reused_without_parsing_during_registration(self):
        from application.intake import submit
        from application.deferred_asset_native import NativeAssetSubmitter
        capture,policy,assets=self.capture()
        binary=next(a for a in assets if a.kind=='binary')
        prepared=await NativeAssetSubmitter(self.context.settings).prepare({},binary)
        with patch('application.intake.inspect_upload',side_effect=AssertionError('Binary parsed inside native registration')):
            job=await submit(self.context,'collection',[(binary.filename,binary.raw_bytes)],
                _web_captures={binary.filename:{'canonical_uri':binary.requested_uri,'role':'binary','media_type':binary.media_type}},
                _streamed=True,_inspections=prepared)
        row=await self.pool.fetchrow('SELECT source_kind,source_units FROM application.source_revisions WHERE job_id=$1',__import__('uuid').UUID(job))
        self.assertEqual(row['source_kind'],'csv')
        units=json.loads(row['source_units']) if isinstance(row['source_units'],str) else row['source_units']
        self.assertTrue(units['units'])

    async def test_page_queue_registration_does_not_revalidate_the_published_corpus(self):
        from application.intake import submit_web_text
        for path in sorted(Path('deployment/application').glob('*.sql')):await self.pool.execute(path.read_text())
        for path in sorted(Path('deployment/collection-control').glob('*.sql')):await self.pool.execute(path.read_text())
        capture,policy,_=self.capture()
        with patch('collection_control.web_groups.resolve_graph',new=AsyncMock(side_effect=AssertionError('Queue intake must not validate the whole published corpus'))):
            jobs=await submit_web_text(self.context,'collection',capture,policy,observation_id='qualification:fast-intake')
        self.assertEqual(len(jobs),2)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),0)
