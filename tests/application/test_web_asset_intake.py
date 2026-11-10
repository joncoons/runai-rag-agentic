"""Actual grouped intake transactions; only remote acquisition is mocked.

The destructive StageBridge fixture admits only loopback qualification/qualification.
Fixtures are generated, public data; no model, Redis, website or VDB is contacted.
"""
from dataclasses import replace
import hashlib
import importlib.util
from io import BytesIO
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
from urllib.parse import unquote, urlsplit
from uuid import UUID
from PIL import Image


def decoded(value):
    return json.loads(value) if isinstance(value,str) else value


def retained(uri):
    parsed=urlsplit(uri)
    if parsed.scheme!='file' or parsed.netloc:raise AssertionError('Expected retained local file')
    return Path(unquote(parsed.path)).read_bytes()


@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'requires disposable qualification PostgreSQL')
class WebAssetIntakeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        source=Path(__file__).resolve().parents[1]/'collection_control/test_stage_bridge.py'
        spec=importlib.util.spec_from_file_location('asset_intake_stage_bridge_fixture',source)
        fixtures=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixtures)
        self.h=fixtures.StageBridgeTests()
        await self.h.asyncSetUp()
        self.addAsyncCleanup(self.h.asyncTearDown)
        self.pool=self.h.pool
        for migration in ('deployment/ingestion/captioning/001_caption_jobs.sql',
            'deployment/collection-control/003_caption_admissions.sql',
            'deployment/collection-control/004_web_groups.sql',
            'deployment/application/001_application.sql','deployment/application/002_typed_sources.sql',
            'deployment/application/003_web_assets.sql'):
            await self.pool.execute(Path(migration).read_text())
        await self.pool.execute("INSERT INTO application.collections(collection_id,name) VALUES('collection','Generated web asset qualification')")
        contracts=json.loads(Path(os.environ['MODEL_CONTRACTS_FILE']).read_text())
        contract={**self.h.contract,'embedding_checkpoint_sha256':contracts['embedding_model_digest']}
        settings=SimpleNamespace(raw=self.h.raw_root,stages=self.h.stage_root,snapshots=self.h.snapshots,
            contract=contract,models={'models':{'embed':{'model':'nvidia/Nemotron-3.5-Embed-1B-NVFP4'}}})
        self.context=SimpleNamespace(pool=self.pool,settings=settings)

    def capture(self,extension='.csv',*,two_binary_urls=False):
        from application.web_acquisition import AcquisitionPolicy,Asset,CrawlCapture,PageCapture,normalize_html_text
        from application.web_asset_acquisition import AssetCapture
        image=BytesIO();Image.new('RGB',(32,24),'white').save(image,format='PNG');image_raw=image.getvalue()
        if extension=='.pdf':
            import pypdfium2 as pdfium
            document=pdfium.PdfDocument.new()
            try:
                document.new_page(200,200);binary=BytesIO();document.save(binary);binary_raw=binary.getvalue()
            finally:document.close()
            media='application/pdf'
        else:binary_raw=b'quarter,megawatts\nfirst,42\nsecond,55\n';media='text/csv'
        base='https://qualification.invalid/'
        pages=[];acquired=[]
        for index,suffix in enumerate(('first','second')):
            uri=base+suffix
            binary_uri=base+('manual-'+suffix if two_binary_urls else 'manual')+extension
            raw=('<html><title>Public guide</title><main><p>Shared asset qualification.</p>'
                '<img src="'+base+'chart.png" alt="Public output chart"><a href="'+binary_uri+'">Output data</a></main></html>').encode()
            assets=(Asset(url=base+'chart.png',kind='image',parent_url=uri,alt='Public output chart',locator='img[0]'),
                    Asset(url=binary_uri,kind='binary',parent_url=uri,alt='',locator='a[0]'))
            evidence={'retrieved_at':'2026-11-08T00:00:00+00:00','http_status':200,'content_type':'text/html','redirect_chain':[{'uri':uri,'status':200}]}
            pages.append(PageCapture(requested_uri=uri,final_uri=uri,raw_bytes=raw,
                raw_sha256='sha256:'+hashlib.sha256(raw).hexdigest(),text=normalize_html_text(raw),
                title='Public guide',links=(),assets=assets,evidence=evidence))
            for asset,payload,content_type,filename in ((assets[0],image_raw,'image/png','chart.png'),
                    (assets[1],binary_raw,media,'manual'+extension)):
                acquired.append(AssetCapture(requested_uri=asset.url,final_uri=asset.url,raw_bytes=payload,
                    raw_sha256='sha256:'+hashlib.sha256(payload).hexdigest(),media_type=content_type,
                    filename=filename,evidence={'http_status':200,'byte_size':len(payload),'missing_means_deleted':False},
                    parent_url=uri,kind=asset.kind,locator=asset.locator,alt=asset.alt))
        return CrawlCapture(pages=tuple(pages),errors=(),complete=True),AcquisitionPolicy(allowed_hosts=('qualification.invalid',),max_pages=2),tuple(acquired)

    async def assert_no_acceptance(self):
        for table in ('public.jobs','application.jobs','public.documents','application.source_revisions',
            'public.stage_units','public.outbox_events','collection_control.web_groups',
            'collection_control.web_group_members','collection_control.operations'):
            self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM '+table),0,table)

    async def submit(self,capture,policy,assets):
        from application.intake import submit_web
        with patch('application.web_asset_acquisition.acquire_assets',AsyncMock(return_value=assets)) as acquire:
            result=await submit_web(self.context,'collection',capture,policy)
        acquire.assert_awaited_once_with(capture,replace(policy,max_response_bytes=policy.max_asset_response_bytes))
        return UUID(result)

    async def check_positive(self,extension):
        from application.image_extraction import image_contract
        from application.intake import embedding_config
        capture,policy,assets=self.capture(extension)
        job=await self.submit(capture,policy,assets)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM public.jobs'),1)
        revisions=await self.pool.fetch('SELECT * FROM application.source_revisions WHERE job_id=$1',job)
        self.assertEqual(len(revisions),4)
        self.assertEqual(len({r['document_id'] for r in revisions}),4)
        self.assertEqual(len({r['source_id'] for r in revisions}),4)
        self.assertEqual(len({r['source_revision_id'] for r in revisions}),4)
        self.assertEqual(sorted(r['web_role'] for r in revisions),['binary','image','page','page'])
        by_document={r['document_id']:r for r in revisions}
        groups=await self.pool.fetch('SELECT * FROM collection_control.web_groups WHERE job_id=$1',job)
        self.assertEqual(len(groups),1);group=groups[0]
        self.assertEqual(group['state'],'pending');self.assertIsNone(group['operation_id'])
        self.assertEqual({r['web_group_id'] for r in revisions},{group['group_id']})
        self.assertEqual({r['status'] for r in revisions},{'queued'})
        self.assertTrue(all(r['operation_id'] is None for r in revisions))
        raw_manifest=retained(group['manifest_uri'])
        self.assertEqual(hashlib.sha256(raw_manifest).hexdigest(),group['manifest_sha256'])
        manifest=json.loads(raw_manifest)
        self.assertEqual(manifest['coverage'],'complete');self.assertEqual(manifest['schema_version'],'web-asset-group.v1')
        self.assertEqual(manifest['job_id'],str(job));self.assertEqual(manifest['group_id'],str(group['group_id']))
        self.assertEqual(len(manifest['members']),4);self.assertEqual(len(manifest['pages']),2)
        self.assertNotIn('base64',raw_manifest.decode())
        members=await self.pool.fetch('SELECT * FROM collection_control.web_group_members WHERE group_id=$1',group['group_id'])
        self.assertEqual({(m['document_id'],m['source_revision_id'],m['role']) for m in members},
            {(r['document_id'],r['source_revision_id'],r['web_role']) for r in revisions})
        model,config=image_contract()
        expected=json.loads(Path(os.environ['MODEL_CONTRACTS_FILE']).read_text())['extraction_model_contract']
        selected={name:expected[name] for name in ('page-elements','ocr','table-structure')}
        self.assertEqual(model,'sha256:'+hashlib.sha256(json.dumps(selected,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest())
        for page in manifest['pages']:
            self.assertEqual(len(page['assets']),2)
            self.assertEqual({a['kind'] for a in page['assets']},{'image','binary'})
            for relation in page['assets']:
                revision=by_document[relation['document_id']]
                for field in ('source_revision_id','canonical_uri','raw_uri','raw_sha256','media_type'):
                    self.assertEqual(relation[field],revision[field])
                self.assertEqual(hashlib.sha256(retained(relation['raw_uri'])).hexdigest(),relation['raw_sha256'])
                expected=next(a for a in assets if a.parent_url==page['canonical_uri'] and a.kind==relation['kind'])
                self.assertEqual(retained(relation['raw_uri']),expected.raw_bytes)
                self.assertEqual(relation['locator'],expected.locator);self.assertEqual(relation['alt'],expected.alt)
                if relation['kind']=='image':self.assertEqual((relation['model_digest'],relation['config_digest']),(model,config))
        self.assertEqual({a['document_id'] for p in manifest['pages'] for a in p['assets'] if a['kind']=='image'},
            {r['document_id'] for r in revisions if r['web_role']=='image'})
        stages=await self.pool.fetch('SELECT * FROM public.stage_units WHERE job_id=$1',job)
        outbox=await self.pool.fetch('SELECT * FROM public.outbox_events')
        self.assertEqual(len(stages),4);self.assertEqual(len(outbox),4)
        self.assertEqual({o['idempotency_key'] for o in outbox},{s['idempotency_key'] for s in stages})
        for stage in stages:
            self.assertEqual(stage['status'],'pending');self.assertEqual(stage['stage'],'extraction')
            envelope=decoded(stage['input_envelope']);revision=by_document[stage['document_id']]
            self.assertEqual(envelope['artifact_uri'],revision['raw_uri'])
            self.assertEqual(envelope['job_id'],str(job))
            expected=(model,config) if revision['web_role']=='image' else (
                'sha256:'+self.context.settings.contract['embedding_checkpoint_sha256'],'sha256:'+embedding_config(self.context.settings))
            self.assertEqual((envelope['model_digest'],envelope['config_digest']),expected)
            if revision['web_role']=='image':self.assertEqual(revision['source_kind'],'web_image')
            elif revision['web_role']=='binary':self.assertEqual(revision['source_kind'],extension[1:])
        for event in outbox:
            self.assertEqual(event['target_stream'],'rag.agentic.extraction.pending')
            self.assertIsNone(event['published_at']);self.assertIsNone(event['broker_message_id'])
            self.assertEqual(event['publish_attempts'],0)
            envelope=decoded(event['envelope'])
            self.assertEqual(envelope['artifact_uri'],by_document[envelope['document_id']]['raw_uri'])
        self.assertFalse(decoded(await self.pool.fetchval('SELECT metadata FROM public.jobs WHERE job_id=$1',job))['ingestion_options']['captions_enabled'])
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),0)
        self.assertIsNone(await self.pool.fetchval("SELECT current_operation FROM collection_control.collections WHERE collection_id='collection'"))

    async def test_shared_image_and_pdf_have_four_transactional_inputs(self):
        await self.check_positive('.pdf')

    async def test_shared_image_and_csv_have_four_transactional_inputs(self):
        await self.check_positive('.csv')

    async def test_same_asset_bytes_at_two_urls_keep_distinct_documents(self):
        capture,policy,assets=self.capture(two_binary_urls=True)
        job=await self.submit(capture,policy,assets)
        rows=await self.pool.fetch("SELECT * FROM application.source_revisions WHERE job_id=$1 AND web_role='binary'",job)
        self.assertEqual(len(rows),2)
        for field in ('canonical_uri','source_id','source_revision_id','document_id'):
            self.assertEqual(len({r[field] for r in rows}),2,field)
        self.assertEqual(len({r['raw_sha256'] for r in rows}),1)
        self.assertEqual(len({r['raw_uri'] for r in rows}),1)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM public.stage_units WHERE job_id=$1',job),5)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),0)

    async def test_partial_asset_failure_leaves_no_accepted_group(self):
        from application.intake import submit_web
        from application.web_asset_acquisition import AssetAcquisitionError
        capture,policy,assets=self.capture()
        with patch('application.web_asset_acquisition.acquire_assets',AsyncMock(side_effect=AssetAcquisitionError('Generated unavailable asset'))):
            with self.assertRaises(AssetAcquisitionError):await submit_web(self.context,'collection',capture,policy)
        await self.assert_no_acceptance()

    async def test_incomplete_acquired_relationship_coverage_rejects_whole_group(self):
        capture,policy,assets=self.capture()
        with self.assertRaises(ValueError):await self.submit(capture,policy,assets[:-1])
        await self.assert_no_acceptance()

    async def test_extra_acquired_relationship_rejects_whole_group(self):
        capture,policy,assets=self.capture()
        unexpected=replace(assets[-1],requested_uri='https://qualification.invalid/not-discovered.csv')
        with self.assertRaises(ValueError):await self.submit(capture,policy,assets+(unexpected,))
        await self.assert_no_acceptance()

    async def test_collection_rejection_keeps_retained_group_uncommitted(self):
        from collection_control.repository import Conflict
        capture,policy,assets=self.capture()
        await self.pool.execute("UPDATE collection_control.collections SET deleted=true WHERE collection_id='collection'")
        with self.assertRaises(Conflict):await self.submit(capture,policy,assets)
        # Retaining immutable bytes is reversible intake preparation. It must
        # not turn into a partial job/group when transactional admission rejects.
        self.assertTrue(list(self.h.raw_root.rglob('*.web-group.json')))
        await self.assert_no_acceptance()
