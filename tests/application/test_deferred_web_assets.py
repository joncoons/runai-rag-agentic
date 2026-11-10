"""Offline acquisition contract tests; no database, Redis or HTTP calls."""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4


class DeferredAssetTests(unittest.IsolatedAsyncioTestCase):
    def test_identity_binds_observation_kind_and_public_policy(self):
        from application.deferred_web_assets import download_identity
        policy={'allowed_hosts':['qualification.invalid'],'private_hosts':['qualification.invalid']}
        same={'allowed_hosts':['qualification.invalid'],'private_hosts':[]}
        key=download_identity('collection:run','https://qualification.invalid/a.pdf','binary',policy)
        self.assertEqual(key, download_identity('collection:run','https://qualification.invalid/a.pdf','binary',same))
        self.assertNotEqual(key,download_identity('collection:next','https://qualification.invalid/a.pdf','binary',same))
        self.assertNotEqual(key,download_identity('collection:run','https://qualification.invalid/a.pdf','image',same))

    def test_retry_is_bounded_and_delayed(self):
        from application.deferred_web_assets import retry_delay
        self.assertEqual([retry_delay(n) for n in (1,2,3,4)], [5,30,120,None])
        with self.assertRaises(ValueError): retry_delay(0)

    async def test_download_occurs_outside_transaction_and_completion_is_awaited(self):
        from application.deferred_web_assets import DeferredWebAssets
        from application.web_acquisition import AcquisitionPolicy
        task={'download_id':uuid4(),'fence':3,'requested_uri':'https://qualification.invalid/a.pdf',
              'kind':'binary','policy':asdict(AcquisitionPolicy(allowed_hosts=('qualification.invalid',))),
              'references':[{'parent_url':'https://qualification.invalid/page','locator':'a[0]','alt':''}],
              'raw_uri':None}
        repository=DeferredWebAssets(None,Path('/tmp'))
        repository.claim=AsyncMock(return_value=task); repository.complete=AsyncMock(return_value={'state':'completed'})
        repository.fail=AsyncMock(); repository.renew=AsyncMock()
        captured=object(); callback=AsyncMock()
        with patch('application.deferred_web_assets.acquire_asset',new=AsyncMock(return_value=captured)) as fetch:
            self.assertTrue(await repository.process_one('worker',callback))
        fetch.assert_awaited_once()
        self.assertEqual(fetch.await_args.args[1].max_response_bytes,fetch.await_args.args[1].max_asset_response_bytes)
        repository.complete.assert_awaited_once_with(task,captured,callback)
        repository.fail.assert_not_awaited()

    async def test_download_failure_is_durable_retry_and_cancellation_propagates(self):
        from application.deferred_web_assets import DeferredWebAssets
        from application.web_acquisition import AcquisitionPolicy
        task={'download_id':uuid4(),'fence':2,'requested_uri':'https://qualification.invalid/a.pdf','kind':'binary',
              'policy':asdict(AcquisitionPolicy(allowed_hosts=('qualification.invalid',))),
              'references':[{'parent_url':'https://qualification.invalid/p','locator':'a[0]','alt':''}], 'raw_uri':None}
        repository=DeferredWebAssets(None,Path('/tmp')); repository.claim=AsyncMock(return_value=task)
        repository.fail=AsyncMock(); repository.renew=AsyncMock()
        with patch('application.deferred_web_assets.acquire_asset',new=AsyncMock(side_effect=ValueError('invalid'))):
            self.assertTrue(await repository.process_one('worker',AsyncMock()))
        repository.fail.assert_awaited_once()
        with patch('application.deferred_web_assets.acquire_asset',new=AsyncMock(side_effect=asyncio.CancelledError)):
            with self.assertRaises(asyncio.CancelledError):await repository.process_one('worker',AsyncMock())
        self.assertEqual(repository.fail.await_count,1)

    async def test_discovery_requires_callers_transaction(self):
        from application.deferred_web_assets import DeferredWebAssets
        connection=SimpleNamespace(is_in_transaction=lambda:False)
        with self.assertRaises(ValueError):
            await DeferredWebAssets(None,Path('/tmp')).discover(connection,observation_id='c:r',collection_id='c',
                parent_source_revision_id='rev',parent_document_id='doc',parent_job_id=uuid4(),
                policy={},assets=[])

    async def test_completion_checks_fence_before_native_intake_and_binds_raw_proof(self):
        from application.deferred_web_assets import DeferredWebAssets
        from application.web_asset_acquisition import AssetCapture
        from collection_control.repository import Fenced
        import hashlib
        from uuid import UUID
        state={'transaction':False,'sql':[]}
        @asynccontextmanager
        async def transaction():
            state['transaction']=True
            try: yield
            finally: state['transaction']=False
        @asynccontextmanager
        async def acquire(): yield connection
        job=uuid4(); raw=b'%PDF-test'; digest=hashlib.sha256(raw).hexdigest()
        revision={'collection_id':'c','job_id':job,'raw_sha256':digest,'canonical_uri':'https://qualification.invalid/a.pdf'}
        async def execute(sql,*args): state['sql'].append((sql,args))
        connection=SimpleNamespace(transaction=transaction,fetchrow=AsyncMock(return_value=revision),execute=execute)
        pool=SimpleNamespace(acquire=acquire)
        task={'download_id':uuid4(),'fence':1,'kind':'binary','requested_uri':revision['canonical_uri'],
              'collection_id':'c','observation_id':'c:r','raw_sha256':None,'policy_sha256':'a'*64}
        capture=AssetCapture(task['requested_uri'],task['requested_uri'],raw,'sha256:'+digest,'application/pdf',
            'a.pdf',{'http_status':200},'https://qualification.invalid/p','binary','a[0]','')
        with TemporaryDirectory() as directory:
            repository=DeferredWebAssets(pool,Path(directory)); repository._leased=AsyncMock(side_effect=Fenced('stale'))
            callback=AsyncMock()
            with self.assertRaises(Fenced):await repository.complete(task,capture,callback)
            callback.assert_not_awaited();self.assertEqual(state['sql'],[])
            repository._leased=AsyncMock(return_value=task)
            async def submit(c,t,body,proof):
                self.assertIs(c,connection);self.assertTrue(state['transaction'])
                self.assertEqual(t['download_id'],task['download_id']);self.assertEqual(body,capture)
                import json
                from collection_control.provenance import verified_file
                manifest=json.loads(verified_file(directory,proof['uri'],proof['sha256']))
                self.assertEqual(manifest['request_id'],str(task['download_id']))
                self.assertEqual(manifest['raw_sha256'],digest)
                return {'job_id':job,'source_revision_id':'asset:1','publication_manifest':{'uri':'file:///proof','sha256':'b'*64}}
            await repository.complete(task,capture,submit)
            self.assertEqual(len(state['sql']),2)
            self.assertIn("state='completed'",state['sql'][-1][0])
            self.assertEqual(state['sql'][-1][1][-2:],('file:///proof','b'*64))

    async def test_completion_rejects_unrelated_native_revision(self):
        from application.deferred_web_assets import DeferredWebAssets
        from application.web_asset_acquisition import AssetCapture
        from collection_control.repository import Conflict
        import hashlib
        @asynccontextmanager
        async def scope():yield connection
        connection=SimpleNamespace(transaction=scope,fetchrow=AsyncMock(return_value=None),execute=AsyncMock())
        pool=SimpleNamespace(acquire=scope)
        raw=b'%PDF-test';digest=hashlib.sha256(raw).hexdigest();uri='https://qualification.invalid/a.pdf'
        task={'download_id':uuid4(),'fence':1,'kind':'binary','requested_uri':uri,'collection_id':'c',
              'observation_id':'c:r','raw_sha256':None,'policy_sha256':'a'*64}
        capture=AssetCapture(uri,uri,raw,digest,'application/pdf','a.pdf',{},uri,'binary','','')
        with TemporaryDirectory() as directory:
            repository=DeferredWebAssets(pool,directory);repository._leased=AsyncMock(return_value=task)
            with self.assertRaises(Conflict):
                await repository.complete(task,capture,AsyncMock(return_value={'job_id':uuid4(),'source_revision_id':'wrong'}))
        connection.execute.assert_not_awaited()

    async def test_relay_uses_separate_notification_without_extraction_envelope(self):
        from application.deferred_web_assets import DeferredWebAssets,STREAM
        event={'event_id':uuid4(),'download_id':uuid4()}
        pool=SimpleNamespace(fetch=AsyncMock(return_value=[event]),execute=AsyncMock())
        redis=SimpleNamespace(xadd=AsyncMock(return_value=b'1-0'))
        repository=DeferredWebAssets(pool,'/tmp')
        self.assertEqual(await repository.relay_once(redis),1)
        args=redis.xadd.await_args.args
        self.assertEqual(args[0],STREAM);self.assertEqual(args[1]['download_id'],str(event['download_id']))
        self.assertNotIn('stage',args[1]);self.assertNotIn('artifact_uri',args[1])
        self.assertEqual(pool.execute.await_args.args[-1],'1-0')

    async def test_late_parent_queues_attachment_without_another_download(self):
        from application.deferred_web_assets import DeferredWebAssets
        from application.web_acquisition import AcquisitionPolicy,Asset
        from uuid import UUID
        calls=[]; parent='https://qualification.invalid/page';job=uuid4()
        policy=AcquisitionPolicy(allowed_hosts=('qualification.invalid',))
        task={'state':'completed','collection_id':'c','policy_sha256':None}
        from collection_control.artifacts import canonical
        import hashlib
        from application.deferred_web_assets import public_policy
        task['policy_sha256']=hashlib.sha256(canonical(public_policy(policy))).hexdigest()
        async def fetchrow(sql,*args):
            if 'collection_control.collections' in sql:return {'deleted':False}
            if 'job_status' in sql:return {'status':'published','job_status':'completed','canonical_uri':parent}
            return task
        async def execute(sql,*args):calls.append((sql,args))
        connection=SimpleNamespace(is_in_transaction=lambda:True,fetchrow=fetchrow,execute=execute,
                                   fetchval=AsyncMock(return_value=uuid4()))
        repository=DeferredWebAssets(None,'/tmp')
        result=await repository.discover(connection,observation_id='c:r',collection_id='c',
            parent_source_revision_id='page:2',parent_document_id='doc:2',parent_job_id=job,policy=policy,
            assets=[Asset('https://qualification.invalid/a.pdf','binary',parent,'alt','a[2]')],captions_enabled=True)
        self.assertEqual(len(result),1)
        self.assertTrue(any("state='pending',attempts=0" in sql for sql,args in calls))
        self.assertTrue(any('web_asset_acquisition_outbox' in sql for sql,args in calls))

    def test_runtime_private_scope_comes_only_from_server_configuration(self):
        from application.deferred_web_assets import runtime_policy
        with patch.dict('os.environ',{'WEB_ASSET_PRIVATE_HOSTS':'trusted.internal','WEB_ASSET_PRIVATE_PORTS':'8443'}):
            policy=runtime_policy({'allowed_hosts':['trusted.internal','other.internal'],
                'private_hosts':['other.internal'],'private_ports':[9999]})
        self.assertEqual(policy.private_hosts,('trusted.internal',));self.assertEqual(policy.private_ports,(8443,))

    async def test_stream_consumption_acknowledges_only_terminal_database_result(self):
        from application.deferred_web_assets import DeferredWebAssets,STREAM
        download=uuid4()
        fields={b'schema_version':b'web-asset-acquisition-request.v1',b'download_id':str(download).encode()}
        redis=SimpleNamespace(xgroup_create=AsyncMock(),xautoclaim=AsyncMock(return_value=('0-0',[('1-0',fields)],[])),
            xreadgroup=AsyncMock(),xack=AsyncMock())
        pool=SimpleNamespace(fetchval=AsyncMock(return_value='retry'))
        repository=DeferredWebAssets(pool,'/tmp');repository.process_one=AsyncMock(return_value=True)
        self.assertTrue(await repository.consume_once(redis,'worker',AsyncMock()))
        redis.xack.assert_not_awaited()
        pool.fetchval.return_value='completed'
        self.assertTrue(await repository.consume_once(redis,'worker',AsyncMock()))
        redis.xack.assert_awaited_once_with(STREAM,'web-asset-acquisition','1-0')
        self.assertEqual(repository.process_one.await_args.kwargs['download_id'],download)

    async def test_lost_notification_recovers_via_due_postgres_claim_after_blocked_read(self):
        from application.deferred_web_assets import DeferredWebAssets
        redis=SimpleNamespace(xgroup_create=AsyncMock(),xautoclaim=AsyncMock(return_value=('0-0',[],[])),
            xreadgroup=AsyncMock(return_value=[]),xack=AsyncMock())
        repository=DeferredWebAssets(None,'/tmp');repository.process_one=AsyncMock(return_value=True)
        self.assertTrue(await repository.consume_once(redis,'worker',AsyncMock()))
        self.assertEqual(redis.xreadgroup.await_args.kwargs['block'],1000)
        self.assertNotIn('download_id',repository.process_one.await_args.kwargs)
        redis.xack.assert_not_awaited()

import os

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'requires disposable guarded qualification PostgreSQL')
class DeferredAssetPostgresTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Reuse only the fixture lifecycle, never its inherited test methods.
        from tests.application.test_web_asset_intake import WebAssetIntakeTests
        self.fixture=WebAssetIntakeTests(methodName='runTest')
        await self.fixture.asyncSetUp();self.addAsyncCleanup(self.fixture.h.asyncTearDown)
        self.pool=self.fixture.pool;self.context=self.fixture.context
        for migration in ('deployment/application/004_source_rechecks.sql','deployment/application/008_deferred_web_assets.sql'):
            await self.pool.execute(Path(migration).read_text())
        from application.deferred_web_assets import DeferredWebAssets
        self.repository=DeferredWebAssets(self.pool,self.context.settings.raw)

    async def admitted(self,*,one_page=False):
        from application.intake import submit
        capture,policy,assets=self.fixture.capture()
        pages=capture.pages[:1] if one_page else capture.pages
        for page in pages:
            job=await submit(self.context,'collection',[('page.html',page.raw_bytes)],
                _web_captures={'page.html':{'canonical_uri':page.final_uri,'role':'page'}})
            revision=await self.pool.fetchrow('SELECT * FROM application.source_revisions WHERE job_id=$1',__import__('uuid').UUID(job))
            async with self.pool.acquire() as c,c.transaction():
                await self.repository.discover(c,observation_id='collection:run',collection_id='collection',
                    parent_source_revision_id=revision['source_revision_id'],parent_document_id=revision['document_id'],
                    parent_job_id=job,policy=policy,assets=page.assets,captions_enabled=True)
        return capture,policy,assets

    async def native(self,c,task,capture,proof):
        from application.intake import submit
        from collection_control.pinned_connection import PinnedPool
        if task.get('child_job_id'):
            return {'job_id':task['child_job_id'],'source_revision_id':task['child_source_revision_id']}
        context=SimpleNamespace(pool=PinnedPool(c),settings=self.context.settings)
        job=await submit(context,'collection',[(capture.filename,capture.raw_bytes)],
            _web_captures={capture.filename:{'canonical_uri':capture.requested_uri,'role':capture.kind,
                'media_type':capture.media_type,'acquisition':proof}})
        revision=await c.fetchrow('SELECT * FROM application.source_revisions WHERE job_id=$1',__import__('uuid').UUID(job))
        return {'job_id':job,'source_revision_id':revision['source_revision_id']}

    async def test_dedupe_claim_native_completion_and_duplicate_notification(self):
        capture,policy,assets=await self.admitted()
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.web_asset_downloads'),2)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.web_asset_references'),4)
        task=await self.repository.claim('worker',ttl=60)
        self.assertEqual(len(task['references']),2)
        asset=next(a for a in assets if a.requested_uri==task['requested_uri'])
        result=await self.repository.complete(task,asset,self.native)
        row=await self.pool.fetchrow('SELECT * FROM application.web_asset_downloads WHERE download_id=$1',task['download_id'])
        self.assertEqual(row['state'],'completed');self.assertEqual(str(row['child_job_id']),str(result['job_id']))
        self.assertIsNotNone(row['acquisition_uri']);self.assertEqual(row['raw_sha256'],asset.raw_sha256.removeprefix('sha256:'))
        self.assertIsNone(await self.repository.claim('duplicate',download_id=task['download_id']))
        revision=await self.pool.fetchrow('SELECT * FROM application.source_revisions WHERE source_revision_id=$1',result['source_revision_id'])
        self.assertEqual(revision['web_asset_download_id'],task['download_id'])

    async def test_lease_takeover_and_bounded_retry(self):
        from collection_control.repository import Fenced
        await self.admitted(one_page=True)
        first=await self.repository.claim('old',ttl=60)
        await self.pool.execute("UPDATE application.web_asset_downloads SET lease_until=clock_timestamp()-interval '1 second' WHERE download_id=$1",first['download_id'])
        second=await self.repository.claim('new',download_id=first['download_id'])
        self.assertEqual(second['attempts'],2);self.assertGreater(second['fence'],first['fence'])
        with self.assertRaises(Fenced):await self.repository.renew(first['download_id'],first['fence'])
        await self.repository.fail(second['download_id'],second['fence'],ValueError('synthetic'))
        self.assertIsNone(await self.repository.claim('tooearly',download_id=first['download_id']))
        for expected in (3,4):
            await self.pool.execute("UPDATE application.web_asset_downloads SET not_before=clock_timestamp()-interval '1 second' WHERE download_id=$1",first['download_id'])
            task=await self.repository.claim('retry',download_id=first['download_id'])
            self.assertEqual(task['attempts'],expected)
            await self.repository.fail(task['download_id'],task['fence'],ValueError('synthetic'))
        self.assertEqual(await self.pool.fetchval('SELECT state FROM application.web_asset_downloads WHERE download_id=$1',first['download_id']),'failed')

    async def test_new_parent_after_completion_reattaches_using_cached_bytes(self):
        from application.intake import submit
        capture,policy,assets=await self.admitted(one_page=True)
        task=await self.repository.claim('worker');asset=next(a for a in assets if a.requested_uri==task['requested_uri'])
        result=await self.repository.complete(task,asset,self.native)
        page=capture.pages[1]
        job=await submit(self.context,'collection',[('page.html',page.raw_bytes)],_web_captures={'page.html':{'canonical_uri':page.final_uri,'role':'page'}})
        revision=await self.pool.fetchrow('SELECT * FROM application.source_revisions WHERE job_id=$1',__import__('uuid').UUID(job))
        async with self.pool.acquire() as c,c.transaction():
            await self.repository.discover(c,observation_id='collection:run',collection_id='collection',
                parent_source_revision_id=revision['source_revision_id'],parent_document_id=revision['document_id'],
                parent_job_id=job,policy=policy,assets=page.assets,captions_enabled=False)
        reattach=await self.repository.claim('attach',download_id=task['download_id'])
        self.assertEqual(len(reattach['references']),2)
        self.assertEqual(reattach['child_job_id'],__import__('uuid').UUID(result['job_id']))
        with patch('application.deferred_web_assets.acquire_asset',new=AsyncMock(side_effect=AssertionError('must reuse bytes'))):
            cached=self.repository._cached(reattach)
            repeated=await self.repository.complete(reattach,cached,self.native)
        self.assertEqual(str(repeated['job_id']),str(result['job_id']))

    async def test_new_parent_revision_cancels_old_download_and_collection_delete_fences(self):
        from application.intake import submit
        from collection_control.repository import Fenced
        capture,policy,assets=await self.admitted(one_page=True)
        task=await self.repository.claim('worker')
        page=capture.pages[0]
        await submit(self.context,'collection',[('new.html',page.raw_bytes)],
            _web_captures={'new.html':{'canonical_uri':page.final_uri,'role':'page'}})
        with self.assertRaises(Fenced):await self.repository.renew(task['download_id'],task['fence'])
        await self.pool.execute("UPDATE application.web_asset_downloads SET lease_until=clock_timestamp()-interval '1 second' WHERE download_id=$1",task['download_id'])
        self.assertIsNone(await self.repository.claim('staleparent',download_id=task['download_id']))
        self.assertEqual(await self.pool.fetchval('SELECT state FROM application.web_asset_downloads WHERE download_id=$1',task['download_id']),'cancelled')
        await self.pool.execute("UPDATE collection_control.collections SET deleted=true WHERE collection_id='collection'")
        self.assertIsNone(await self.repository.claim('deleted'))
        self.assertEqual(await self.pool.fetchval("SELECT count(*) FROM application.web_asset_downloads WHERE state!='cancelled'"),0)

    async def test_callback_fault_rolls_back_native_job_and_keeps_request_recoverable(self):
        capture,policy,assets=await self.admitted(one_page=True)
        task=await self.repository.claim('worker');asset=next(a for a in assets if a.requested_uri==task['requested_uri'])
        jobs=await self.pool.fetchval('SELECT count(*) FROM application.jobs')
        async def interrupted(c,t,body,proof):
            await self.native(c,t,body,proof)
            raise RuntimeError('synthetic process failure before completion commit')
        with self.assertRaises(RuntimeError):await self.repository.complete(task,asset,interrupted)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.jobs'),jobs)
        row=await self.pool.fetchrow('SELECT * FROM application.web_asset_downloads WHERE download_id=$1',task['download_id'])
        self.assertEqual(row['state'],'running');self.assertIsNone(row['child_job_id']);self.assertIsNone(row['raw_uri'])
        await self.repository.complete(task,asset,self.native)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.jobs'),jobs+1)

    async def test_native_preparation_does_not_hold_collection_lock(self):
        capture,policy,assets=await self.admitted(one_page=True)
        task=await self.repository.claim('prepare-worker')
        asset=next(a for a in assets if a.requested_uri==task['requested_uri'])
        entered=asyncio.Event();release=asyncio.Event();prepared={'inspection':'prepared-outside-transaction'}
        testcase=self
        class Submitter:
            async def prepare(self,t,body):
                testcase.assertEqual(t['download_id'],task['download_id'])
                testcase.assertEqual(body,asset)
                entered.set();await release.wait();return prepared
            async def __call__(self,c,t,body,proof):
                testcase.assertTrue(c.is_in_transaction())
                testcase.assertIs(t['_native_prepared'],prepared)
                return await testcase.native(c,t,body,proof)
        completion=asyncio.create_task(self.repository.complete(task,asset,Submitter()))
        try:
            async with asyncio.timeout(2):await entered.wait()
            # Stand in for an unrelated page's intake transaction. It must commit
            # while native parsing is deliberately blocked, before native intake.
            async with asyncio.timeout(2):
                async with self.pool.acquire() as c,c.transaction():
                    await c.fetchrow("SELECT * FROM collection_control.collections WHERE collection_id='collection' FOR UPDATE")
                    await c.execute("INSERT INTO application.events(event_type,payload) VALUES('page_committed_during_asset_prepare','{}'::jsonb)")
            self.assertFalse(completion.done())
            self.assertEqual(await self.pool.fetchval("SELECT count(*) FROM application.events WHERE event_type='page_committed_during_asset_prepare'"),1)
            self.assertIsNone(await self.pool.fetchval('SELECT child_job_id FROM application.web_asset_downloads WHERE download_id=$1',task['download_id']))
        finally:
            release.set()
            if not entered.is_set():completion.cancel()
            try:await completion
            except asyncio.CancelledError:pass
        self.assertEqual(await self.pool.fetchval('SELECT state FROM application.web_asset_downloads WHERE download_id=$1',task['download_id']),'completed')

    async def bind_source_run(self,policy,*,state,paused=False):
        from application.source_registry import SourceRegistry
        from application.deferred_web_assets import public_policy
        registry=SourceRegistry(self.pool)
        source=await registry.create('collection',name='Generated partial scan',
            seed_urls=['https://qualification.invalid/first'],policy=public_policy(policy),schedule=None)
        if paused:
            # A manual observation may be requested for an already paused source;
            # this does not change its immutable source version.
            await self.pool.execute('UPDATE application.website_sources SET paused=true WHERE source_id=$1',source['source_id'])
        run=await registry.request(source['source_id'],'collection')
        run=await registry.claim('scanner')
        self.assertIsNotNone(run)
        await self.pool.execute('UPDATE application.web_asset_references SET source_run_id=$1,source_version=$2',run['run_id'],run['source_version'])
        await self.pool.execute('UPDATE application.website_source_runs SET state=$2,lease_until=NULL WHERE run_id=$1',run['run_id'],state)
        return run

    async def test_failed_scan_does_not_cancel_assets_of_accepted_valid_pages(self):
        capture,policy,assets=await self.admitted(one_page=True)
        await self.bind_source_run(policy,state='failed')
        task=await self.repository.claim('asset-after-partial-scan')
        self.assertIsNotNone(task,'Unrelated page 404 must not discard accepted-page asset work')
        asset=next(a for a in assets if a.requested_uri==task['requested_uri'])
        await self.repository.complete(task,asset,self.native)
        self.assertEqual(await self.pool.fetchval('SELECT state FROM application.web_asset_downloads WHERE download_id=$1',task['download_id']),'completed')

    async def test_explicitly_cancelled_scan_fences_deferred_assets(self):
        capture,policy,assets=await self.admitted(one_page=True)
        await self.bind_source_run(policy,state='cancelled')
        self.assertIsNone(await self.repository.claim('cancelled-run'))
        self.assertEqual(await self.pool.fetchval("SELECT count(*) FROM application.web_asset_downloads WHERE state='cancelled'"),2)

    async def test_manual_run_for_paused_source_keeps_accepted_assets_eligible(self):
        capture,policy,assets=await self.admitted(one_page=True)
        await self.bind_source_run(policy,state='accepted',paused=True)
        task=await self.repository.claim('manual-paused-source')
        self.assertIsNotNone(task)
        asset=next(a for a in assets if a.requested_uri==task['requested_uri'])
        await self.repository.complete(task,asset,self.native)
        self.assertEqual(await self.pool.fetchval('SELECT state FROM application.web_asset_downloads WHERE download_id=$1',task['download_id']),'completed')

    async def test_paused_scheduled_run_still_fences_assets(self):
        capture,policy,assets=await self.admitted(one_page=True)
        run=await self.bind_source_run(policy,state='accepted',paused=True)
        await self.pool.execute("UPDATE application.website_source_runs SET trigger='schedule' WHERE run_id=$1",run['run_id'])
        self.assertIsNone(await self.repository.claim('paused-schedule'))

    async def test_failed_parent_still_fences_assets_even_when_scan_failure_is_allowed(self):
        capture,policy,assets=await self.admitted(one_page=True)
        await self.bind_source_run(policy,state='failed')
        await self.pool.execute("UPDATE application.source_revisions SET status='failed'")
        self.assertIsNone(await self.repository.claim('failed-parent'))

# A download lease is delivery bookkeeping; only completion grants model work.
@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'requires disposable guarded qualification PostgreSQL')
class DeferredLeaseConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=DeferredAssetPostgresTests.asyncSetUp
    admitted=DeferredAssetPostgresTests.admitted
    async def test_claim_renew_and_failure_do_not_block_on_publication_lock(self):
        await self.admitted(one_page=True)
        async with self.pool.acquire() as c,c.transaction():
            await c.fetchrow("SELECT * FROM collection_control.collections WHERE collection_id='collection' FOR UPDATE")
            task=await asyncio.wait_for(self.repository.claim('independent-download'),.5)
            self.assertIsNotNone(task)
            await asyncio.wait_for(self.repository.renew(task['download_id'],task['fence']),.5)
            await asyncio.wait_for(self.repository.fail(task['download_id'],task['fence'],ValueError('generated retry')),.5)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.jobs'),1)

    async def test_native_completion_still_obeys_collection_delete_fence(self):
        from collection_control.repository import Fenced
        _,_,assets=await self.admitted(one_page=True)
        task=await self.repository.claim('completion-fence')
        capture=next(a for a in assets if a.requested_uri==task['requested_uri'])
        entered=asyncio.Event();called=[]
        class Submitter:
            async def prepare(self,*args):entered.set();return None
            async def __call__(self,*args):called.append(True);raise AssertionError('Deleted collection must not create model work')
        completion=None
        async with self.pool.acquire() as c,c.transaction():
            await c.fetchrow("SELECT * FROM collection_control.collections WHERE collection_id='collection' FOR UPDATE")
            completion=asyncio.create_task(self.repository.complete(task,capture,Submitter()))
            await asyncio.wait_for(entered.wait(),.5)
            await asyncio.sleep(.05)
            self.assertFalse(completion.done())
            await c.execute("UPDATE collection_control.collections SET deleted=true WHERE collection_id='collection'")
        with self.assertRaises(Fenced):await completion
        self.assertEqual(called,[])
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.jobs'),1)
