"""Real PostgreSQL lifecycle tests; only an isolated qualification database can reset."""
import asyncio
from datetime import datetime, timezone, timedelta
import json
import os
from pathlib import Path
import unittest
from urllib.parse import urlsplit
from uuid import uuid4

UTC = timezone.utc


class WindowTests(unittest.TestCase):
    def test_dst_gap_skips_and_fold_uses_earlier_occurrence(self):
        from application.source_registry import schedule_window, next_window, validate_schedule
        schedule = validate_schedule({'enabled': True, 'timezone': 'America/Chicago', 'start': '02:30', 'end': '06:00'})
        self.assertIsNone(schedule_window(schedule, datetime(2026, 3, 8, 9, tzinfo=UTC)))
        self.assertEqual(next_window(schedule, datetime(2026, 3, 8, 9, tzinfo=UTC))[0], datetime(2026, 3, 9, 7, 30, tzinfo=UTC))
        schedule['start'] = '01:30'
        self.assertEqual(schedule_window(schedule, datetime(2026, 11, 1, 7, 45, tzinfo=UTC)),
                         (datetime(2026, 11, 1, 6, 30, tzinfo=UTC), datetime(2026, 11, 1, 12, tzinfo=UTC)))

    def test_strict_schedule_validation(self):
        from application.source_registry import validate_schedule
        for schedule in ({'enabled': 'false'}, {'timezone': 'Not/AZone'}, {'start': '2:00'},
                         {'start': '06:00', 'end': '02:00'}, {'unexpected': 1}, {'end': '24:00'}):
            with self.subTest(schedule=schedule), self.assertRaises(ValueError): validate_schedule(schedule)
        self.assertFalse(validate_schedule(None)['enabled'])


@unittest.skipUnless(os.environ.get('SOURCE_RECHECK_TEST_DSN'), 'requires isolated source refresh PostgreSQL')
class RegistryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import asyncpg
        dsn = os.environ['SOURCE_RECHECK_TEST_DSN']
        target = urlsplit(dsn)
        self.assertIn(target.hostname, ('localhost', '127.0.0.1'))
        self.assertEqual(target.port, 55442)
        self.pool = await asyncpg.create_pool(dsn, min_size=1, max_size=8)
        who = await self.pool.fetchrow('SELECT current_database() AS db, current_user AS usr')
        self.assertEqual((who['db'], who['usr']), ('qualification', 'qualification'))
        self.addAsyncCleanup(self.pool.close)
        await self.pool.execute('''DROP SCHEMA IF EXISTS application CASCADE; DROP SCHEMA IF EXISTS collection_control CASCADE;
            CREATE SCHEMA collection_control; CREATE SCHEMA application;
            CREATE TABLE collection_control.collections(collection_id text PRIMARY KEY, deleted boolean NOT NULL DEFAULT false);
            CREATE TABLE application.collections(collection_id text PRIMARY KEY REFERENCES collection_control.collections, name text);
            CREATE TABLE application.jobs(job_id uuid PRIMARY KEY, collection_id text REFERENCES application.collections, status text DEFAULT 'queued');
            INSERT INTO collection_control.collections VALUES('collection', false);
            INSERT INTO application.collections VALUES('collection', 'Generated qualification');''')
        await self.pool.execute(Path('deployment/application/004_source_rechecks.sql').read_text())
        await self.pool.execute(Path('deployment/application/009_streamed_website_runs.sql').read_text())
        from application.source_registry import SourceRegistry
        self.registry = SourceRegistry(self.pool)

    async def source(self, schedule=None):
        return await self.registry.create('collection', name='Generated website', seed_urls=['https://qualification.invalid/'],
                                          policy={'allowed_hosts': ['qualification.invalid']}, schedule=schedule)

    async def claimed(self):
        source = await self.source()
        await self.registry.request(source['source_id'], 'collection')
        return source, await self.registry.claim('qualification-worker')

    async def test_manual_coalescing_and_competing_claims(self):
        source = await self.source()
        self.assertFalse(source['schedule']['enabled'])
        self.assertEqual(len(await self.registry.list('collection')), 1)
        requests = await asyncio.gather(*(self.registry.request(source['source_id'], 'collection') for _ in range(8)))
        self.assertEqual(len({r['run_id'] for r in requests}), 1)
        claims = await asyncio.gather(*(self.registry.claim('worker-'+str(i)) for i in range(5)))
        claimed = [r for r in claims if r]
        self.assertEqual(len(claimed), 1)
        self.assertEqual((claimed[0]['state'], claimed[0]['attempts'], claimed[0]['fence']), ('scanning', 1, 1))
        self.assertIsNone(claimed[0]['baseline'])
        self.assertGreater(claimed[0]['deadline']-claimed[0]['created_at'],timedelta(days=365))

    async def test_expired_lease_is_fenced_and_attempts_are_bounded(self):
        from collection_control.repository import Fenced
        source, first = await self.claimed()
        await self.pool.execute("UPDATE application.website_source_runs SET lease_until=clock_timestamp()-interval '1 second' WHERE run_id=$1", first['run_id'])
        second = await self.registry.claim('takeover')
        self.assertEqual((second['run_id'], second['fence']), (first['run_id'], 2))
        with self.assertRaises(Fenced): await self.registry.finish(first['run_id'], first['fence'], status='unchanged')
        await self.pool.execute("UPDATE application.website_source_runs SET lease_until=clock_timestamp()-interval '1 second'")
        third = await self.registry.claim('third')
        self.assertEqual(third['attempts'], 3)
        await self.pool.execute("UPDATE application.website_source_runs SET lease_until=clock_timestamp()-interval '1 second'")
        self.assertIsNone(await self.registry.claim('fourth'))
        self.assertEqual((await self.registry.history(source['source_id'], 'collection'))[0]['state'], 'failed')

    async def test_edit_pause_and_collection_delete_invalidate_scans(self):
        from collection_control.repository import Fenced, Conflict
        source, run = await self.claimed()
        changed = await self.registry.update(source['source_id'], 'collection', paused=True)
        self.assertEqual(changed['version'], 2)
        with self.assertRaises(Fenced): await self.registry.finish(run['run_id'], run['fence'], status='unchanged')
        manual = await self.registry.request(source['source_id'], 'collection')
        self.assertEqual(manual['trigger'], 'manual')
        self.assertIsNotNone(await self.registry.claim('paused-manual'))
        await self.registry.update(source['source_id'], 'collection', paused=False)
        await self.registry.request(source['source_id'], 'collection')
        await self.pool.execute("UPDATE collection_control.collections SET deleted=true")
        self.assertIsNone(await self.registry.claim('worker'))
        self.assertEqual(await self.registry.list('collection'), [])

    async def test_atomic_accept_rollback_publication_baseline_and_unchanged(self):
        from collection_control.repository import Fenced
        source, run = await self.claimed()
        job = uuid4()
        async def accept(c):
            await c.execute("INSERT INTO application.jobs VALUES($1,'collection','queued')", job)
            await self.registry.accept(c, run['run_id'], run['fence'], job_id=job, fingerprint='a'*64,
                manifest_uri='file:///generated/manifest.json', manifest_sha256='b'*64,
                observations=[{'uri':'https://qualification.invalid/','raw_sha256':'c'*64,'state':'present'}], counts={'pages':1})
        with self.assertRaises(RuntimeError):
            async with self.pool.acquire() as c, c.transaction():
                await accept(c)
                raise RuntimeError('qualification rollback')
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.jobs'), 0)
        self.assertEqual((await self.registry.history(source['source_id'], 'collection'))[0]['state'], 'scanning')
        async with self.pool.acquire() as c, c.transaction(): await accept(c)
        self.assertIsNone(await self.registry.claim('other'))
        with self.assertRaises(Fenced): await self.registry.finish(run['run_id'], run['fence'], status='failed')
        await self.registry.update(source['source_id'], 'collection', paused=True)
        self.assertEqual((await self.registry.history(source['source_id'], 'collection'))[0]['state'], 'accepted')
        await self.pool.execute("UPDATE application.jobs SET status='completed'")
        await self.registry.reconcile()
        self.assertEqual((await self.registry.list('collection'))[0]['last_successful_run_id'], run['run_id'])
        await self.registry.update(source['source_id'], 'collection', paused=False)
        await self.registry.request(source['source_id'], 'collection')
        again = await self.registry.claim('again')
        self.assertEqual(again['baseline']['fingerprint'], 'a'*64)
        await self.registry.finish(again['run_id'], again['fence'], status='unchanged', counts={'pages':1})
        self.assertEqual((await self.registry.list('collection'))[0]['last_successful_run_id'], run['run_id'])
        history = await self.registry.history(source['source_id'], 'collection')
        self.assertEqual([r['state'] for r in history], ['unchanged', 'published'])
        self.assertEqual(history[1]['counts'], {'pages':1})

    async def test_due_fire_is_current_window_only_and_unique(self):
        source = await self.source({'enabled':True,'timezone':'UTC','start':'02:00','end':'06:00'})
        self.assertEqual(await self.registry.enqueue_due(datetime(2026, 1, 1, 7, tzinfo=UTC)), 0)
        self.assertEqual(await self.registry.enqueue_due(datetime(2026, 1, 2, 3, tzinfo=UTC)), 1)
        self.assertEqual(await self.registry.enqueue_due(datetime(2026, 1, 2, 4, tzinfo=UTC)), 0)
        rows = await self.registry.history(source['source_id'], 'collection')
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['nominal_fire'], datetime(2026, 1, 2, 2, tzinfo=UTC))
        self.assertEqual((await self.registry.list('collection'))[0]['next_fire'], datetime(2026, 1, 3, 2, tzinfo=UTC))

    async def test_accept_rejects_expiry_edit_and_delete(self):
        from collection_control.repository import Fenced
        source, run = await self.claimed()
        await self.pool.execute("UPDATE collection_control.collections SET deleted=true")
        async with self.pool.acquire() as c, c.transaction():
            with self.assertRaises(Fenced):
                await self.registry.accept(c, run['run_id'], run['fence'], job_id=uuid4(), fingerprint='x',
                    manifest_uri='file:///generated', manifest_sha256='y', observations=[], counts={})

    async def test_daily_deferred_restarts_same_run_next_window(self):
        now = datetime.now(UTC)
        schedule = {'enabled':True,'timezone':'UTC','start':'00:00','end':'23:59'}
        source = await self.source(schedule)
        await self.registry.enqueue_due(now)
        run = await self.registry.claim('first')
        self.assertIsNotNone(run)
        await self.registry.finish(run['run_id'], run['fence'], status='deferred', error_code='window_expired')
        self.assertIsNone(await self.registry.claim('too-early'))
        tomorrow = now + timedelta(days=1)
        self.assertEqual(await self.registry.enqueue_due(tomorrow), 0)
        rows = await self.registry.history(source['source_id'], 'collection')
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['state'], 'queued')
        self.assertEqual(rows[0]['run_id'], run['run_id'])

    async def test_expired_accept_and_deadline_never_link_a_job(self):
        from collection_control.repository import Fenced
        source, run = await self.claimed()
        job = uuid4()
        await self.pool.execute("INSERT INTO application.jobs VALUES($1,'collection','queued')", job)
        for field in ('lease_until', 'deadline'):
            await self.pool.execute("UPDATE application.website_source_runs SET lease_until=clock_timestamp()+interval '5 minutes', deadline=clock_timestamp()+interval '5 minutes'")
            await self.pool.execute('UPDATE application.website_source_runs SET '+field+"=clock_timestamp()-interval '1 second'")
            with self.subTest(field=field), self.assertRaises(Fenced):
                async with self.pool.acquire() as c, c.transaction():
                    await self.registry.accept(c, run['run_id'], run['fence'], job_id=job, fingerprint='a'*64,
                        manifest_uri='file:///generated', manifest_sha256='b'*64, observations=[], counts={})
            self.assertIsNone(await self.pool.fetchval('SELECT job_id FROM application.website_source_runs'))

    async def test_failed_publication_never_advances_baseline(self):
        source, run = await self.claimed()
        job = uuid4()
        async with self.pool.acquire() as c, c.transaction():
            await c.execute("INSERT INTO application.jobs VALUES($1,'collection','queued')", job)
            await self.registry.accept(c, run['run_id'], run['fence'], job_id=job, fingerprint='a'*64,
                manifest_uri='file:///generated', manifest_sha256='b'*64, observations=[], counts={})
        await self.pool.execute("UPDATE application.jobs SET status='failed'")
        await asyncio.gather(self.registry.reconcile(), self.registry.reconcile())
        self.assertIsNone((await self.registry.list('collection'))[0]['last_successful_run_id'])
        self.assertEqual((await self.registry.history(source['source_id'], 'collection'))[0]['state'], 'failed')

    async def test_concurrent_due_ticks_coalesce_and_schedule_edits_fence(self):
        from collection_control.repository import Fenced
        now = datetime.now(UTC)
        source = await self.source({'enabled':True,'timezone':'UTC','start':'00:00','end':'23:59'})
        totals = await asyncio.gather(*(self.registry.enqueue_due(now) for _ in range(5)))
        self.assertEqual(sum(totals), 1)
        run = await self.registry.claim('first')
        await self.registry.update(source['source_id'], 'collection', schedule={'enabled':False})
        self.assertEqual(await self.registry.enqueue_due(now), 0)
        with self.assertRaises(Fenced): await self.registry.finish(run['run_id'], run['fence'], status='unchanged')
        self.assertEqual((await self.registry.history(source['source_id'], 'collection'))[0]['state'], 'cancelled')

    async def test_manual_deadline_failure_releases_overlap(self):
        source, run = await self.claimed()
        await self.pool.execute("UPDATE application.website_source_runs SET deadline=clock_timestamp()-interval '1 second',lease_until=clock_timestamp()-interval '1 second'")
        self.assertIsNone(await self.registry.claim('after-expiry'))
        history = await self.registry.history(source['source_id'], 'collection')
        self.assertEqual((history[0]['state'], history[0]['error_code']), ('failed', 'deadline_expired'))
        another = await self.registry.request(source['source_id'], 'collection')
        self.assertNotEqual(another['run_id'], run['run_id'])

    async def test_previous_window_expired_scan_restarts_in_current_window(self):
        now = datetime.now(UTC)
        source = await self.source({'enabled':True,'timezone':'UTC','start':'00:00','end':'23:59'})
        await self.registry.enqueue_due(now-timedelta(days=1))
        await self.pool.execute("UPDATE application.website_source_runs SET state='scanning',attempts=1,fence=1,lease_until=clock_timestamp()-interval '1 day'")
        self.assertEqual(await self.registry.enqueue_due(now), 0)
        resumed = await self.registry.claim('restart')
        self.assertIsNotNone(resumed)
        self.assertEqual(resumed['attempts'], 2)
        self.assertEqual(resumed['deadline'].date(), now.date())
        self.assertEqual(len(await self.registry.history(source['source_id'], 'collection')), 1)

    async def test_saved_policy_excludes_server_private_scope(self):
        source = await self.source()
        self.assertNotIn('private_hosts', source['policy'])
        self.assertNotIn('private_ports', source['policy'])
        with self.assertRaises(ValueError):
            await self.registry.create('collection', name='bad private scope', seed_urls=['https://qualification.invalid/'],
                policy={'allowed_hosts':['qualification.invalid'],'private_hosts':['qualification.invalid']}, schedule=None)

    async def test_registration_rejects_credentials_and_invalid_schedule(self):
        for url in ('https://user:password@qualification.invalid/', 'https://qualification.invalid/?token=secret'):
            with self.assertRaises(ValueError):
                await self.registry.create('collection', name='bad', seed_urls=[url], policy={}, schedule=None)
        with self.assertRaises(ValueError): await self.source({'enabled':True,'start':'18:00','end':'02:00'})

    async def test_manual_queue_lease_is_renewable_without_crawl_time_ceiling(self):
        from collection_control.repository import Fenced
        source,run=await self.claimed()
        self.assertGreater(run['deadline'],datetime.now(UTC)+timedelta(days=365))
        old=run['lease_until']
        renewed=await self.registry.renew(run['run_id'],run['fence'],ttl=600)
        self.assertGreater(renewed,old)
        await self.pool.execute("UPDATE application.website_source_runs SET lease_until=clock_timestamp()-interval '1 second' WHERE run_id=$1",run['run_id'])
        with self.assertRaises(Fenced):await self.registry.renew(run['run_id'],run['fence'])
