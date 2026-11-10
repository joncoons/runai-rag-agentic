import asyncio
import json
import os
import unittest
from pathlib import Path

import asyncpg

from collection_control.repository import ControlRepository, Conflict, Fenced


@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'), 'Requires disposable local PostgreSQL')
class RepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from urllib.parse import urlparse
        dsn=os.environ['COLLECTION_TEST_DSN']
        self.assertIn(urlparse(dsn).hostname,('127.0.0.1','localhost'))
        self.pool = await asyncpg.create_pool(dsn)
        who=await self.pool.fetchrow('SELECT current_database() AS db,current_user AS usr')
        self.assertEqual((who['db'],who['usr']),('qualification','qualification'))
        await self.pool.execute(Path('deployment/collection-control/001_initial.sql').read_text())
        await self.pool.execute('TRUNCATE collection_control.collections CASCADE')
        self.repo = ControlRepository(self.pool)
        await self.repo.create_collection('test', 'a'*64)

    async def asyncTearDown(self):
        await self.pool.close()

    async def intent(self, key='one', kind='snapshot', revision=0):
        return await self.repo.prepare('test', key, kind, revision, 'b'*64, 'a'*64)

    async def publish(self, op):
        lease = await self.repo.claim(op['operation_id'], ttl_seconds=60)
        for target in lease['targets']:
            await self.repo.receipt(lease, target, 'c'*64, 4)
        await self.repo.publish(lease)
        return lease

    async def test_intent_outbox_and_idempotency_are_atomic(self):
        op = await self.intent()
        self.assertEqual(op['operation_id'], (await self.intent())['operation_id'])
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.outbox'), 1)
        with self.assertRaises(Conflict):
            await self.repo.prepare('test','one','snapshot',0,'d'*64,'a'*64)
        with self.assertRaises(Conflict):
            await self.intent('other')
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),1)

    async def test_partial_receipts_do_not_publish_and_previous_pair_survives(self):
        first=await self.publish(await self.intent())
        op=await self.intent('enroll','enroll_milvus',1)
        lease=await self.repo.claim(op['operation_id'],ttl_seconds=60)
        self.assertEqual(lease['targets'],['elasticsearch','milvus'])
        await self.repo.receipt(lease,'elasticsearch','c'*64,4)
        with self.assertRaises(Conflict): await self.repo.publish(lease)
        self.assertEqual((await self.repo.resolve('test'))['operation_id'],first['operation_id'])
        await self.repo.receipt(lease,'milvus','c'*64,4)
        await self.repo.publish(lease)
        manifest=await self.repo.resolve('test')
        self.assertEqual(manifest['revision'],2)
        self.assertEqual(manifest['target_epoch'],1)
        self.assertEqual(set(manifest['handles']),{'elasticsearch','milvus'})
        nxt=await self.intent('later','snapshot',2)
        self.assertEqual(nxt['targets'],['elasticsearch','milvus'])

    async def test_mismatched_content_receipt_blocks_publication(self):
        await self.publish(await self.intent())
        op=await self.intent('dual','enroll_milvus',1)
        lease=await self.repo.claim(op['operation_id'],ttl_seconds=60)
        await self.repo.receipt(lease,'elasticsearch','c'*64,4)
        await self.repo.receipt(lease,'milvus','d'*64,4)
        with self.assertRaises(Conflict):await self.repo.publish(lease)

    async def test_reclaim_has_new_handles_and_rejects_late_receipts(self):
        op=await self.intent()
        old=await self.repo.claim(op['operation_id'],ttl_seconds=60)
        self.assertIsNone(await self.repo.claim(op['operation_id'],ttl_seconds=60))
        await self.pool.execute("UPDATE collection_control.operations SET lease_until=clock_timestamp()-interval '1 second'")
        new=await self.repo.claim(op['operation_id'],ttl_seconds=60)
        self.assertNotEqual(old['handles'],new['handles'])
        with self.assertRaises(Fenced):await self.repo.receipt(old,'elasticsearch','c'*64,4)
        with self.assertRaises(Fenced):await self.repo.publish(old)
        await self.repo.receipt(new,'elasticsearch','c'*64,4)
        await self.repo.publish(new)
        self.assertEqual((await self.repo.resolve('test'))['handles'],new['handles'])

    async def test_deletion_denies_reads_and_fences_pending_enrollment(self):
        await self.publish(await self.intent())
        lease=await self.repo.claim((await self.intent('enroll','enroll_milvus',1))['operation_id'],ttl_seconds=60)
        await self.repo.delete_collection('test')
        with self.assertRaises(Conflict):await self.repo.resolve('test')
        with self.assertRaises(Fenced):await self.repo.receipt(lease,'elasticsearch','c'*64,4)
        with self.assertRaises(Conflict):await self.intent('new','snapshot',1)
        self.assertEqual(await self.pool.fetchval("SELECT count(*) FROM collection_control.outbox WHERE event_type='collection_deleted'"),1)

    async def test_contract_revision_targets_and_receipts_are_not_caller_controlled(self):
        with self.assertRaises(Conflict):await self.repo.prepare('test','x','snapshot',1,'b'*64,'a'*64)
        with self.assertRaises(Conflict):await self.repo.prepare('test','x','snapshot',0,'b'*64,'d'*64)
        lease=await self.repo.claim((await self.intent())['operation_id'],ttl_seconds=60)
        with self.assertRaises(Conflict):await self.repo.receipt(lease,'milvus','c'*64,4)
        with self.assertRaises(Conflict):await self.repo.receipt(lease,'elasticsearch','short',4)

    async def test_attempt_history_and_read_release_gate_survive_reclaim(self):
        first=await self.publish(await self.intent())
        manifest=await self.repo.resolve('test')
        await self.repo.revalidate(manifest)
        op=await self.intent('two','snapshot',1)
        old=await self.repo.claim(op['operation_id'],ttl_seconds=60)
        await self.pool.execute("UPDATE collection_control.operations SET lease_until=clock_timestamp()-interval '1 second' WHERE operation_id=$1",op['operation_id'])
        new=await self.repo.claim(op['operation_id'],ttl_seconds=60)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.attempts WHERE operation_id=$1',op['operation_id']),2)
        await self.repo.receipt(new,'elasticsearch','c'*64,4)
        await self.repo.publish(new)
        with self.assertRaises(Fenced):await self.repo.revalidate(manifest)
        await self.repo.delete_collection('test')
        event=json.loads(await self.pool.fetchval("SELECT payload::text FROM collection_control.outbox WHERE event_type='collection_deleted'"))
        self.assertEqual(len(event['retained_generations']),3)

    async def test_remove_projection_preserves_es_and_advances_membership(self):
        await self.publish(await self.intent('first','enroll_milvus'))
        old=await self.repo.resolve('test')
        self.assertEqual(old['target_epoch'],1)
        lease=await self.publish(await self.intent('remove','remove_milvus',1))
        self.assertEqual(lease['targets'],['elasticsearch'])
        current=await self.repo.resolve('test')
        self.assertEqual(current['target_epoch'],2)
        self.assertEqual(set(current['handles']),{'elasticsearch'})
        with self.assertRaises(Fenced):await self.repo.revalidate(old)

    async def test_concurrent_intents_and_claims_have_one_owner(self):
        results=await asyncio.gather(self.intent('left'),self.intent('right'),return_exceptions=True)
        winners=[x for x in results if isinstance(x,dict)]
        self.assertEqual(len(winners),1)
        self.assertEqual(sum(isinstance(x,Conflict) for x in results),1)
        claims=await asyncio.gather(*[self.repo.claim(winners[0]['operation_id'],ttl_seconds=60) for _ in range(2)])
        self.assertEqual(sum(x is not None for x in claims),1)

    async def test_outbox_failure_rolls_back_intent(self):
        async def fail_event(*args):raise RuntimeError('injected-before-outbox')
        self.repo._event=fail_event
        with self.assertRaises(RuntimeError):await self.intent()
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),0)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.outbox'),0)

    async def test_cancel_preserves_publication_and_allows_corrected_intent(self):
        await self.publish(await self.intent())
        previous=await self.repo.resolve('test')
        op=await self.intent('bad','snapshot',1)
        lease=await self.repo.claim(op['operation_id'],ttl_seconds=60)
        await self.repo.cancel(lease,'artifact_admission_rejected')
        self.assertEqual(await self.repo.resolve('test'),previous)
        self.assertIsNotNone(await self.intent('corrected','snapshot',1))
        with self.assertRaises(Fenced):await self.repo.receipt(lease,'elasticsearch','c'*64,4)

    async def test_membership_changes_cannot_replace_published_content(self):
        await self.publish(await self.intent())
        with self.assertRaises(Conflict):await self.repo.prepare('test','wrong','enroll_milvus',1,'d'*64,'a'*64)
        await self.publish(await self.intent('enroll','enroll_milvus',1))
        with self.assertRaises(Conflict):await self.repo.prepare('test','wrongremove','remove_milvus',2,'d'*64,'a'*64)

    async def test_renewal_keeps_the_same_attempt_and_rejects_stale_owner(self):
        lease=await self.repo.claim((await self.intent())['operation_id'],ttl_seconds=1)
        await asyncio.sleep(.6)
        await self.repo.renew(lease,ttl_seconds=2)
        await asyncio.sleep(.6)
        await self.repo.receipt(lease,'elasticsearch','c'*64,4)
        await self.repo.publish(lease)
        self.assertFalse(await self.repo.renew(lease,ttl_seconds=2))

    async def test_slow_writer_heartbeats_while_store_is_inflight(self):
        import hashlib,tempfile
        from tests.collection_control.test_writer import fixture
        from collection_control.artifacts import canonical,content_digest
        from collection_control.writer import GenerationWriter
        artifact=fixture();contract=hashlib.sha256(canonical(artifact['contract'])).hexdigest()
        raw=canonical(artifact);sha=hashlib.sha256(raw).hexdigest()
        await self.repo.create_collection('slow',contract)
        op=await self.repo.prepare('slow','one','snapshot',0,sha,contract)
        class SlowStore:
            async def write_verify(self,*args):
                await asyncio.sleep(1.4)
                return content_digest(artifact['records']),1
        with tempfile.TemporaryDirectory() as d:
            (Path(d)/(sha+'.json')).write_bytes(raw)
            self.assertTrue(await GenerationWriter(self.repo,Path(d),{'elasticsearch':SlowStore()},allow_synthetic=True).run(op['operation_id'],ttl_seconds=1))
        self.assertEqual((await self.repo.resolve('slow'))['revision'],1)

    async def test_writer_admission_failure_cancels_without_deleting_collection(self):
        import tempfile
        from collection_control.writer import GenerationWriter
        op=await self.intent('bad')
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(Conflict):await GenerationWriter(self.repo,Path(d),{},allow_synthetic=True).run(op['operation_id'])
        self.assertEqual(await self.pool.fetchval('SELECT status FROM collection_control.operations WHERE operation_id=$1',op['operation_id']),'cancelled')
        self.assertIsNotNone(await self.intent('corrected'))
