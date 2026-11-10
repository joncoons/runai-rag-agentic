"""Admission failures release the exact lease and cannot strand later revisions."""
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock
from tests.application.test_worker import ApplicationAdmissionTests
from collection_control.repository import Conflict

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'requires disposable qualification PostgreSQL')
class AdmissionFailureTests(unittest.IsolatedAsyncioTestCase):
 asyncSetUp=ApplicationAdmissionTests.asyncSetUp
 asyncTearDown=ApplicationAdmissionTests.asyncTearDown
 __getattr__=ApplicationAdmissionTests.__getattr__
 async def test_permanent_payload_rejection_is_terminal_and_fenced(self):
  from application.worker import ApplicationWorker
  from nv_ingest_durable.contracts import ClaimedMessage
  envelope,lease,body,path=await self.fixture()
  await self.pool.execute('DELETE FROM public.deadletters WHERE idempotency_key=$1',envelope.idempotency_key)
  worker=ApplicationWorker.__new__(ApplicationWorker);worker.pool=self.pool;worker.legacy=self.legacy
  message=ClaimedMessage(stream='rag.agentic.pages.index',group='application-admission-v1',redis_id='1-0',envelope=envelope)
  error=Conflict('Payload exceeds supported Milvus JSON field size')
  await worker._admission_failure(message,envelope,lease,error,'collection','missing-revision')
  row=await self.pool.fetchrow('SELECT status,lease_token,last_error FROM public.stage_units WHERE idempotency_key=$1',envelope.idempotency_key)
  self.assertEqual(row['status'],'deadlettered');self.assertIsNone(row['lease_token'])
  self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM public.deadletters WHERE idempotency_key=$1',envelope.idempotency_key),1)
  from uuid import UUID
  await self.pool.execute("INSERT INTO application.collections(collection_id,name) VALUES('collection','Admission failure fixture') ON CONFLICT DO NOTHING")
  await self.pool.execute("INSERT INTO application.jobs(job_id,collection_id) VALUES($1,'collection') ON CONFLICT DO NOTHING",UUID(envelope.job_id))
  worker.transport=SimpleNamespace(publish=AsyncMock(return_value='2-0'))
  self.assertEqual(await worker.relay_once(),1)
  self.assertEqual(worker.transport.publish.await_args.args[0],'rag.agentic.admission.deadletter')

  await worker._admission_failure(message,envelope,'00000000-0000-0000-0000-000000000000',error,'collection','missing-revision')
  self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM public.deadletters WHERE idempotency_key=$1',envelope.idempotency_key),1)
 async def test_retryable_failure_releases_lease_and_fifth_failure_is_terminal(self):
  from application.worker import ApplicationWorker
  from nv_ingest_durable.contracts import ClaimedMessage
  envelope,lease,body,path=await self.fixture()
  await self.pool.execute('DELETE FROM public.deadletters WHERE idempotency_key=$1',envelope.idempotency_key)
  worker=ApplicationWorker.__new__(ApplicationWorker);worker.pool=self.pool;worker.legacy=self.legacy
  message=ClaimedMessage(stream='rag.agentic.pages.index',group='application-admission-v1',redis_id='2-0',envelope=envelope)
  await worker._admission_failure(message,envelope,lease,RuntimeError('transient storage error'),'collection','missing-revision')
  self.assertEqual(await self.pool.fetchval('SELECT status FROM public.stage_units WHERE idempotency_key=$1',envelope.idempotency_key),'failed')
  from datetime import timedelta
  lease=await self.legacy.acquire_lease(envelope,owner='retry',ttl=timedelta(seconds=180))
  await self.pool.execute('UPDATE public.stage_attempts SET attempt_number=5 WHERE idempotency_key=$1 AND attempt_number=2',envelope.idempotency_key)
  await self.pool.execute('UPDATE public.stage_units SET attempt_count=5 WHERE idempotency_key=$1',envelope.idempotency_key)
  await worker._admission_failure(message,envelope,lease,RuntimeError('persistent storage error'),'collection','missing-revision')
  self.assertEqual(await self.pool.fetchval('SELECT status FROM public.stage_units WHERE idempotency_key=$1',envelope.idempotency_key),'deadlettered')
