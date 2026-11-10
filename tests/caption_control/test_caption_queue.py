"""Disposable PostgreSQL contracts for caption stages over existing orchestration."""
import asyncio
from dataclasses import replace
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from uuid import uuid4
import asyncpg
import pandas as pd
from nv_ingest_durable.contracts import Stage,StageEnvelope,ClaimedMessage
from nv_ingest_durable.postgres import PostgresManifestRepository,LeaseLostError
from nv_ingest_durable.caption_repository import CaptionManifestRepository,PermanentImageError,backoff
from nv_ingest_durable.deferred_caption import CaptionHandoffStager,canonical,retain
from nv_ingest_durable.stage_processing import FusedCheckpointStager
from nv_ingest_durable.elasticsearch_writer import EsReadyCheckpointStager
from nv_ingest_durable.artifacts import FileArtifactStore


@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'Requires disposable loopback qualification PostgreSQL')
class CaptionQueueTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.pool=await asyncpg.create_pool(os.environ['COLLECTION_TEST_DSN'])
        identity=await self.pool.fetchrow('SELECT current_database() AS db,current_user AS usr')
        self.assertEqual((identity['db'],identity['usr']),('qualification','qualification'))
        await self.pool.execute('TRUNCATE public.jobs CASCADE; TRUNCATE caption_control.assets')
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)/'stages';self.root.mkdir()
        self.legacy=PostgresManifestRepository(self.pool);self.repo=CaptionManifestRepository(self.legacy,self.root)
        self.digest='sha256:'+'a'*64

    async def asyncTearDown(self):
        await self.pool.close();self.temp.cleanup()

    async def fixture(self):
        job=uuid4();document='doc-'+job.hex
        await self.pool.execute("INSERT INTO public.jobs(job_id,corpus_namespace,status,pipeline_version,metadata) VALUES($1,'caption-tests','running','async-v1','{\"ingestion_options\":{\"captions_enabled\":true}}'::jsonb)",job)
        await self.pool.execute("INSERT INTO public.documents(document_id,job_id,source_uri,source_digest,status) VALUES($1,$2,'https://fixture.invalid/page',$3,'processing')",document,job,self.digest)
        envelope=StageEnvelope(job_id=str(job),document_id=document,stage=Stage.EXTRACTION,artifact_uri='file:///source.pdf',idempotency_key='sha256:'+job.hex*2,pipeline_version='async-v1',config_digest=self.digest,model_digest=self.digest)
        # Intake owns source registration; child registration is exercised
        # exclusively through CaptionManifestRepository.commit_stage below.
        await self.pool.execute("INSERT INTO public.stage_units(idempotency_key,job_id,document_id,stage,status,input_envelope) VALUES($1,$2::uuid,$3,'extraction','pending',$4::jsonb)",envelope.idempotency_key,envelope.job_id,envelope.document_id,json.dumps(envelope.to_dict()))
        lease=await self.legacy.acquire_lease(envelope,owner='foreground',ttl=timedelta(minutes=1))
        uri,sha=retain(self.root,b'corrupt-generated-image','.image')
        row={'text':'Document body remains available','metadata':{},'text_embeddings_1b_v2':{'embedding':[1.0]+[0.0]*2047}}
        frame=pd.DataFrame([row]);frame.attrs['caption_assets']=[{'asset_uri':uri,'asset_sha256':sha,'asset_size_bytes':23,'page_number':1,'region_kind':'image','region_index':0,'bbox':None,'model_name':'test-model','model_digest':self.digest,'caption_config':{'temperature':0,'max_tokens':512},'source_metadata':{}}]
        stager=CaptionHandoffStager(FusedCheckpointStager(EsReadyCheckpointStager(self.root,self.digest,self.digest,2048),self.digest,self.digest),self.root)
        pending,downstream=await stager.stage(envelope,frame)
        artifact=await FileArtifactStore(self.root).finalize(pending)
        child=next(e for _,e in downstream if e.stage==Stage.CAPTION)
        return envelope,lease,artifact,downstream,child,frame,stager

    async def queued(self):
        items=await self.fixture();envelope,lease,artifact,downstream,*_=items
        await self.repo.commit_stage(envelope,lease_token=lease,artifact=artifact,downstream=downstream)
        return items

    def message(self,envelope):
        return ClaimedMessage(stream='ingest.caption.pending',group='caption-test',redis_id='1-0',envelope=envelope)

    async def test_foreground_commit_enqueues_caption_atomically(self):
        envelope,lease,artifact,downstream,child,*_=await self.queued()
        self.assertEqual(await self.pool.fetchval('SELECT status FROM public.stage_units WHERE idempotency_key=$1',envelope.idempotency_key),'committed')
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM caption_control.assets'),1)
        events=await self.repo.fetch_unpublished_outbox(limit=100)
        self.assertEqual([event.envelope.idempotency_key for event in events],[child.idempotency_key])
        body=json.loads(Path(artifact.uri.removeprefix('file://')).read_text())
        self.assertEqual(len(body['documents']),1)

    async def test_caption_enqueue_failure_rolls_back_document_commit(self):
        envelope,lease,artifact,downstream,*_=await self.fixture()
        await self.pool.execute("CREATE FUNCTION caption_control.reject_insert() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'injected'; END$$; CREATE TRIGGER reject_caption BEFORE INSERT ON caption_control.assets FOR EACH ROW EXECUTE FUNCTION caption_control.reject_insert()")
        try:
            with self.assertRaises(asyncpg.RaiseError):await self.repo.commit_stage(envelope,lease_token=lease,artifact=artifact,downstream=downstream)
            self.assertEqual(await self.pool.fetchval('SELECT status FROM public.stage_units WHERE idempotency_key=$1',envelope.idempotency_key),'running')
            self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM public.outbox_events'),0)
        finally:await self.pool.execute('DROP TRIGGER reject_caption ON caption_control.assets; DROP FUNCTION caption_control.reject_insert()')

    async def test_retry_is_persisted_and_cannot_be_claimed_or_relayed_early(self):
        *_,child,frame,stager=await self.queued()
        token=await self.repo.acquire_lease(child,owner='caption',ttl=timedelta(minutes=1))
        await self.repo.record_failure(child,lease_token=token,error=TimeoutError('generated timeout'))
        self.assertIsNone(await self.repo.acquire_lease(child,owner='early',ttl=timedelta(minutes=1)))
        self.assertEqual(await self.repo.fetch_unpublished_outbox(limit=100),[])
        await self.pool.execute("UPDATE caption_control.assets SET next_attempt_at=clock_timestamp()-interval '1 second'")
        self.assertIsNotNone(await self.repo.acquire_lease(child,owner='due',ttl=timedelta(minutes=1)))
        self.assertEqual(await self.pool.fetchval('SELECT attempt_count FROM public.stage_units WHERE idempotency_key=$1',child.idempotency_key),2)

    async def test_crash_reclaims_cannot_exceed_four_attempts(self):
        *_,child,frame,stager=await self.queued()
        for attempt in range(4):
            self.assertIsNotNone(await self.repo.acquire_lease(child,owner='crash-'+str(attempt),ttl=timedelta(minutes=1)))
            await self.pool.execute("UPDATE public.stage_units SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE idempotency_key=$1",child.idempotency_key)
        self.assertIsNone(await self.repo.acquire_lease(child,owner='fifth',ttl=timedelta(minutes=1)))
        self.assertTrue(await self.repo.terminal(self.message(child)))
        self.assertEqual(await self.pool.fetchval('SELECT attempt_count FROM public.stage_units WHERE idempotency_key=$1',child.idempotency_key),4)

    async def test_permanent_image_failure_is_terminal_without_retry_or_parent_failure(self):
        envelope,_,_,_,child,*_=await self.queued()
        token=await self.repo.acquire_lease(child,owner='caption',ttl=timedelta(minutes=1))
        await self.repo.record_failure(child,lease_token=token,error=PermanentImageError('corrupt image'))
        self.assertTrue(await self.repo.terminal(self.message(child)))
        self.assertEqual(await self.pool.fetchval('SELECT status FROM public.documents WHERE document_id=$1',envelope.document_id),'processing')
        self.assertEqual(await self.pool.fetchval('SELECT attempt_count FROM public.stage_units WHERE idempotency_key=$1',child.idempotency_key),1)

    async def test_source_url_revision_and_acl_are_preserved_and_revalidated(self):
        envelope,_,_,_,child,*_=await self.queued()
        context=await self.repo.source_context(child)
        self.assertEqual(context['source_uri'],'https://fixture.invalid/page')
        self.assertEqual(context['source_digest'],self.digest)
        await self.pool.execute('UPDATE public.jobs SET metadata=$2::jsonb WHERE job_id=$1',__import__('uuid').UUID(envelope.job_id),json.dumps({'acl':['changed-scope'],'ingestion_options':{'captions_enabled':True}}))
        with self.assertRaises(PermanentImageError):await self.repo.source_context(child)

    async def test_source_url_change_invalidates_pending_caption(self):
        _,_,_,_,child,*_=await self.queued()
        await self.pool.execute("UPDATE public.documents SET source_uri='https://fixture.invalid/changed' WHERE document_id=$1",child.document_id)
        with self.assertRaises(PermanentImageError):await self.repo.source_context(child)

    async def test_concurrent_claims_consume_only_one_attempt(self):
        _,_,_,_,child,*_=await self.queued()
        tokens=await asyncio.gather(*(self.repo.acquire_lease(child,owner='duplicate-'+str(n),ttl=timedelta(minutes=1)) for n in range(5)))
        self.assertEqual(sum(value is not None for value in tokens),1)
        self.assertEqual(await self.pool.fetchval('SELECT attempt_count FROM public.stage_units WHERE idempotency_key=$1',child.idempotency_key),1)

    async def test_terminal_disposition_cannot_overwrite_active_or_committed_stage(self):
        _,_,artifact,_,child,*_=await self.queued()
        token=await self.repo.acquire_lease(child,owner='active',ttl=timedelta(minutes=1))
        await self.pool.execute("UPDATE caption_control.assets SET terminal_reason='simulated stale disposition' WHERE stage_key=$1",child.idempotency_key)
        self.assertFalse(await self.repo.terminal(self.message(child)))
        await self.pool.execute("UPDATE public.stage_units SET status='committed',lease_token=NULL,lease_expires_at=NULL WHERE idempotency_key=$1",child.idempotency_key)
        self.assertFalse(await self.repo.terminal(self.message(child)))
        self.assertFalse(await self.repo.deadletter(self.message(child),error=PermanentImageError('stale error'),target_stream='ingest.caption.deadletter'))
        self.assertEqual(await self.pool.fetchval('SELECT status FROM public.stage_units WHERE idempotency_key=$1',child.idempotency_key),'committed')

    async def test_enqueue_without_user_opt_in_is_rejected_atomically(self):
        envelope,lease,artifact,downstream,*_=await self.fixture()
        await self.pool.execute("UPDATE public.jobs SET metadata='{}'::jsonb WHERE job_id=$1",__import__('uuid').UUID(envelope.job_id))
        with self.assertRaises(ValueError):await self.repo.commit_stage(envelope,lease_token=lease,artifact=artifact,downstream=downstream)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM caption_control.assets'),0)
        self.assertEqual(await self.pool.fetchval('SELECT status FROM public.stage_units WHERE idempotency_key=$1',envelope.idempotency_key),'running')

    async def test_stale_lease_and_changed_source_cannot_commit_caption(self):
        _,_,artifact,_,child,*_=await self.queued()
        first=await self.repo.acquire_lease(child,owner='first',ttl=timedelta(minutes=1))
        await self.pool.execute("UPDATE public.stage_units SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE idempotency_key=$1",child.idempotency_key)
        second=await self.repo.acquire_lease(child,owner='second',ttl=timedelta(minutes=1))
        with self.assertRaises(LeaseLostError):await self.repo.commit_stage(child,lease_token=first,artifact=artifact,downstream=())
        await self.pool.execute("UPDATE public.documents SET source_digest='changed' WHERE document_id=$1",child.document_id)
        with self.assertRaises(PermanentImageError):await self.repo.commit_stage(child,lease_token=second,artifact=artifact,downstream=())

    async def test_replayed_handoff_is_deterministic_and_cannot_change_committed_artifact(self):
        envelope,lease,artifact,downstream,child,frame,stager=await self.queued()
        pending,replayed=await stager.stage(envelope,frame)
        self.assertEqual(pending.digest,artifact.digest)
        self.assertEqual(next(e for _,e in replayed if e.stage==Stage.CAPTION),child)
        await self.repo.commit_stage(envelope,lease_token=lease,artifact=artifact,downstream=downstream)
        with self.assertRaises(ValueError):await self.repo.commit_stage(envelope,lease_token=lease,artifact=replace(artifact,digest='sha256:'+'b'*64),downstream=downstream)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM caption_control.assets'),1)

    def test_backoff_is_exponential_and_bounded(self):
        from unittest.mock import patch
        with patch('nv_ingest_durable.caption_repository.random.uniform',return_value=1):
            self.assertEqual([backoff(n) for n in range(1,5)],[1,2,4,8])
            self.assertLessEqual(backoff(20),60)

    async def committed_caption(self):
        from nv_ingest_durable.artifacts import JsonResultStager
        envelope,lease,source,downstream,child,*_=await self.queued()
        # Exercise only the real scheduling/lease/commit path. There is no SQL
        # input-envelope setup for the child; source commit must have saved it.
        token=await self.repo.acquire_lease(child,owner='generated-caption-worker',ttl=timedelta(minutes=1))
        pending,_=await JsonResultStager(self.root).stage(child,{'schema_version':'1.0','payload_format':'es_docs','documents':[{'text':'Generated caption fixture','vector':[1.0]+[0.0]*2047,'metadata':{'generated_annotation':True}}]})
        artifact=await FileArtifactStore(self.root).finalize(pending)
        ready=replace(child,stage=Stage.INDEX,artifact_uri=artifact.uri)
        await self.repo.commit_stage(child,lease_token=token,artifact=artifact,downstream=(('ingest.caption.results',ready),))
        return envelope,lease,source,downstream,child,artifact

    async def test_real_caption_child_commit_preserves_input_without_manual_sql_identity_setup(self):
        _,_,_,_,child,artifact=await self.committed_caption()
        row=await self.pool.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1',child.idempotency_key)
        self.assertEqual(json.loads(row['input_envelope']),child.to_dict())
        self.assertEqual((row['status'],row['attempt_count'],row['artifact_digest']),('committed',1,artifact.digest))
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM public.outbox_events WHERE target_stream=$1','ingest.caption.results'),1)

    async def test_source_commit_registers_pending_exact_child_before_outbox_is_visible(self):
        *_,child,frame,stager=await self.queued()
        row=await self.pool.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1',child.idempotency_key)
        self.assertEqual(row['status'],'pending');self.assertEqual(row['attempt_count'],0)
        self.assertEqual(json.loads(row['input_envelope']),child.to_dict())
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM public.stage_attempts WHERE idempotency_key=$1',child.idempotency_key),0)

    async def test_committed_parent_replay_backfills_only_missing_child_input_preserving_caption_result(self):
        envelope,lease,source,downstream,child,artifact=await self.committed_caption()
        await self.pool.execute('UPDATE public.stage_units SET input_envelope=NULL WHERE idempotency_key=$1',child.idempotency_key)
        before=dict(await self.pool.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1',child.idempotency_key));before.pop('input_envelope')
        attempts=[dict(r) for r in await self.pool.fetch('SELECT * FROM public.stage_attempts ORDER BY attempt_id')]
        outbox=[dict(r) for r in await self.pool.fetch('SELECT * FROM public.outbox_events ORDER BY event_id')]
        await self.repo.commit_stage(envelope,lease_token=None,artifact=source,downstream=downstream)
        after=dict(await self.pool.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1',child.idempotency_key))
        self.assertEqual(json.loads(after.pop('input_envelope')),child.to_dict());self.assertEqual(before,after)
        self.assertEqual(attempts,[dict(r) for r in await self.pool.fetch('SELECT * FROM public.stage_attempts ORDER BY attempt_id')])
        self.assertEqual(outbox,[dict(r) for r in await self.pool.fetch('SELECT * FROM public.outbox_events ORDER BY event_id')])
        await self.repo.commit_stage(envelope,lease_token=None,artifact=source,downstream=downstream)
        self.assertEqual(await self.pool.fetchval('SELECT artifact_digest FROM public.stage_units WHERE idempotency_key=$1',child.idempotency_key),artifact.digest)

    async def test_retry_message_fields_do_not_replace_original_persisted_child_identity(self):
        _,_,_,_,child,*_=await self.queued()
        token=await self.repo.acquire_lease(child,owner='first',ttl=timedelta(minutes=1))
        await self.repo.record_failure(child,lease_token=token,error=TimeoutError('generated fixture'))
        await self.pool.execute("UPDATE caption_control.assets SET next_attempt_at=clock_timestamp()-interval '1 second'")
        retry=replace(child,message_id=str(uuid4()),attempt=1)
        self.assertIsNotNone(await self.repo.acquire_lease(retry,owner='retry',ttl=timedelta(minutes=1)))
        stored=await self.pool.fetchval('SELECT input_envelope FROM public.stage_units WHERE idempotency_key=$1',child.idempotency_key)
        self.assertEqual(json.loads(stored),child.to_dict())

    async def test_mismatched_child_identity_rolls_back_parent_assets_and_outbox_atomically(self):
        envelope,lease,source,downstream,child,*_=await self.fixture()
        await self.pool.execute("INSERT INTO public.stage_units(idempotency_key,job_id,document_id,stage,status,input_envelope) VALUES($1,$2::uuid,$3,'caption','pending',$4::jsonb)",child.idempotency_key,child.job_id,child.document_id,json.dumps(replace(child,model_digest='sha256:'+'f'*64).to_dict()))
        with self.assertRaises(ValueError):await self.repo.commit_stage(envelope,lease_token=lease,artifact=source,downstream=downstream)
        self.assertEqual(await self.pool.fetchval('SELECT status FROM public.stage_units WHERE idempotency_key=$1',envelope.idempotency_key),'running')
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM caption_control.assets'),0)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM public.outbox_events'),0)

    async def test_acquisition_rejects_missing_or_changed_persisted_processing_identity(self):
        _,_,_,_,child,*_=await self.queued()
        with self.assertRaises(PermanentImageError):await self.repo.acquire_lease(replace(child,config_digest='sha256:'+'f'*64),owner='wrong-config',ttl=timedelta(minutes=1))
        await self.pool.execute('UPDATE public.stage_units SET input_envelope=NULL WHERE idempotency_key=$1',child.idempotency_key)
        with self.assertRaises(PermanentImageError):await self.repo.acquire_lease(child,owner='missing-input',ttl=timedelta(minutes=1))
        self.assertEqual(await self.pool.fetchval('SELECT attempt_count FROM public.stage_units WHERE idempotency_key=$1',child.idempotency_key),0)

    async def test_committed_parent_recovery_rejects_tampered_retained_manifest_without_changing_child(self):
        envelope,_,source,downstream,child,*_=await self.queued()
        await self.pool.execute('UPDATE public.stage_units SET input_envelope=NULL WHERE idempotency_key=$1',child.idempotency_key)
        Path(child.artifact_uri.removeprefix('file://')).write_text('changed retained manifest')
        with self.assertRaises(ValueError):await self.repo.commit_stage(envelope,lease_token=None,artifact=source,downstream=downstream)
        self.assertIsNone(await self.pool.fetchval('SELECT input_envelope FROM public.stage_units WHERE idempotency_key=$1',child.idempotency_key))
        self.assertEqual(await self.pool.fetchval('SELECT attempt_count FROM public.stage_units WHERE idempotency_key=$1',child.idempotency_key),0)

    async def test_operator_dry_run_and_apply_use_actual_committed_extraction_without_replaying_caption(self):
        import importlib.util
        from types import SimpleNamespace
        from unittest.mock import patch
        from uuid import UUID
        for path in ('deployment/collection-control/001_initial.sql','deployment/collection-control/002_stage_admissions.sql','deployment/collection-control/003_caption_admissions.sql','deployment/application/001_application.sql'):
            await self.pool.execute(Path(path).read_text())
        envelope,_,_,_,child,artifact=await self.committed_caption()
        await self.pool.execute("INSERT INTO collection_control.collections(collection_id,contract_sha256) VALUES('caption-tests',$1) ON CONFLICT DO NOTHING",'d'*64)
        await self.pool.execute("INSERT INTO application.collections(collection_id,name) VALUES('caption-tests','Generated recovery fixture') ON CONFLICT DO NOTHING")
        await self.pool.execute("INSERT INTO application.jobs(job_id,collection_id) VALUES($1,'caption-tests')",UUID(envelope.job_id))
        await self.pool.execute('UPDATE public.stage_units SET input_envelope=NULL WHERE idempotency_key=$1',child.idempotency_key)
        spec=importlib.util.spec_from_file_location('recovery_operator',Path('deployment/application/recover_caption_input.py'))
        operator=importlib.util.module_from_spec(spec);spec.loader.exec_module(operator)
        args=SimpleNamespace(caption_stage_key=child.idempotency_key,apply=False)
        with patch.dict(os.environ,{'POSTGRES_DSN':os.environ['COLLECTION_TEST_DSN'],'ARTIFACT_ROOT':str(self.root.parent)}):
            preview=await operator.run(args)
            self.assertEqual(preview['missing_input_count'],1)
            self.assertEqual(preview['model_calls'],0)
            self.assertIsNone(await self.pool.fetchval('SELECT input_envelope FROM public.stage_units WHERE idempotency_key=$1',child.idempotency_key))
            args.apply=True;result=await operator.run(args)
        self.assertEqual(result['recovered_input_count'],1)
        self.assertTrue(result['attempts_artifacts_statuses_outbox_preserved'])
        row=await self.pool.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1',child.idempotency_key)
        self.assertEqual(json.loads(row['input_envelope']),child.to_dict())
        self.assertEqual((row['attempt_count'],row['status'],row['artifact_digest']),(1,'committed',artifact.digest))
