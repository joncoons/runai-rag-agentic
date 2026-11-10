"""Disposable-PG admission and retry fence regressions; no external models."""
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID
import unittest
from tests.collection_control import test_stage_bridge as fixtures
from collection_control.artifacts import canonical
from collection_control.repository import Conflict,Fenced

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'requires disposable qualification database')
class ApplicationAdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.h=fixtures.StageBridgeTests();await self.h.asyncSetUp()
        self.pool=self.h.pool
        await self.pool.execute(Path('deployment/ingestion/captioning/001_caption_jobs.sql').read_text())
        await self.pool.execute(Path('deployment/collection-control/003_caption_admissions.sql').read_text())
        await self.pool.execute(Path('deployment/application/001_application.sql').read_text())

    async def asyncTearDown(self):await self.h.asyncTearDown()

    def __getattr__(self,name):
        if name=="h":raise AttributeError(name)
        return getattr(self.h,name)

    async def test_native_checkpoint_producer_admits_then_publishes_and_reconciles(self):
        from application.provenance import enrich_checkpoint
        from application.worker import ApplicationWorker
        envelope,lease,body,raw_path=await self.fixture()
        job=UUID(envelope.job_id)
        revision={'source_id':'source:new','source_revision_id':'capture:new','collection_id':'collection','document_id':envelope.document_id,'job_id':str(job),'canonical_uri':'https://qualification.invalid/doc-a','raw_uri':raw_path.as_uri(),'raw_sha256':hashlib.sha256(raw_path.read_bytes()).hexdigest(),'observed_at':'2026-11-08T00:00:00Z','ingestion_run_id':str(job),'filename':'test.pdf','page_count':1,'previous_source_revision_id':None}
        await self.pool.execute("UPDATE public.jobs SET metadata=jsonb_set(metadata,'{ingestion_run_id}',to_jsonb($2::text)) WHERE job_id=$1",job,str(job))
        body['documents'][0]['metadata']['content_metadata']['page_number']=1
        body['native_page_completions']={'schema_version':'native-page-completions.v1','pages':[{'page_number':1,'status':'complete'}]}
        enriched,manifest=enrich_checkpoint(body,revision,raw_root=self.raw_root,model_digest='e'*64,config_digest='c'*64,ocr_model_digest='a'*64)
        raw=canonical(manifest);sha=hashlib.sha256(raw).hexdigest();path=self.raw_root/(sha+'.json');path.write_bytes(raw)
        await self.pool.execute("UPDATE public.jobs SET metadata=metadata||$2::jsonb WHERE job_id=$1",job,json.dumps({'acl':['local-operator'],'source_manifests':{envelope.document_id:{'uri':path.as_uri(),'sha256':sha}}}))
        await self.pool.execute("INSERT INTO application.collections(collection_id,name) VALUES('collection','Generated test')")
        await self.pool.execute("INSERT INTO application.jobs(job_id,collection_id) VALUES($1,'collection')",job)
        await self.pool.execute("INSERT INTO application.sources(source_id,collection_id,canonical_uri) VALUES('source:new','collection',$1)",revision['canonical_uri'])
        await self.pool.execute("""INSERT INTO application.source_revisions(source_revision_id,source_id,collection_id,document_id,job_id,canonical_uri,filename,raw_sha256,raw_uri,observed_at,ingestion_run_id,page_count,status) VALUES('capture:new','source:new','collection',$1,$2,$3,'test.pdf',$4,$5,'2026-11-08T00:00:00Z',$6,1,'publishing')""",envelope.document_id,job,revision['canonical_uri'],revision['raw_sha256'],revision['raw_uri'],str(job))
        await self.bridge.commit_stage(envelope,lease_token=lease,artifact=self.finalized(envelope,enriched),downstream=())
        op=await self.pool.fetchval('SELECT operation_id FROM collection_control.stage_admissions WHERE stage_key=$1',envelope.idempotency_key)
        await self.pool.execute("UPDATE application.source_revisions SET operation_id=$1",op)
        await self.publish(op)
        worker=ApplicationWorker.__new__(ApplicationWorker);worker.pool=self.pool
        await worker.reconcile_publications();await worker.refresh_jobs()
        self.assertEqual(await self.pool.fetchval('SELECT current_revision_id FROM application.sources'),'capture:new')
        self.assertEqual(await self.pool.fetchval('SELECT status FROM application.jobs'),'completed')
        self.assertEqual(await self.pool.fetchval('SELECT status FROM public.jobs'),'completed')
        self.assertEqual(await self.pool.fetchval('SELECT status FROM public.documents'),'completed')
        self.assertEqual(await self.pool.fetchval("SELECT count(*) FROM collection_control.outbox WHERE event_type='generation_requested' AND delivered_at IS NOT NULL"),1)

    async def test_retry_release_fences_old_receipts_and_uses_new_physical_handles(self):
        envelope,lease,body,_=await self.fixture();admission=await self.commit(envelope,lease,body)
        first=await self.repo.claim(admission['operation_id'],ttl_seconds=60)
        await self.repo.release_retry(first)
        second=await self.repo.claim(admission['operation_id'],ttl_seconds=60)
        self.assertNotEqual(first['handles'],second['handles'])
        with self.assertRaises(Fenced):await self.repo.receipt(first,'elasticsearch','a'*64,1)
        with self.assertRaises(Conflict):await self.repo.resolve('collection')

    async def source_fixture(self,*,document='native-source',source_name='generated-source',empty=False):
        from dataclasses import replace
        from datetime import timedelta
        from nv_ingest_durable.contracts import Stage,ClaimedMessage
        from application.worker import ApplicationWorker
        from collection_control.writer import GenerationWriter
        from collection_control.artifacts import content_digest
        e,_,body,raw_path=await self.fixture(document=document,source_name=source_name)
        body['documents'][0]['metadata']['content_metadata']['page_number']=1
        body['native_page_completions']={'schema_version':'native-page-completions.v1','pages':[{'page_number':1,'status':'complete'}]}
        if empty:body['documents']=[]
        from application.intake import embedding_config
        settings=SimpleNamespace(stages=self.stage_root,raw=self.raw_root,snapshots=self.snapshots,contract=self.contract,models={'models':{'embed':{'model':'generated-embedding-model'}}})
        extraction=replace(e,stage=Stage.EXTRACTION,config_digest='sha256:'+embedding_config(settings),idempotency_key='sha256:'+hashlib.sha256(('generated-extraction:'+document).encode()).hexdigest())
        lease=await self.legacy.acquire_lease(extraction,owner='generated-extraction-worker',ttl=timedelta(minutes=5))
        await self.pool.execute('UPDATE public.stage_units SET input_envelope=$2::jsonb WHERE idempotency_key=$1',extraction.idempotency_key,json.dumps(extraction.to_dict()))
        artifact=self.finalized(extraction,body)
        notification=replace(extraction,stage=Stage.INDEX,artifact_uri=artifact.uri,idempotency_key='sha256:'+hashlib.sha256(('generated-notification:'+document).encode()).hexdigest())
        await self.legacy.commit_stage(extraction,lease_token=lease,artifact=artifact,downstream=(('rag.agentic.index',notification),))
        job=UUID(e.job_id)
        await self.pool.execute("UPDATE public.jobs SET metadata=jsonb_set(metadata,'{acl}','[\"local-operator\"]'::jsonb) WHERE job_id=$1",job)
        metadata=json.loads(await self.pool.fetchval('SELECT metadata FROM public.jobs WHERE job_id=$1',job))
        await self.pool.execute("INSERT INTO application.collections(collection_id,name) VALUES('collection','Generated source workflow') ON CONFLICT DO NOTHING")
        await self.pool.execute("INSERT INTO application.jobs(job_id,collection_id) VALUES($1,'collection')",job)
        source_id='source:'+hashlib.sha256(source_name.encode()).hexdigest();revision='revision:'+document
        uri='https://qualification.invalid/'+source_name
        await self.pool.execute("INSERT INTO application.sources(source_id,collection_id,canonical_uri) VALUES($1,'collection',$2) ON CONFLICT DO NOTHING",source_id,uri)
        await self.pool.execute("""INSERT INTO application.source_revisions(source_revision_id,source_id,collection_id,document_id,job_id,canonical_uri,filename,raw_sha256,raw_uri,observed_at,ingestion_run_id,page_count) VALUES($1,$2,'collection',$3,$4,$5,'generated.pdf',$6,$7,'2026-11-08T00:00:00Z',$8,1)""",revision,source_id,e.document_id,job,uri,hashlib.sha256(raw_path.read_bytes()).hexdigest(),raw_path.as_uri(),metadata['ingestion_run_id'])
        class VerifiedFixtureStore:
            async def write_verify(self,handle,artifact):return content_digest(artifact['records']),len(artifact['records'])
        settings=SimpleNamespace(stages=self.stage_root,raw=self.raw_root,snapshots=self.snapshots,contract=self.contract,models={'models':{'embed':{'model':'generated-embedding-model'}}})
        context=SimpleNamespace(control=self.repo,settings=settings,pool=self.pool,writer=GenerationWriter(self.repo,self.snapshots,{'elasticsearch':VerifiedFixtureStore(),'milvus':VerifiedFixtureStore()}))
        worker=ApplicationWorker.__new__(ApplicationWorker);worker.pool=self.pool;worker.settings=settings;worker.context=context;worker.legacy=self.legacy
        message=ClaimedMessage(stream='rag.agentic.index',group='generated-fixture',redis_id='1-0',envelope=notification)
        return worker,message,revision

    async def test_actual_source_notification_enriches_admits_writes_and_reconciles(self):
        from unittest.mock import patch
        worker,message,revision=await self.source_fixture()
        with patch.dict(os.environ,{'OCR_MODEL_DIGEST':'a'*64}):await worker.source(message)
        row=await self.pool.fetchrow('SELECT * FROM application.source_revisions WHERE source_revision_id=$1',revision)
        self.assertEqual(row['status'],'publishing');self.assertEqual(row['records'],1)
        self.assertIsNone(await self.pool.fetchval('SELECT current_revision_id FROM application.sources'))
        self.assertEqual(await worker.publish_once(),1)
        self.assertEqual(await self.pool.fetchval('SELECT current_revision_id FROM application.sources'),revision)
        self.assertEqual(await self.pool.fetchval('SELECT status FROM application.jobs'),'completed')
        self.assertEqual(await self.pool.fetchval('SELECT status FROM public.jobs'),'completed')
        self.assertEqual(await self.pool.fetchval('SELECT status FROM public.documents'),'completed')
        with patch('application.worker.enrich_checkpoint',side_effect=AssertionError('published source must not re-enrich')):
            await worker.source(message)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),1)

    async def test_actual_source_recovers_committed_admission_after_application_update_crash(self):
        from unittest.mock import patch,AsyncMock
        worker,message,revision=await self.source_fixture()
        with patch.dict(os.environ,{'OCR_MODEL_DIGEST':'a'*64}),patch.object(worker,'_record_source_admission',AsyncMock(side_effect=RuntimeError('generated crash boundary'))):
            with self.assertRaises(RuntimeError):await worker.source(message)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),1)
        with patch('application.worker.enrich_checkpoint',side_effect=AssertionError('recovery must use retained committed bytes')):
            await worker.source(message)
        self.assertEqual(await self.pool.fetchval('SELECT status FROM application.source_revisions'),'publishing')
        await worker.publish_once()
        self.assertEqual(await self.pool.fetchval('SELECT current_revision_id FROM application.sources'),revision)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),1)

    async def test_writer_reconciles_source_commit_crash_before_notification_retry(self):
        from unittest.mock import patch,AsyncMock
        worker,message,revision=await self.source_fixture()
        with patch.dict(os.environ,{'OCR_MODEL_DIGEST':'a'*64}),patch.object(worker,'_record_source_admission',AsyncMock(side_effect=RuntimeError('generated crash boundary'))):
            with self.assertRaises(RuntimeError):await worker.source(message)
        await worker.publish_once()
        row=await self.pool.fetchrow('SELECT * FROM application.source_revisions WHERE source_revision_id=$1',revision)
        self.assertEqual(row['status'],'published');self.assertEqual(row['records'],1);self.assertIsNotNone(row['operation_id'])
        self.assertEqual(await self.pool.fetchval('SELECT current_revision_id FROM application.sources'),revision)

    async def test_actual_empty_source_capture_commits_without_empty_generation(self):
        from unittest.mock import patch
        worker,message,revision=await self.source_fixture(empty=True)
        with patch.dict(os.environ,{'OCR_MODEL_DIGEST':'a'*64}):await worker.source(message)
        row=await self.pool.fetchrow('SELECT * FROM application.source_revisions WHERE source_revision_id=$1',revision)
        self.assertEqual(row['status'],'captured');self.assertEqual(row['records'],0)
        self.assertIsNone(row['operation_id']);self.assertIsNotNone(row['admission_stage_key'])
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),0)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.source_revisions'),1)
        self.assertIsNone(await self.pool.fetchval('SELECT current_revision_id FROM application.sources'))

    async def test_source_previous_revision_uses_current_snapshot_and_reconciliation_never_rolls_back(self):
        from unittest.mock import patch
        worker,first,revision1=await self.source_fixture(document='native-first')
        with patch.dict(os.environ,{'OCR_MODEL_DIGEST':'a'*64}):await worker.source(first)
        await worker.publish_once()
        worker,second,revision2=await self.source_fixture(document='native-second')
        # Simulate an application mirror lagging behind the authoritative
        # publication; the next capture must still link to the published source.
        await self.pool.execute('UPDATE application.sources SET current_revision_id=NULL')
        with patch.dict(os.environ,{'OCR_MODEL_DIGEST':'a'*64}):await worker.source(second)
        row=await self.pool.fetchrow('SELECT * FROM application.source_revisions WHERE source_revision_id=$1',revision2)
        self.assertEqual(row['previous_source_revision_id'],revision1)
        await worker.publish_once()
        await self.pool.execute("UPDATE application.source_revisions SET status='publishing' WHERE source_revision_id=$1",revision1)
        await worker.reconcile_publications()
        self.assertEqual(await self.pool.fetchval('SELECT current_revision_id FROM application.sources'),revision2)

    async def test_actual_source_rejects_relabeling_native_embedding_configuration(self):
        from unittest.mock import patch
        worker,message,_=await self.source_fixture()
        await self.pool.execute("UPDATE public.stage_units SET input_envelope=jsonb_set(input_envelope,'{config_digest}',to_jsonb($2::text)) WHERE job_id=$1 AND stage='extraction'",UUID(message.envelope.job_id),'sha256:'+'f'*64)
        with patch.dict(os.environ,{'OCR_MODEL_DIGEST':'a'*64}):
            with self.assertRaises(Conflict):await worker.source(message)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),0)

    async def test_application_relay_never_publishes_historical_or_mismatched_scope_events(self):
        from uuid import uuid4
        from unittest.mock import AsyncMock
        worker,message,_=await self.source_fixture()
        extraction=await self.pool.fetchrow("SELECT * FROM public.stage_units WHERE job_id=$1 AND stage='extraction'",UUID(message.envelope.job_id))
        # Add real app extraction, old-prefix and forged cross-document events.
        await self.pool.execute('INSERT INTO public.outbox_events(event_id,idempotency_key,target_stream,envelope) VALUES($1,$2,$3,$4::jsonb)',uuid4(),extraction['idempotency_key'],'rag.agentic.extraction.pending',extraction['input_envelope'])
        await self.pool.execute('INSERT INTO public.outbox_events(event_id,idempotency_key,target_stream,envelope) VALUES($1,$2,$3,$4::jsonb)',uuid4(),extraction['idempotency_key'],'ingest.index',json.dumps(message.envelope.to_dict()))
        forged=message.envelope.to_dict();forged['document_id']='foreign-document';forged['message_id']=str(uuid4())
        await self.pool.execute('INSERT INTO public.outbox_events(event_id,idempotency_key,target_stream,envelope) VALUES($1,$2,$3,$4::jsonb)',uuid4(),extraction['idempotency_key'],'rag.agentic.index',json.dumps(forged))
        foreign,_,_,_=await self.fixture(document='foreign-legacy-document')
        await self.pool.execute('INSERT INTO public.outbox_events(event_id,idempotency_key,target_stream,envelope) VALUES($1,$2,$3,$4::jsonb)',uuid4(),foreign.idempotency_key,'rag.agentic.extraction.pending',json.dumps(foreign.to_dict()))
        worker.transport=SimpleNamespace(publish=AsyncMock(return_value='generated-redis-id'))
        self.assertEqual(await worker.relay_once(),2)
        self.assertEqual(worker.transport.publish.await_count,2)
        self.assertEqual({call.args[0] for call in worker.transport.publish.await_args_list},{'rag.agentic.index','rag.agentic.extraction.pending'})
        self.assertEqual(await worker.relay_once(),0)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM public.outbox_events WHERE published_at IS NULL'),3)

    async def test_source_failed_admission_releases_lease_and_recovers_same_identity(self):
        from unittest.mock import patch,AsyncMock
        worker,message,revision=await self.source_fixture()
        with patch.dict(os.environ,{'OCR_MODEL_DIGEST':'a'*64}),patch('application.worker.StageCollectionRepository.commit_stage',AsyncMock(side_effect=RuntimeError('generated pre-commit crash'))):
            with self.assertRaises(RuntimeError):await worker.source(message)
        first=await self.pool.fetchrow('SELECT * FROM application.source_revisions WHERE source_revision_id=$1',revision)
        failed=await self.pool.fetchrow('SELECT status,lease_token FROM public.stage_units WHERE idempotency_key=$1',first['admission_stage_key'])
        self.assertEqual(failed['status'],'failed');self.assertIsNone(failed['lease_token'])
        with patch.dict(os.environ,{'OCR_MODEL_DIGEST':'a'*64}):await worker.source(message)
        second=await self.pool.fetchrow('SELECT * FROM application.source_revisions WHERE source_revision_id=$1',revision)
        self.assertEqual(first['admission_stage_key'],second['admission_stage_key'])
        self.assertEqual(first['previous_source_revision_id'],second['previous_source_revision_id'])
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),1)
        await worker.publish_once()
        self.assertEqual(await self.pool.fetchval('SELECT current_revision_id FROM application.sources'),revision)

    async def caption_workflow(self,*,empty=False):
        from dataclasses import replace
        from unittest.mock import patch
        from nv_ingest_durable.contracts import Stage
        from application.intake import embedding_config
        from tests.caption_publication.test_caption_admission import CaptionAdmissionTests
        worker,message,revision=await self.source_fixture(empty=empty)
        with patch.dict(os.environ,{'OCR_MODEL_DIGEST':'a'*64}):await worker.source(message)
        producer=SimpleNamespace(h=self.h,pool=self.pool,env=message.envelope)
        original_commit=self.legacy.commit_stage
        async def commit_fixture(envelope,*,lease_token,artifact,downstream):
            if envelope.stage==Stage.CAPTION:
                path=Path(artifact.uri.removeprefix('file://'));checkpoint=json.loads(path.read_text())
                checkpoint['embedding_config_digest']=embedding_config(worker.settings)
                raw=canonical(checkpoint);path.write_bytes(raw)
                artifact=replace(artifact,digest='sha256:'+hashlib.sha256(raw).hexdigest(),size_bytes=len(raw))
            return await original_commit(envelope,lease_token=lease_token,artifact=artifact,downstream=downstream)
        with patch.object(self.legacy,'commit_stage',commit_fixture):key,artifact,_=await CaptionAdmissionTests.caption(producer)
        caption_message=replace(message,envelope=replace(message.envelope,idempotency_key=key,artifact_uri=artifact.uri))
        with patch.dict(os.environ,{'CAPTION_MODEL_DIGEST':'a'*64}):
            if not empty:
                with self.assertRaises(Conflict):await worker.caption(caption_message)
                await worker.publish_once()
            else:
                self.assertEqual(await self.pool.fetchval('SELECT status FROM application.source_revisions'),'captured')
            with self.assertRaises(Conflict):await worker.caption(replace(caption_message,envelope=replace(caption_message.envelope,artifact_uri='file:///wrong-result.json')))
            await worker.caption(caption_message)
        self.assertEqual(await self.pool.fetchval('SELECT status FROM application.caption_publications'),'pending')
        await worker.publish_once()
        self.assertEqual(await self.pool.fetchval('SELECT status FROM application.caption_publications'),'published')
        self.assertEqual(await self.pool.fetchval('SELECT current_revision_id FROM application.sources'),revision)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.caption_admissions'),1)

    async def test_actual_caption_handler_waits_for_source_then_publishes_generated_annotation(self):
        await self.caption_workflow()

    async def test_actual_image_only_source_and_caption_publish_derived_only_first_generation(self):
        await self.caption_workflow(empty=True)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.stage_admissions'),0)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.record_lineage'),0)
        self.assertEqual(await self.pool.fetchval('SELECT status FROM application.source_revisions'),'published')
        self.assertIsNotNone(await self.pool.fetchval('SELECT operation_id FROM application.source_revisions'))

    async def test_empty_replacement_without_caption_is_failed_not_false_completion(self):
        from unittest.mock import patch
        worker,message,first=await self.source_fixture(document='initial-content')
        with patch.dict(os.environ,{'OCR_MODEL_DIGEST':'a'*64}):await worker.source(message)
        await worker.publish_once()
        worker,message,second=await self.source_fixture(document='empty-replacement',empty=True)
        with patch.dict(os.environ,{'OCR_MODEL_DIGEST':'a'*64}):await worker.source(message)
        self.assertEqual(await self.pool.fetchval('SELECT current_revision_id FROM application.sources'),first)
        self.assertEqual(await self.pool.fetchval('SELECT status FROM application.source_revisions WHERE source_revision_id=$1',second),'failed')
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),1)

    async def test_later_source_revision_waits_for_earlier_uncommitted_admission(self):
        from unittest.mock import patch,AsyncMock
        worker,first_message,first=await self.source_fixture(document='first-admission')
        worker,later_message,later=await self.source_fixture(document='later-admission')
        await self.pool.execute("UPDATE application.source_revisions SET observed_at='2026-11-08T00:00:01Z' WHERE source_revision_id=$1",later)
        with patch.dict(os.environ,{'OCR_MODEL_DIGEST':'a'*64}):
            with self.assertRaisesRegex(Conflict,'Earlier revision'):await worker.source(later_message)
            await worker.source(first_message)
            await worker.publish_once()
            await worker.source(later_message)
        await worker.publish_once()
        self.assertEqual(await self.pool.fetchval('SELECT current_revision_id FROM application.sources'),later)
