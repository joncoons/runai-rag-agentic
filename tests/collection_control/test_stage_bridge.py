import asyncio
import copy
import hashlib
import json
import os
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import asyncpg

from collection_control.artifacts import canonical,content_digest,record_digest,load_artifact
from collection_control.repository import Conflict,Fenced
from collection_control.stage_bridge import StageCollectionRepository,ProvenanceRepository
from collection_control.writer import GenerationWriter


def sha(raw):return hashlib.sha256(raw).hexdigest()


@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'Requires disposable local PostgreSQL and local durable source')
class StageBridgeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        global Stage,StageEnvelope,CommittedArtifact,PostgresManifestRepository
        from nv_ingest_durable.contracts import Stage,StageEnvelope,CommittedArtifact
        from nv_ingest_durable.postgres import PostgresManifestRepository
        from urllib.parse import urlparse
        dsn=os.environ['COLLECTION_TEST_DSN'];self.assertIn(urlparse(dsn).hostname,('127.0.0.1','localhost'))
        self.pool=await asyncpg.create_pool(dsn)
        who=await self.pool.fetchrow('SELECT current_database() AS db,current_user AS usr')
        self.assertEqual((who['db'],who['usr']),('qualification','qualification'))
        source=Path(os.environ['DURABLE_SOURCE_ROOT'])
        for legacy_migration in ['001_initial.sql','002_coordinator.sql']:
            await self.pool.execute((source/'src/nv_ingest_durable/migrations'/legacy_migration).read_text())
        for name in ['001_initial.sql','002_stage_admissions.sql']:
            await self.pool.execute((Path('deployment/collection-control')/name).read_text())
        await self.pool.execute('TRUNCATE collection_control.collections,public.jobs CASCADE')
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.stage_root=self.root/'stages';self.raw_root=self.root/'raw';self.snapshots=self.root/'snapshots'
        for root in [self.stage_root,self.raw_root,self.snapshots]:root.mkdir()
        self.legacy=PostgresManifestRepository(self.pool);self.repo=ProvenanceRepository(self.pool)
        self.contract={'dimensions':2048,'dtype':'float32','normalization':'unit','metric':'IP','embedding_checkpoint_sha256':'e'*64,'schema_version':1}
        await self.repo.create_collection('collection',sha(canonical(self.contract)))
        self.bridge=StageCollectionRepository(self.legacy,self.repo,'collection',self.contract,self.stage_root,self.raw_root,self.snapshots)

    async def asyncTearDown(self):
        await self.pool.close();self.temp.cleanup()

    async def fixture(self,document='doc-a',revision=None,previous=None,source_name=None):
        revision=revision or ('srcrev-original' if document=='doc-a' else 'srcrev-'+document)
        raw=('Captured qualification source '+document+' '+revision).encode();raw_hash=sha(raw)
        raw_path=self.raw_root/raw_hash;raw_path.write_bytes(raw)
        text='Captured qualification text '+document+' '+revision;text_hash=sha(text.encode())
        job=uuid4();capture_run=uuid4();uri='https://qualification.invalid/'+(source_name or document)
        await self.pool.execute("INSERT INTO public.jobs(job_id,corpus_namespace,status,pipeline_version,metadata) VALUES($1,'collection','running','qualification-v1',$2::jsonb)",job,json.dumps({'acl':['local-qualification'],'ingestion_run_id':str(capture_run)}))
        await self.pool.execute("INSERT INTO public.documents(document_id,job_id,source_uri,source_digest,status) VALUES($1,$2,$3,$4,'processing')",document,job,uri,'sha256:'+raw_hash)
        envelope=StageEnvelope(job_id=str(job),document_id=document,stage=Stage.EMBEDDING,artifact_uri=raw_path.as_uri(),idempotency_key='sha256:'+sha((document+revision).encode()),pipeline_version='qualification-v1',config_digest='c'*64,model_digest='e'*64)
        lease=await self.legacy.acquire_lease(envelope,owner='qualification-worker',ttl=timedelta(minutes=5))
        await self.pool.execute('UPDATE public.stage_units SET input_envelope=$2::jsonb WHERE idempotency_key=$1',envelope.idempotency_key,json.dumps(envelope.to_dict()))
        p={'schema_version':'unstructured-source-provenance.v1','ingestion_run_id':str(capture_run),'source_revision_id':revision,'source_chunk_id':'chunk-'+document+'-'+revision,'source_system':'web_crawl','source_kind':'web_page','modality':'text','canonical_uri':uri,'final_uri':uri,'observed_at':'2026-11-08T00:00:00Z','hashes':{'raw_sha256':'sha256:'+raw_hash,'text_sha256':'sha256:'+text_hash},'anchors':{'heading_path':['Qualification']},'transforms':[{'component':'parser','version':'fixture-parser-v1','config_digest':'b'*64},{'component':'page_unit','version':'whole-page-v1','config_digest':'b'*64},{'component':'embedding','version':'qualification-v1','config_digest':'c'*64,'model_digest':'e'*64}],'raw_artifact_uri':raw_path.as_uri()}
        if previous:p['previous_source_revision_id']=previous
        metadata={'source':{'source_id':'upstream-'+document,'source_revision_id':revision,'source_chunk_id':p['source_chunk_id']},'content_metadata':{'provenance_schema_version':p['schema_version'],'ingestion_run_id':str(capture_run),'source_revision_id':revision,'source_chunk_id':p['source_chunk_id'],'canonical_uri':uri,'source_system':'web_crawl','source_kind':'web_page','modality':'text','final_uri':uri,'retrieved_at':'2026-11-08T00:00:00Z','parser_version':'fixture-parser-v1','chunker_version':'whole-page-v1','raw_sha256':'sha256:'+raw_hash,'text_sha256':'sha256:'+text_hash},'provenance':p,'acl':['local-qualification'],'custom_retained_field':'preserve-me'}
        body={'schema_version':'1.0','payload_format':'es_docs','documents':[{'text':text,'vector':[1.0]+[0.0]*2047,'metadata':metadata}]}
        manifest={'schema_version':'source-unit-manifest.v1','document_id':document,'source_revision_id':revision,'canonical_uri':uri,'raw_sha256':'sha256:'+raw_hash,'coverage':'complete','expected_units':['whole-source']}
        manifest_bytes=canonical(manifest);manifest_sha=sha(manifest_bytes);manifest_path=self.raw_root/(manifest_sha+'.json');manifest_path.write_bytes(manifest_bytes)
        await self.pool.execute("UPDATE public.jobs SET metadata=metadata || $2::jsonb WHERE job_id=$1",job,json.dumps({'source_manifests':{document:{'uri':manifest_path.as_uri(),'sha256':manifest_sha}}}))
        body['coverage']={'manifest_sha256':manifest_sha,'completed_units':{'whole-source':[p['source_chunk_id']]},'failed_units':[]}
        return envelope,lease,body,raw_path

    def finalized(self,envelope,body):
        raw=canonical(body);path=self.stage_root/(envelope.idempotency_key[7:]+'.json');path.write_bytes(raw)
        return CommittedArtifact(uri=path.as_uri(),digest='sha256:'+sha(raw),size_bytes=len(raw),metadata={'payload_format':'es_docs'})

    async def commit(self,envelope,lease,body):
        artifact=self.finalized(envelope,body)
        await self.bridge.commit_stage(envelope,lease_token=lease,artifact=artifact,downstream=())
        return await self.pool.fetchrow('SELECT * FROM collection_control.stage_admissions WHERE stage_key=$1',envelope.idempotency_key)

    async def publish(self,operation_id):
        lease=await self.repo.claim(operation_id,ttl_seconds=60)
        artifact=load_artifact(self.snapshots,lease['artifact_sha256'])
        await self.repo.verify_admission(lease,artifact)
        for target in lease['targets']:await self.repo.receipt(lease,target,content_digest(artifact['records']),len(artifact['records']))
        await self.repo.publish(lease)
        return lease,artifact

    async def test_atomic_handoff_preserves_upstream_provenance_and_suppresses_direct_index(self):
        envelope,lease,body,_=await self.fixture()
        artifact=self.finalized(envelope,body)
        index=StageEnvelope(job_id=envelope.job_id,document_id=envelope.document_id,stage=Stage.INDEX,artifact_uri=artifact.uri,idempotency_key='sha256:'+'f'*64,pipeline_version=envelope.pipeline_version,config_digest='c'*64,model_digest='e'*64)
        await self.bridge.commit_stage(envelope,lease_token=lease,artifact=artifact,downstream=(('ingest.index',index),))
        admission=await self.pool.fetchrow('SELECT * FROM collection_control.stage_admissions')
        self.assertEqual(await self.pool.fetchval('SELECT status FROM public.stage_units'),'committed')
        self.assertEqual(await self.pool.fetchval('SELECT outcome FROM public.stage_attempts'),'committed')
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM public.outbox_events'),0)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.outbox'),1)
        lease,snapshot=await self.publish(admission['operation_id'])
        record=snapshot['records'][0]
        self.assertEqual(record['record_id'],body['documents'][0]['metadata']['provenance']['source_chunk_id'])
        self.assertEqual(record['metadata']['provenance'],body['documents'][0]['metadata']['provenance'])
        self.assertEqual(record['metadata']['custom_retained_field'],'preserve-me')
        self.assertEqual(record['metadata']['origin'],'grounded')

    async def test_post_stage_event_failure_rolls_back_all_database_effects(self):
        envelope,lease,body,_=await self.fixture()
        # Fault after local legacy commit SQL, before the collection event inserts.
        await self.pool.execute("CREATE OR REPLACE FUNCTION public.bridge_test_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected-domain-event-failure'; END $$")
        await self.pool.execute('CREATE TRIGGER bridge_test_fail BEFORE INSERT ON collection_control.outbox FOR EACH ROW EXECUTE FUNCTION public.bridge_test_fail()')
        try:
            with self.assertRaises(asyncpg.RaiseError):await self.commit(envelope,lease,body)
        finally:await self.pool.execute('DROP TRIGGER bridge_test_fail ON collection_control.outbox')
        self.assertEqual(await self.pool.fetchval('SELECT status FROM public.stage_units'),'running')
        self.assertIsNone(await self.pool.fetchval('SELECT outcome FROM public.stage_attempts'))
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),0)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.stage_admissions'),0)
        self.assertIsNotNone(await self.commit(envelope,lease,body))

    async def test_replay_is_exact_and_not_a_second_generation(self):
        envelope,lease,body,_=await self.fixture();first=await self.commit(envelope,lease,body)
        again=await self.commit(envelope,lease,body);self.assertEqual(first['operation_id'],again['operation_id'])
        changed=copy.deepcopy(body);changed['documents'][0]['metadata']['acl']=['another-principal']
        with self.assertRaises(Conflict):await self.commit(envelope,lease,changed)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),1)

    async def test_missing_ids_raw_tampering_and_mirrored_hash_conflicts_fail_closed(self):
        envelope,lease,body,raw=await self.fixture()
        for mutation in [lambda x:x['documents'][0]['metadata']['provenance'].pop('source_chunk_id'),lambda x:x['documents'][0]['metadata']['content_metadata'].__setitem__('text_sha256','sha256:'+'0'*64),lambda x:x['documents'][0]['metadata']['provenance'].__setitem__('transforms',[])]:
            changed=copy.deepcopy(body);mutation(changed)
            with self.assertRaises(Conflict):await self.commit(envelope,lease,changed)
        raw.write_bytes(b'tampered')
        with self.assertRaises(Conflict):await self.commit(envelope,lease,body)
        self.assertEqual(await self.pool.fetchval('SELECT status FROM public.stage_units'),'running')

    async def test_expired_stage_lease_cannot_commit_even_if_token_matches(self):
        envelope,lease,body,_=await self.fixture()
        await self.pool.execute("UPDATE public.stage_units SET lease_expires_at=clock_timestamp()-interval '1 second'")
        with self.assertRaises(Fenced):await self.commit(envelope,lease,body)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),0)

    async def test_document_merge_preserves_other_sources_and_membership_proof(self):
        a=await self.fixture('doc-a');admission=await self.commit(*a[:3]);_,first=await self.publish(admission['operation_id'])
        b=await self.fixture('doc-b');admission=await self.commit(*b[:3]);_,second=await self.publish(admission['operation_id'])
        self.assertEqual(len(second['records']),2);self.assertIn(first['records'][0],second['records'])
        manifest=await self.repo.resolve('collection')
        op=await self.repo.prepare('collection','enroll','enroll_milvus',2,sha(canonical(second)),sha(canonical(self.contract)))
        lease=await self.repo.claim(op['operation_id'],ttl_seconds=60)
        await self.repo.verify_admission(lease,second)
        await self.repo.cancel(lease,'operator_cancelled')
        self.assertEqual(await self.repo.resolve('collection'),manifest)

    async def test_pending_collection_mutation_rolls_back_second_document_commit(self):
        a=await self.fixture('doc-a');await self.commit(*a[:3])
        b=await self.fixture('doc-b')
        with self.assertRaises(Conflict):await self.commit(*b[:3])
        self.assertEqual(await self.pool.fetchval('SELECT status FROM public.stage_units WHERE document_id=$1','doc-b'),'running')

    async def test_grounded_writer_requires_complete_database_admission(self):
        envelope,lease,body,_=await self.fixture();admission=await self.commit(envelope,lease,body)
        class Store:
            calls=0
            async def write_verify(self,handle,artifact):
                self.calls+=1
                return content_digest(artifact['records']),len(artifact['records'])
        store=Store()
        self.assertTrue(await GenerationWriter(self.repo,self.snapshots,{'elasticsearch':store}).run(admission['operation_id']))
        self.assertEqual(store.calls,1)
        manifest=await self.repo.resolve('collection')
        original=load_artifact(self.snapshots,admission['snapshot_sha256'])
        forged=copy.deepcopy(original);forged['records'][0]['metadata']['custom_retained_field']='forged'
        from collection_control.provenance import store_snapshot
        forged_sha=store_snapshot(self.snapshots,forged)
        op=await self.repo.prepare('collection','forged','snapshot',1,forged_sha,sha(canonical(self.contract)))
        with self.assertRaises(Conflict):await GenerationWriter(self.repo,self.snapshots,{'elasticsearch':store}).run(op['operation_id'])
        self.assertEqual(store.calls,1);self.assertEqual(await self.repo.resolve('collection'),manifest)

    async def test_source_revision_replacement_requires_explicit_history(self):
        a=await self.fixture();admission=await self.commit(*a[:3]);await self.publish(admission['operation_id'])
        b=await self.fixture('doc-a-v2','srcrev-next',source_name='doc-a')
        with self.assertRaises(Conflict):await self.commit(*b[:3])
        b[2]['documents'][0]['metadata']['provenance']['previous_source_revision_id']='srcrev-original'
        admission=await self.commit(*b[:3]);_,snapshot=await self.publish(admission['operation_id'])
        self.assertEqual(len(snapshot['records']),1)
        self.assertEqual(snapshot['records'][0]['metadata']['source_revision_id'],'srcrev-next')
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.stage_admissions'),2)

    async def test_persisted_input_contract_cannot_be_changed_by_worker(self):
        from dataclasses import replace
        envelope,lease,body,_=await self.fixture()
        changed=replace(envelope,config_digest='d'*64)
        changed_body=copy.deepcopy(body);changed_body['documents'][0]['metadata']['provenance']['transforms'][-1]['config_digest']='d'*64
        with self.assertRaises(Conflict):await self.commit(changed,lease,changed_body)
        self.assertEqual(await self.pool.fetchval('SELECT status FROM public.stage_units'),'running')

    async def test_late_source_contract_change_blocks_publication_after_receipts(self):
        envelope,lease,body,_=await self.fixture();admission=await self.commit(envelope,lease,body)
        attempt=await self.repo.claim(admission['operation_id'],ttl_seconds=60)
        snapshot=load_artifact(self.snapshots,attempt['artifact_sha256'])
        await self.repo.verify_admission(attempt,snapshot)
        await self.repo.receipt(attempt,'elasticsearch',content_digest(snapshot['records']),1)
        await self.pool.execute("UPDATE public.stage_units SET input_envelope=jsonb_set(input_envelope,'{config_digest}',to_jsonb($1::text)) WHERE idempotency_key=$2",'d'*64,envelope.idempotency_key)
        with self.assertRaises(Conflict):await self.repo.publish(attempt)
        self.assertIsNone(await self.pool.fetchval('SELECT current_operation FROM collection_control.collections'))

    async def test_synthetic_relabel_and_missing_carried_proof_cannot_bypass_admission(self):
        a=await self.fixture();admission=await self.commit(*a[:3]);_,original=await self.publish(admission['operation_id'])
        from collection_control.provenance import store_snapshot
        forged=copy.deepcopy(original);forged['records'][0]['metadata']['origin']='synthetic'
        new_sha=store_snapshot(self.snapshots,forged)
        op=await self.repo.prepare('collection','forged','snapshot',1,new_sha,sha(canonical(self.contract)))
        class Store:
            async def write_verify(self,*a):raise AssertionError('Unadmitted bytes reached a store')
        with self.assertRaises(Conflict):await GenerationWriter(self.repo,self.snapshots,{'elasticsearch':Store()},allow_synthetic=True).run(op['operation_id'])
        await self.pool.execute('DELETE FROM collection_control.record_lineage WHERE operation_id=$1',admission['operation_id'])
        b=await self.fixture('doc-b')
        with self.assertRaises(Conflict):await self.commit(*b[:3])

    async def test_scope_acl_and_retained_file_boundaries_are_enforced(self):
        envelope,lease,body,raw=await self.fixture()
        changed=copy.deepcopy(body);changed['documents'][0]['metadata']['acl']=['unregistered']
        with self.assertRaises(Conflict):await self.commit(envelope,lease,changed)
        await self.pool.execute("UPDATE public.jobs SET corpus_namespace='another-collection'")
        with self.assertRaises(Conflict):await self.commit(envelope,lease,body)
        await self.pool.execute("UPDATE public.jobs SET corpus_namespace='collection'")
        outside=self.root/'outside';outside.write_bytes(raw.read_bytes());raw.unlink();raw.symlink_to(outside)
        with self.assertRaises(Conflict):await self.commit(envelope,lease,body)

    async def test_source_revision_and_chunk_ids_are_immutable_across_history(self):
        a=await self.fixture();admission=await self.commit(*a[:3]);await self.publish(admission['operation_id'])
        same_revision=await self.fixture('doc-a-changed','srcrev-original',source_name='doc-a')
        with self.assertRaises(Conflict):await self.commit(*same_revision[:3])
        valid_next=await self.fixture('doc-a-next','srcrev-next',previous='srcrev-original',source_name='doc-a')
        old_chunk=a[2]['documents'][0]['metadata']['provenance']['source_chunk_id']
        for section in ['source','content_metadata','provenance']:valid_next[2]['documents'][0]['metadata'][section]['source_chunk_id']=old_chunk
        with self.assertRaises(Conflict):await self.commit(*valid_next[:3])

    async def test_all_required_source_mirrors_and_source_specific_lineage_are_checked(self):
        envelope,lease,body,_=await self.fixture()
        for key,value in [('source_system','wrong'),('source_kind','wrong'),('modality','video'),('final_uri','https://different.invalid'),('retrieved_at','2020-01-01T00:00:00Z')]:
            changed=copy.deepcopy(body);changed['documents'][0]['metadata']['content_metadata'][key]=value
            with self.assertRaises(Conflict):await self.commit(envelope,lease,changed)
        changed=copy.deepcopy(body);changed['documents'][0]['metadata']['provenance']['anchors']={'unrelated':True}
        with self.assertRaises(Conflict):await self.commit(envelope,lease,changed)
        changed=copy.deepcopy(body);changed['documents'][0]['metadata']['provenance']['transforms']=[changed['documents'][0]['metadata']['provenance']['transforms'][-1]]
        with self.assertRaises(Conflict):await self.commit(envelope,lease,changed)

    async def test_ocr_extraction_method_requires_ocr_model_transform(self):
        envelope,lease,body,_=await self.fixture()
        metadata=body['documents'][0]['metadata'];p=metadata['provenance'];cm=metadata['content_metadata']
        p.update(source_system='document_capture',source_kind='scanned_document',modality='document')
        cm.update(source_system=p['source_system'],source_kind=p['source_kind'],modality=p['modality'],extraction_method='ocr')
        with self.assertRaises(Conflict):await self.commit(envelope,lease,body)
        p['transforms'].insert(-1,{'component':'ocr','version':'fixture-ocr-v1','config_digest':'b'*64,'model_digest':'f'*64})
        self.assertIsNotNone(await self.commit(envelope,lease,body))

    async def test_process_kill_before_domain_outbox_commit_recovers_atomically(self):
        import sys,textwrap
        envelope,lease,body,_=await self.fixture();artifact=self.finalized(envelope,body)
        config=self.root/'child-private.json'
        config.write_text(json.dumps({'envelope':envelope.to_dict(),'lease':str(lease),'artifact':{'uri':artifact.uri,'digest':artifact.digest,'size_bytes':artifact.size_bytes,'metadata':dict(artifact.metadata)},'contract':self.contract,'stage_root':str(self.stage_root),'raw_root':str(self.raw_root),'snapshots':str(self.snapshots)}))
        config.chmod(0o600)
        code=textwrap.dedent('''
            import asyncio,json,os,sys
            from pathlib import Path
            import asyncpg
            from nv_ingest_durable.contracts import StageEnvelope,Stage,CommittedArtifact
            from nv_ingest_durable.postgres import PostgresManifestRepository
            from collection_control.stage_bridge import StageCollectionRepository,ProvenanceRepository
            async def main():
                x=json.loads(Path(sys.argv[1]).read_text());e=x['envelope'];e['stage']=Stage(e['stage'])
                pool=await asyncpg.create_pool(os.environ['COLLECTION_TEST_DSN'],server_settings={'application_name':'stage-bridge-crash-qualification'})
                legacy=PostgresManifestRepository(pool);repo=ProvenanceRepository(pool)
                bridge=StageCollectionRepository(legacy,repo,'collection',x['contract'],x['stage_root'],x['raw_root'],x['snapshots'])
                await bridge.commit_stage(StageEnvelope(**e),lease_token=x['lease'],artifact=CommittedArtifact(**x['artifact']),downstream=())
                await pool.close()
            asyncio.run(main())
        ''')
        guard=await self.pool.acquire();process=None
        await guard.execute('SELECT pg_advisory_lock(92101401)')
        await self.pool.execute("CREATE OR REPLACE FUNCTION public.bridge_crash_pause() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(92101401); RETURN NEW; END $$")
        await self.pool.execute('CREATE TRIGGER bridge_crash_pause BEFORE INSERT ON collection_control.outbox FOR EACH ROW EXECUTE FUNCTION public.bridge_crash_pause()')
        try:
            process=await asyncio.create_subprocess_exec(sys.executable,'-c',code,str(config),stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
            for _ in range(100):
                if await self.pool.fetchval("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='stage-bridge-crash-qualification' AND wait_event='advisory')"):break
                if process.returncode is not None:
                    stdout,stderr=await process.communicate();self.fail('Crash-test worker exited before its transaction boundary: '+stderr.decode()[-1200:])
                await asyncio.sleep(.05)
            else:self.fail('Worker did not reach the guarded post-stage/pre-outbox boundary')
            self.assertEqual(await self.pool.fetchval('SELECT status FROM public.stage_units'),'running')
            process.kill();await process.communicate();self.assertEqual(process.returncode,-9)
        finally:
            if process and process.returncode is None:process.kill();await process.communicate()
            await guard.execute('SELECT pg_advisory_unlock(92101401)');await self.pool.release(guard)
            await self.pool.execute('DROP TRIGGER bridge_crash_pause ON collection_control.outbox')
        self.assertEqual(await self.pool.fetchval('SELECT status FROM public.stage_units'),'running')
        self.assertIsNone(await self.pool.fetchval('SELECT outcome FROM public.stage_attempts'))
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),0)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.stage_admissions'),0)
        self.assertIsNotNone(await self.commit(envelope,lease,body))

    async def test_incomplete_revision_cannot_remove_previous_document_records(self):
        envelope,lease,body,_=await self.fixture()
        for coverage in [None,{'manifest_sha256':'0'*64,'completed_units':{},'failed_units':[]},{'manifest_sha256':'0'*64,'completed_units':{},'failed_units':['page:1']}]:
            changed=copy.deepcopy(body)
            if coverage is None:changed.pop('coverage',None)
            else:changed['coverage']=coverage
            with self.assertRaises(Conflict):await self.commit(envelope,lease,changed)
        self.assertEqual(await self.pool.fetchval('SELECT status FROM public.stage_units'),'running')

    async def test_cancelled_job_cannot_admit_or_publish_late_work(self):
        envelope,lease,body,_=await self.fixture()
        await self.pool.execute("UPDATE public.jobs SET status='cancelled'")
        with self.assertRaises(Conflict):await self.commit(envelope,lease,body)
        await self.pool.execute("UPDATE public.jobs SET status='running'")
        admitted=await self.commit(envelope,lease,body)
        attempt=await self.repo.claim(admitted['operation_id'],ttl_seconds=60)
        snapshot=load_artifact(self.snapshots,attempt['artifact_sha256'])
        await self.repo.receipt(attempt,'elasticsearch',content_digest(snapshot['records']),1)
        await self.pool.execute("UPDATE public.jobs SET status='cancelled'")
        with self.assertRaises(Conflict):await self.repo.publish(attempt)

    async def retained_source_attempt(self,*,enroll=False):
        envelope,lease,body,raw=await self.fixture()
        admitted=await self.commit(envelope,lease,body)
        baseline=None
        if enroll:
            baseline,_=await self.publish(admitted['operation_id'])
            col=await self.pool.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1','collection')
            op=await self.repo.prepare('collection','generated-retention-enrollment','enroll_milvus',col['revision'],admitted['snapshot_sha256'],col['contract_sha256'])
        else:op={'operation_id':admitted['operation_id']}
        attempt=await self.repo.claim(op['operation_id'],ttl_seconds=60)
        artifact=load_artifact(self.snapshots,attempt['artifact_sha256'])
        await self.repo.verify_admission(attempt,artifact)
        for backend in attempt['targets']:await self.repo.receipt(attempt,backend,content_digest(artifact['records']),len(artifact['records']))
        proof=json.loads(admitted['proof'])
        paths={'raw':raw,'manifest':Path(proof['source_manifest']['uri'].removeprefix('file://')),'stage':Path(proof['stage_artifact_uri'].removeprefix('file://'))}
        return attempt,artifact,paths,baseline

    async def test_deleted_retained_source_raw_manifest_and_stage_block_verification_and_publication(self):
        attempt,artifact,paths,_=await self.retained_source_attempt()
        for kind,path in paths.items():
            with self.subTest(kind=kind):
                original=path.read_bytes();path.unlink()
                try:
                    with self.assertRaises(Conflict):await self.repo.verify_admission(attempt,artifact)
                    with self.assertRaises(Conflict):await self.repo.publish(attempt)
                    with self.assertRaises(Conflict):await self.repo.resolve('collection')
                finally:path.write_bytes(original)
        await self.repo.publish(attempt)

    async def test_same_size_tampering_of_source_raw_manifest_and_stage_blocks_publication(self):
        attempt,artifact,paths,_=await self.retained_source_attempt()
        for kind,path in paths.items():
            with self.subTest(kind=kind):
                original=path.read_bytes();path.write_bytes(bytes([original[0]^1])+original[1:])
                try:
                    with self.assertRaises(Conflict):await self.repo.verify_admission(attempt,artifact)
                    with self.assertRaises(Conflict):await self.repo.publish(attempt)
                finally:path.write_bytes(original)
        await self.repo.publish(attempt)

    async def test_enrollment_inherited_source_retention_revalidated_without_changing_prior_publication(self):
        attempt,artifact,paths,baseline=await self.retained_source_attempt(enroll=True)
        for kind,path in paths.items():
            for mutation in ('delete','same-size-tamper'):
                with self.subTest(kind=kind,mutation=mutation):
                    original=path.read_bytes()
                    if mutation=='delete':path.unlink()
                    else:path.write_bytes(bytes([original[0]^1])+original[1:])
                    try:
                        with self.assertRaises(Conflict):await self.repo.verify_admission(attempt,artifact)
                        with self.assertRaises(Conflict):await self.repo.publish(attempt)
                        self.assertEqual((await self.repo.resolve('collection'))['operation_id'],baseline['operation_id'])
                    finally:path.write_bytes(original)
        await self.repo.publish(attempt)

    async def test_retained_source_files_are_read_once_per_stage_despite_multiple_source_chunks(self):
        from unittest.mock import patch
        from collection_control.provenance import verified_file
        envelope,lease,body,_=await self.fixture()
        second=copy.deepcopy(body['documents'][0])
        for key in ('source','content_metadata','provenance'):second['metadata'][key]['source_chunk_id']+='-second'
        body['documents'].append(second)
        body['coverage']['completed_units']['whole-source'].append(second['metadata']['provenance']['source_chunk_id'])
        admitted=await self.commit(envelope,lease,body)
        attempt=await self.repo.claim(admitted['operation_id'],ttl_seconds=60)
        artifact=load_artifact(self.snapshots,attempt['artifact_sha256'])
        with patch('collection_control.admission_repository.verified_file',wraps=verified_file) as read:
            await self.repo.verify_admission(attempt,artifact)
        self.assertEqual(read.call_count,3)
