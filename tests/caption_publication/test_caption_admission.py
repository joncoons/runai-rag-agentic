"""Generated fixtures only; no model calls or retained application databases."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import unittest

from collection_control.artifacts import canonical,content_digest,load_artifact,admit
from collection_control.repository import Conflict
from collection_control.stage_bridge import CaptionCollectionRepository

spec=importlib.util.spec_from_file_location('source_bridge_fixture',Path(__file__).parents[1]/'collection_control/test_stage_bridge.py')
fixtures=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixtures)


@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'Requires disposable loopback qualification PostgreSQL')
class CaptionAdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.h=fixtures.StageBridgeTests()
        await self.h.asyncSetUp()
        self.pool=self.h.pool
        await self.pool.execute(Path('deployment/ingestion/captioning/001_caption_jobs.sql').read_text())
        await self.pool.execute(Path('deployment/collection-control/003_caption_admissions.sql').read_text())
        self.bridge=CaptionCollectionRepository(self.h.repo,'collection',self.h.contract,self.h.stage_root,self.h.snapshots)
        self.env,self.lease,self.body,_=await self.h.fixture()
        if self._testMethodName.startswith('test_empty_'):
            self.source=None
            self.body['documents']=[]
            self.body['coverage']['completed_units']={'whole-source':[]}
            from uuid import UUID
            meta=json.loads(await self.pool.fetchval('SELECT metadata FROM public.jobs WHERE job_id=$1',UUID(self.env.job_id)))
            document=await self.pool.fetchrow('SELECT * FROM public.documents WHERE document_id=$1',self.env.document_id)
            self.body['retained_source_capture']={'source_revision_id':'srcrev-original','canonical_uri':document['source_uri'],'raw_sha256':document['source_digest'],'raw_artifact_uri':self.env.artifact_uri,'observed_at':'2026-11-08T00:00:00Z','ingestion_run_id':meta['ingestion_run_id'],'source_manifest':meta['source_manifests'][self.env.document_id]}
            self.bridge=CaptionCollectionRepository(self.h.repo,'collection',self.h.contract,self.h.stage_root,self.h.snapshots,raw_root=self.h.raw_root)
        else:self.source=await self.h.commit(self.env,self.lease,self.body)

    async def asyncTearDown(self):await self.h.asyncTearDown()

    async def caption(self,number=1,*,manifest_bbox=None,result_bbox=None):
        from uuid import UUID
        from nv_ingest_durable.contracts import Stage,StageEnvelope
        stage_key='sha256:'+fixtures.sha(('generated-caption-fixture-'+self.env.job_id+str(number)).encode())
        config={'prompt':'Generated qualification image fixture'}
        config_digest=fixtures.sha(canonical(config))
        image=b'generated fixture image pixels '+str(number).encode()
        image_path=self.h.stage_root/('fixture-'+str(number)+'.image');image_path.write_bytes(image)
        source=await self.pool.fetchrow('SELECT * FROM public.documents WHERE document_id=$1',self.env.document_id)
        job_meta=json.loads(await self.pool.fetchval('SELECT metadata FROM public.jobs WHERE job_id=$1',UUID(self.env.job_id)))
        await self.pool.execute("UPDATE public.jobs SET metadata=metadata || '{\"ingestion_options\":{\"captions_enabled\":true}}'::jsonb WHERE job_id=$1",UUID(self.env.job_id))
        scope={'acl':job_meta['acl'],'ingestion_run_id':job_meta['ingestion_run_id']}
        extraction_key='sha256:'+fixtures.sha(('generated-extraction-'+self.env.job_id+str(number)).encode())
        manifest={'parent_stage_key':extraction_key,'parent_job_id':self.env.job_id,'document_id':self.env.document_id,'asset_uri':image_path.as_uri(),'asset_sha256':fixtures.sha(image),'page_number':1,'bbox':manifest_bbox,'source_metadata':{'fixture':'generated'},'model_digest':'a'*64,'caption_config':config}
        manifest_raw=canonical(manifest);manifest_path=self.h.stage_root/(fixtures.sha(manifest_raw)+'.json');manifest_path.write_bytes(manifest_raw)
        env=StageEnvelope(job_id=self.env.job_id,document_id=self.env.document_id,stage=Stage.CAPTION,artifact_uri=manifest_path.as_uri(),idempotency_key=stage_key,pipeline_version=self.env.pipeline_version,config_digest=config_digest,model_digest='a'*64)
        from datetime import timedelta
        from dataclasses import replace
        extraction=replace(env,stage=Stage.EXTRACTION,idempotency_key=extraction_key)
        extraction_lease=await self.h.legacy.acquire_lease(extraction,owner='generated-fixture',ttl=timedelta(minutes=5))
        extraction_body={'caption_jobs':[{'envelope':env.to_dict(),'manifest_uri':manifest_path.as_uri(),'manifest_sha256':fixtures.sha(manifest_raw),'asset_sha256':fixtures.sha(image)}]}
        extraction_artifact=self.h.finalized(extraction,extraction_body)
        await self.h.legacy.commit_stage(extraction,lease_token=extraction_lease,artifact=extraction_artifact,downstream=())
        lease=await self.h.legacy.acquire_lease(env,owner='generated-fixture',ttl=timedelta(minutes=5))
        await self.pool.execute('UPDATE public.stage_units SET input_envelope=$2::jsonb WHERE idempotency_key=$1',stage_key,json.dumps(env.to_dict()))
        text='Generated image caption fixture '+str(number)
        p={k:manifest[k] for k in ('parent_job_id','document_id','parent_stage_key','asset_sha256','asset_uri','source_metadata','model_digest')};p['config_digest']=config_digest;p['source_context']={'source_uri':source['source_uri'],'source_digest':source['source_digest'],'collection_id':'collection',**scope}
        checkpoint={'payload_format':'es_docs','schema_version':'1.0','embedding_contract':self.h.contract,'embedding_config_digest':'c'*64,'caption_text_sha256':fixtures.sha(text.encode()),'caption_asset_sha256':fixtures.sha(image),'documents':[{'text':text,'vector':[1.0]+[0.0]*2047,'metadata':{'acl':scope['acl'],'caption_provenance':p,'content_metadata':{'type':'image_caption','generated_annotation':True,'page_number':1,'bbox':copy.deepcopy(manifest_bbox) if result_bbox is None else result_bbox}}}]}
        artifact=self.h.finalized(env,checkpoint)
        # Caption identity is deliberately opaque; fixture writer's historical
        # filename convention is irrelevant to the stored committed artifact.
        await self.h.legacy.commit_stage(env,lease_token=lease,artifact=artifact,downstream=())
        await self.pool.execute('INSERT INTO caption_control.assets(stage_key,parent_job_id,document_id,source_digest,source_uri,collection_id,source_metadata,manifest_uri,manifest_sha256,asset_sha256) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10)',stage_key,UUID(env.job_id),env.document_id,source['source_digest'],source['source_uri'],'collection',json.dumps(scope),manifest_path.as_uri(),fixtures.sha(manifest_raw),fixtures.sha(image))
        return stage_key,artifact,image_path

    async def test_pending_source_retries_then_append_and_idempotent_replay(self):
        key,artifact,_=await self.caption()
        with self.assertRaises(Conflict):await self.bridge.admit(key,artifact,expected_model_digest='a'*64)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),1)
        _,source=await self.h.publish(self.source['operation_id'])
        op=await self.bridge.admit(key,artifact,expected_model_digest='a'*64)
        _,snapshot=await self.h.publish(op['operation_id'])
        self.assertEqual(len(snapshot['records']),2)
        self.assertEqual(snapshot['records'][0] if snapshot['records'][0]['metadata']['origin']=='grounded' else snapshot['records'][1],source['records'][0])
        derived=next(r for r in snapshot['records'] if r['metadata']['origin']=='derived')
        self.assertTrue(derived['metadata']['generated_annotation'])
        self.assertEqual((await self.bridge.admit(key,expected_model_digest='a'*64))['operation_id'],op['operation_id'])

    async def test_two_captions_preserved_and_asset_revalidated_before_publish(self):
        await self.h.publish(self.source['operation_id'])
        first,_,_=await self.caption(1)
        op=await self.bridge.admit(first,expected_model_digest='a'*64);await self.h.publish(op['operation_id'])
        second,_,image=await self.caption(2)
        op=await self.bridge.admit(second,expected_model_digest='a'*64)
        lease=await self.h.repo.claim(op['operation_id'],ttl_seconds=60)
        snapshot=load_artifact(self.h.snapshots,lease['artifact_sha256']);self.assertEqual(len(snapshot['records']),3)
        await self.h.repo.verify_admission(lease,snapshot)
        for target in lease['targets']:await self.h.repo.receipt(lease,target,content_digest(snapshot['records']),3)
        image.write_bytes(b'changed generated fixture')
        with self.assertRaises(Conflict):await self.h.repo.publish(lease)
        self.assertEqual((await self.h.repo.resolve('collection'))['operation_id'],(await self.pool.fetchrow('SELECT current_operation FROM collection_control.collections'))['current_operation'])
        self.assertNotEqual((await self.h.repo.resolve('collection'))['operation_id'],op['operation_id'])

    async def test_opt_out_and_wrong_model_rejected_without_intent(self):
        from uuid import UUID
        await self.h.publish(self.source['operation_id'])
        key,_,_=await self.caption()
        with self.assertRaises(Conflict):await self.bridge.admit(key,expected_model_digest='b'*64)
        await self.pool.execute("UPDATE public.jobs SET metadata=jsonb_set(metadata,'{ingestion_options,captions_enabled}','false') WHERE job_id=$1",UUID(self.env.job_id))
        with self.assertRaises(Conflict):await self.bridge.admit(key,expected_model_digest='a'*64)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),1)

    async def test_membership_inherits_and_revalidates_captions(self):
        from uuid import UUID
        await self.h.publish(self.source['operation_id'])
        key,_,_=await self.caption()
        op=await self.bridge.admit(key,expected_model_digest='a'*64)
        await self.h.publish(op['operation_id'])
        membership=await self.h.repo.prepare('collection','generated-membership','enroll_milvus',2,op['artifact_sha256'],fixtures.sha(canonical(self.h.contract)))
        lease=await self.h.repo.claim(membership['operation_id'],ttl_seconds=60)
        artifact=load_artifact(self.h.snapshots,lease['artifact_sha256'])
        await self.h.repo.verify_admission(lease,artifact)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.caption_record_lineage WHERE operation_id=$1',membership['operation_id']),1)
        await self.pool.execute("UPDATE public.jobs SET metadata=jsonb_set(metadata,'{ingestion_options,captions_enabled}','false') WHERE job_id=$1",UUID(self.env.job_id))
        with self.assertRaises(Conflict):await self.h.repo.verify_admission(lease,artifact)

    async def test_source_append_preserves_caption_and_replacement_removes_old_derivative(self):
        await self.h.publish(self.source['operation_id'])
        key,_,_=await self.caption()
        op=await self.bridge.admit(key,expected_model_digest='a'*64);await self.h.publish(op['operation_id'])
        env,lease,body,_=await self.h.fixture(document='doc-b')
        source=await self.h.commit(env,lease,body);_,snapshot=await self.h.publish(source['operation_id'])
        self.assertEqual(len(snapshot['records']),3)
        self.assertEqual(sum(r['metadata']['origin']=='derived' for r in snapshot['records']),1)
        env,lease,body,_=await self.h.fixture(document='doc-a-new',revision='srcrev-replacement',previous='srcrev-original',source_name='doc-a')
        source=await self.h.commit(env,lease,body);_,snapshot=await self.h.publish(source['operation_id'])
        self.assertEqual(len(snapshot['records']),2)
        self.assertTrue(all(r['metadata']['origin']=='grounded' for r in snapshot['records']))
        # Historical immutable admission remains, but its old source cannot be
        # reintroduced by replaying an already published caption operation.
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.caption_admissions'),1)

    async def test_retained_caption_contract_tampering_rejected(self):
        await self.h.publish(self.source['operation_id'])
        key,artifact,_=await self.caption()
        checkpoint=json.loads(Path(artifact.uri.removeprefix('file://')).read_text())
        checkpoint['embedding_contract']['embedding_checkpoint_sha256']='f'*64
        raw=canonical(checkpoint);Path(artifact.uri.removeprefix('file://')).write_bytes(raw)
        await self.pool.execute('UPDATE public.stage_units SET artifact_digest=$2 WHERE idempotency_key=$1',key,'sha256:'+fixtures.sha(raw))
        with self.assertRaises(Conflict):await self.bridge.admit(key,expected_model_digest='a'*64)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),1)

    async def test_empty_complete_source_allows_derived_only_first_generation(self):
        await self.h.bridge.commit_stage(self.env,lease_token=self.lease,artifact=self.h.finalized(self.env,self.body),downstream=())
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),0)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.stage_admissions'),0)
        key,_,_=await self.caption()
        op=await self.bridge.admit(key,expected_model_digest='a'*64)
        _,snapshot=await self.h.publish(op['operation_id'])
        self.assertEqual(len(snapshot['records']),1)
        self.assertEqual(snapshot['records'][0]['metadata']['origin'],'derived')
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.record_lineage'),0)
        second,_,_=await self.caption(2)
        op=await self.bridge.admit(second,expected_model_digest='a'*64)
        _,snapshot=await self.h.publish(op['operation_id'])
        self.assertEqual(len(snapshot['records']),2)
        self.assertTrue(all(r['metadata']['origin']=='derived' for r in snapshot['records']))

    async def test_empty_missing_coverage_cannot_commit_or_publish_caption(self):
        self.body['coverage']['completed_units']={}
        with self.assertRaises(Conflict):
            await self.h.bridge.commit_stage(self.env,lease_token=self.lease,artifact=self.h.finalized(self.env,self.body),downstream=())
        key,_,_=await self.caption()
        with self.assertRaises(Conflict):await self.bridge.admit(key,expected_model_digest='a'*64)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),0)

    async def test_empty_retained_raw_revalidated_before_publication(self):
        await self.h.bridge.commit_stage(self.env,lease_token=self.lease,artifact=self.h.finalized(self.env,self.body),downstream=())
        key,_,_=await self.caption()
        op=await self.bridge.admit(key,expected_model_digest='a'*64)
        lease=await self.h.repo.claim(op['operation_id'],ttl_seconds=60)
        artifact=load_artifact(self.h.snapshots,lease['artifact_sha256'])
        await self.h.repo.verify_admission(lease,artifact)
        for target in lease['targets']:await self.h.repo.receipt(lease,target,content_digest(artifact['records']),1)
        Path(self.env.artifact_uri.removeprefix('file://')).write_bytes(b'changed generated raw fixture')
        with self.assertRaises(Conflict):await self.h.repo.publish(lease)
        self.assertIsNone(await self.pool.fetchval('SELECT current_operation FROM collection_control.collections'))

    async def test_missing_caption_metadata_cannot_bypass_admission(self):
        await self.h.publish(self.source['operation_id'])
        key,artifact,_=await self.caption()
        checkpoint=json.loads(Path(artifact.uri.removeprefix('file://')).read_text())
        del checkpoint['documents'][0]['metadata']['caption_provenance']
        raw=canonical(checkpoint);Path(artifact.uri.removeprefix('file://')).write_bytes(raw)
        await self.pool.execute('UPDATE public.stage_units SET artifact_digest=$2 WHERE idempotency_key=$1',key,'sha256:'+fixtures.sha(raw))
        with self.assertRaises(Conflict):await self.bridge.admit(key,expected_model_digest='a'*64)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),1)

    async def test_caption_lineage_digest_tampering_fails_writer_gate(self):
        await self.h.publish(self.source['operation_id'])
        key,_,_=await self.caption()
        op=await self.bridge.admit(key,expected_model_digest='a'*64)
        lease=await self.h.repo.claim(op['operation_id'],ttl_seconds=60)
        artifact=load_artifact(self.h.snapshots,lease['artifact_sha256'])
        await self.pool.execute('UPDATE collection_control.caption_record_lineage SET record_sha256=$2 WHERE operation_id=$1',op['operation_id'],'f'*64)
        with self.assertRaises(Conflict):await self.h.repo.verify_admission(lease,artifact)

    async def test_explicit_empty_replacement_removes_old_source_and_captions(self):
        from uuid import UUID
        await self.h.publish(self.source['operation_id'])
        old_key,_,_=await self.caption()
        op=await self.bridge.admit(old_key,expected_model_digest='a'*64);await self.h.publish(op['operation_id'])
        env,lease,body,_=await self.h.fixture(document='doc-unrelated')
        source=await self.h.commit(env,lease,body);await self.h.publish(source['operation_id'])
        self.env,self.lease,self.body,_=await self.h.fixture(document='doc-image-replacement',revision='srcrev-image-replacement',previous='srcrev-original',source_name='doc-a')
        self.body['documents']=[];self.body['coverage']['completed_units']={'whole-source':[]}
        meta=json.loads(await self.pool.fetchval('SELECT metadata FROM public.jobs WHERE job_id=$1',UUID(self.env.job_id)))
        document=await self.pool.fetchrow('SELECT * FROM public.documents WHERE document_id=$1',self.env.document_id)
        self.body['retained_source_capture']={'source_revision_id':'srcrev-image-replacement','previous_source_revision_id':'srcrev-original','canonical_uri':document['source_uri'],'raw_sha256':document['source_digest'],'raw_artifact_uri':self.env.artifact_uri,'observed_at':'2026-11-08T01:00:00Z','ingestion_run_id':meta['ingestion_run_id'],'source_manifest':meta['source_manifests'][self.env.document_id]}
        await self.h.bridge.commit_stage(self.env,lease_token=self.lease,artifact=self.h.finalized(self.env,self.body),downstream=())
        self.bridge=CaptionCollectionRepository(self.h.repo,'collection',self.h.contract,self.h.stage_root,self.h.snapshots,raw_root=self.h.raw_root)
        key,_,_=await self.caption()
        before=await self.h.repo.resolve('collection')
        op=await self.bridge.admit(key,expected_model_digest='a'*64)
        self.assertEqual(await self.h.repo.resolve('collection'),before)
        _,snapshot=await self.h.publish(op['operation_id'])
        self.assertEqual(len(snapshot['records']),2)
        self.assertEqual({r['metadata']['source_revision_id'] for r in snapshot['records']},{'srcrev-image-replacement','srcrev-doc-unrelated'})
        self.assertEqual(sum(r['metadata']['origin']=='derived' for r in snapshot['records']),1)
        self.assertEqual(await self.pool.fetchval('SELECT document_id FROM collection_control.caption_admissions WHERE stage_key=$1',key),self.env.document_id)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.source_revisions'),3)

    async def test_missing_processing_input_fails_closed_before_generation_intent(self):
        await self.h.publish(self.source['operation_id'])
        key,_,_=await self.caption()
        await self.pool.execute('UPDATE public.stage_units SET input_envelope=NULL WHERE idempotency_key=$1',key)
        with self.assertRaisesRegex(Conflict,'persisted processing input'):
            await self.bridge.admit(key,expected_model_digest='a'*64)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),1)

    async def test_legacy_ten_digit_bbox_with_identical_float32_geometry_is_admitted_without_mutation(self):
        import struct
        await self.h.publish(self.source['operation_id'])
        native=[0.0,0.0,0.9947954416275024,0.9737284779548645]
        rounded=[0.0,0.0,0.9947954416,0.973728478]
        self.assertEqual(struct.pack('<4f',*native),struct.pack('<4f',*rounded))
        key,artifact,_=await self.caption(manifest_bbox=native,result_bbox=rounded)
        path=Path(artifact.uri.removeprefix('file://'));before=path.read_bytes()
        op=await self.bridge.admit(key,expected_model_digest='a'*64)
        _,snapshot=await self.h.publish(op['operation_id'])
        derived=next(r for r in snapshot['records'] if r['metadata']['origin']=='derived')
        self.assertEqual(derived['metadata']['content_metadata']['bbox'],native)
        self.assertEqual(derived['metadata']['caption_derivation']['geometry_encoding'],'retained_float32_equivalent')
        self.assertEqual(path.read_bytes(),before)
        self.assertEqual((await self.bridge.admit(key,expected_model_digest='a'*64))['operation_id'],op['operation_id'])
        self.assertEqual(path.read_bytes(),before)

    async def test_one_float32_ulp_changed_crop_is_rejected(self):
        import struct
        await self.h.publish(self.source['operation_id'])
        native=[0.0,0.0,0.9947954416275024,0.9737284779548645]
        changed=list(native);bits=struct.unpack('<I',struct.pack('<f',native[2]))[0];changed[2]=struct.unpack('<f',struct.pack('<I',bits+1))[0]
        key,artifact,_=await self.caption(manifest_bbox=native,result_bbox=changed)
        before=Path(artifact.uri.removeprefix('file://')).read_bytes()
        with self.assertRaises(Conflict):await self.bridge.admit(key,expected_model_digest='a'*64)
        self.assertEqual(Path(artifact.uri.removeprefix('file://')).read_bytes(),before)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),1)

    async def test_arbitrary_non_float32_retained_geometry_is_not_rounded_into_compatibility(self):
        await self.h.publish(self.source['operation_id'])
        native=[0.0,0.0,0.9947954416275025,0.9737284779548645]
        rounded=[0.0,0.0,0.9947954416,0.973728478]
        key,_,_=await self.caption(manifest_bbox=native,result_bbox=rounded)
        with self.assertRaises(Conflict):await self.bridge.admit(key,expected_model_digest='a'*64)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),1)

    async def test_exact_json_crop_preserves_geometry_without_float32_conversion(self):
        await self.h.publish(self.source['operation_id'])
        native=[0.0,0.0,0.9947954416275025,0.9737284779548645]
        key,_,_=await self.caption(manifest_bbox=native)
        op=await self.bridge.admit(key,expected_model_digest='a'*64)
        _,snapshot=await self.h.publish(op['operation_id'])
        derived=next(r for r in snapshot['records'] if r['metadata']['origin']=='derived')
        self.assertEqual(derived['metadata']['content_metadata']['bbox'],native)
        self.assertEqual(derived['metadata']['caption_derivation']['geometry_encoding'],'exact_json')
