"""Real PostgreSQL qualification, restricted to an isolated loopback database."""
import copy
from dataclasses import replace
from datetime import timedelta
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from urllib.parse import urlparse
from uuid import UUID

from collection_control.artifacts import canonical,content_digest,load_artifact,record_digest
from collection_control.repository import Conflict
from collection_control.web_captions import WebCaptionRepository
from tests.collection_control import test_web_groups as groups
sha=groups.sha


@unittest.skipUnless(os.environ.get('WEB_CAPTION_TEST_DSN'),'Requires isolated web caption PostgreSQL')
class WebCaptionTests(unittest.IsolatedAsyncioTestCase):
    fixture=groups.WebGroupTests.fixture
    finalized=groups.WebGroupTests.finalized
    publish=groups.WebGroupTests.publish
    group=groups.WebGroupTests.group
    materialize=groups.WebGroupTests.materialize
    candidate=groups.WebGroupTests.candidate
    add_image=groups.WebGroupTests.add_image

    async def asyncSetUp(self):
        from unittest.mock import patch
        override=patch.dict(os.environ,{'COLLECTION_TEST_DSN':os.environ['WEB_CAPTION_TEST_DSN']});override.start();self.addCleanup(override.stop)
        dsn=urlparse(os.environ['COLLECTION_TEST_DSN'])
        self.assertEqual(dsn.port,55445)
        await groups.WebGroupTests.asyncSetUp(self)
        await self.pool.execute(Path('deployment/ingestion/captioning/001_caption_jobs.sql').read_text())
        await self.pool.execute('TRUNCATE caption_control.assets')
        await self.pool.execute(Path('deployment/ingestion/captioning/002_web_captions.sql').read_text())
        self.bridge=WebCaptionRepository(self.repo,'collection',self.contract,self.stage_root,self.raw_root,self.snapshots)
        self.calls=[]
    async def asyncTearDown(self):await groups.WebGroupTests.asyncTearDown(self)

    def embed(self,text):self.calls.append(text);return [0.0,1.0]+[0.0]*2046
    async def admit(self,key):return await self.bridge.admit(key,embed=self.embed,embedding_config_digest='c'*64,expected_model_digest='a'*64)

    async def ready(self,*,publish=True,shared=False,image_size=(64,64)):
        from nv_ingest_durable.artifacts import JsonResultStager,FileArtifactStore
        from nv_ingest_durable.caption_repository import CaptionManifestRepository
        from nv_ingest_durable.contracts import StageEnvelope
        from application.web_captions import stage_web_caption_handoff
        fixtures=[await self.fixture('page-a')]
        if shared:fixtures.append(await self.fixture('page-b'))
        self.group_id,items=await self.group(fixtures)
        image=await self.add_image(self.group_id,items,committed=False,image_size=image_size)
        if shared:
            group=await self.pool.fetchrow('SELECT * FROM collection_control.web_groups WHERE group_id=$1',self.group_id)
            manifest=json.loads(Path(urlparse(group['manifest_uri']).path).read_text())
            manifest['pages'][1]['assets']=copy.deepcopy(manifest['pages'][0]['assets'])
            encoded=canonical(manifest);path=self.raw_root/(sha(encoded)+'.group.json');path.write_bytes(encoded)
            await self.pool.execute('UPDATE collection_control.web_groups SET manifest_uri=$2,manifest_sha256=$3 WHERE group_id=$1',self.group_id,path.as_uri(),sha(encoded))
        row=await self.pool.fetchrow("SELECT * FROM public.stage_units WHERE document_id='image-doc'")
        envelope=StageEnvelope.from_stream_fields(json.loads(row['input_envelope']))
        models={'qualification_model':'checkpoint'};config={'qualified_image_adapter':'v1'}
        evidence={'schema_version':'native-web-image-evidence.v1','status':'complete','document_id':'image-doc','source_revision_id':'image-rev','raw_uri':image.as_uri(),'raw_sha256':sha(image.read_bytes()),'native':{'model_digest':envelope.model_digest,'config_digest':envelope.config_digest,'models':models,'config':config,'coordinate_frame':'normalized_image','internal_frame_number':1},'normalized':{'uri':image.as_uri(),'sha256':sha(image.read_bytes())},'text':'qualification OCR'}
        await self.pool.execute("UPDATE public.jobs SET metadata=metadata || '{\"ingestion_options\":{\"captions_enabled\":true}}'::jsonb WHERE job_id=$1",UUID(envelope.job_id))
        pending,downstream=await JsonResultStager(self.stage_root).stage(envelope,evidence)
        pending,downstream=await stage_web_caption_handoff(envelope,pending,downstream,stages=self.stage_root,raw_root=self.raw_root,caption_params=SimpleNamespace(model_name='qualified-caption-model'),model_digest='a'*64)
        artifact=await FileArtifactStore(self.stage_root).finalize(pending)
        queue=CaptionManifestRepository(self.legacy,self.stage_root)
        await queue.commit_stage(envelope,lease_token=row['lease_token'],artifact=artifact,downstream=downstream)
        if min(image_size)<32:
            self.assertEqual(downstream,())
            self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM caption_control.assets'),0)
            self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM public.outbox_events WHERE target_stream LIKE $1','%caption.pending'),0)
            committed=json.loads(Path(urlparse(artifact.uri).path).read_bytes())
            self.assertEqual(committed['caption_status'],'skipped')
            self.assertEqual(committed['caption_skip']['reason'],'below_caption_library_minimum')
            items=[await self.materialize(self.group_id,item) for item in items]
            for item in items:await self.candidate(self.group_id,item)
            op=await self.group_repo.admit_group(self.group_id)
            _,snapshot=await self.publish(op['operation_id'])
            self.assertIn('qualification OCR',snapshot['records'][0]['text'])
            self.assertEqual(await self.pool.fetchval("SELECT status FROM public.stage_units WHERE idempotency_key=$1",envelope.idempotency_key),'committed')
            return
        child=downstream[0][1]
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM caption_control.assets'),1)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM public.outbox_events WHERE target_stream LIKE $1','%caption.pending'),1)
        items=[await self.materialize(self.group_id,item) for item in items]
        for item in items:await self.candidate(self.group_id,item)
        op=await self.group_repo.admit_group(self.group_id)
        if publish:await self.publish(op['operation_id'])
        asset=await self.pool.fetchrow('SELECT * FROM caption_control.assets WHERE stage_key=$1',child.idempotency_key)
        manifest=json.loads(Path(urlparse(asset['manifest_uri']).path).read_text())
        lease=await queue.acquire_lease(child,owner='caption-qualified-fixture',ttl=timedelta(minutes=5))
        context=await queue.source_context(child)
        p={k:manifest[k] for k in ('parent_job_id','document_id','parent_stage_key','asset_sha256','asset_uri','source_metadata','model_digest')};p['config_digest']=child.config_digest;p['source_context']=context
        text='A generated machine description of the image'
        result={'payload_format':'es_docs','schema_version':'1.0','embedding_contract':self.contract,'embedding_config_digest':'c'*64,'caption_text_sha256':sha(text.encode()),'caption_asset_sha256':asset['asset_sha256'],'documents':[{'text':text,'vector':[1.0]+[0.0]*2047,'metadata':{'acl':context['acl'],'caption_provenance':p,'content_metadata':{'type':'image_caption','generated_annotation':True,'page_number':1,'bbox':manifest['bbox']}}}]}
        await queue.commit_stage(child,lease_token=lease,artifact=self.finalized(child,result),downstream=())
        self.original_op=op;self.child=child;self.image=image
        return child.idempotency_key

    async def test_small_image_skip_commits_and_parent_publishes_without_caption_job(self):
        await self.ready(image_size=(31,64))

    async def test_callback_waits_for_original_receipts_then_replaces_same_page(self):
        key=await self.ready(publish=False)
        with self.assertRaises(Conflict):await self.admit(key)
        self.assertEqual(self.calls,[])
        await self.publish(self.original_op['operation_id'])
        before=load_artifact(self.snapshots,self.original_op['artifact_sha256'])
        op=await self.admit(key);_,after=await self.publish(op['operation_id'])
        self.assertEqual(len(after['records']),1)
        self.assertEqual(after['records'][0]['record_id'],before['records'][0]['record_id'])
        self.assertIn('qualification OCR',after['records'][0]['text'])
        self.assertIn('generated machine description',after['records'][0]['text'])
        self.assertEqual(len(self.calls),1)
        self.assertEqual((await self.admit(key))['operation_id'],op['operation_id'])
        self.assertEqual(len(self.calls),1)

    async def test_selected_pair_receipts_and_historical_asset_retention(self):
        key=await self.ready();await self.pool.execute("UPDATE collection_control.collections SET milvus_enabled=true WHERE collection_id='collection'")
        op=await self.admit(key);lease=await self.repo.claim(op['operation_id'],ttl_seconds=60)
        snapshot=load_artifact(self.snapshots,op['artifact_sha256'])
        await self.repo.verify_admission(lease,snapshot)
        await self.repo.receipt(lease,'elasticsearch',content_digest(snapshot['records']),1)
        with self.assertRaises(Conflict):await self.repo.publish(lease)
        await self.repo.receipt(lease,'milvus',content_digest(snapshot['records']),1)
        self.image.write_bytes(b'corrupt retained original image')
        with self.assertRaises(Conflict):await self.repo.publish(lease)
        self.assertEqual(await self.pool.fetchval("SELECT current_operation FROM collection_control.collections WHERE collection_id='collection'"),self.original_op['operation_id'])

    async def test_collection_pointer_change_fences_embedded_callback(self):
        key=await self.ready()
        # A legitimate intervening generation changes the current pointer after
        # embedding; no stale parent snapshot is allowed to prepare.
        def embed_and_mark(text):
            import asyncio
            async def update():
                import asyncpg
                c=await asyncpg.connect(os.environ['COLLECTION_TEST_DSN'])
                await c.execute("UPDATE collection_control.collections SET revision=revision+1 WHERE collection_id='collection'")
                await c.close()
            asyncio.run(update());return self.embed(text)
        with self.assertRaisesRegex(Conflict,'changed during embedding'):
            await self.bridge.admit(key,embed=embed_and_mark,embedding_config_digest='c'*64,expected_model_digest='a'*64)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),1)

    async def test_opt_out_and_wrong_model_reject_before_embedding(self):
        key=await self.ready()
        with self.assertRaises(Conflict):await self.bridge.admit(key,embed=self.embed,embedding_config_digest='c'*64,expected_model_digest='b'*64)
        await self.pool.execute("UPDATE public.jobs SET metadata=jsonb_set(metadata,'{ingestion_options,captions_enabled}','false')")
        with self.assertRaises(Conflict):await self.admit(key)
        self.assertEqual(self.calls,[])

    async def test_pending_callback_replay_reuses_prepared_parent(self):
        key=await self.ready();op=await self.admit(key)
        replay=await self.admit(key)
        self.assertEqual(op['operation_id'],replay['operation_id'])
        self.assertEqual(len(self.calls),1)

    async def test_unlinked_callback_terminally_fenced(self):
        key=await self.ready()
        group,items=await self.group([await self.fixture('page-next',revision='revision-next',previous='srcrev-page-a',source_name='page-a')])
        await self.candidate(group,items[0]);op=await self.group_repo.admit_group(group);await self.publish(op['operation_id'])
        self.assertIsNone(await self.admit(key))
        self.assertEqual(self.calls,[])

    async def test_immutable_derived_proof_tamper_blocks_writer(self):
        key=await self.ready();op=await self.admit(key)
        lease=await self.repo.claim(op['operation_id'],ttl_seconds=60)
        row=await self.pool.fetchrow('SELECT * FROM collection_control.web_caption_derivations')
        Path(urlparse(row['proof_uri']).path).write_text('{}')
        with self.assertRaises(Conflict):await self.repo.verify_admission(lease,load_artifact(self.snapshots,op['artifact_sha256']))

    async def test_membership_and_unrelated_source_inherit_and_verify_same_parent(self):
        key=await self.ready();op=await self.admit(key);await self.publish(op['operation_id'])
        before=load_artifact(self.snapshots,op['artifact_sha256'])['records'][0]
        e,lease,body,_=await self.fixture('manual')
        await self.bridge_control_commit(e,lease,body)
        next_op=await self.pool.fetchrow("SELECT * FROM collection_control.operations WHERE status='pending'")
        _,snapshot=await self.publish(next_op['operation_id'])
        self.assertIn(before,snapshot['records'])
        col=await self.pool.fetchrow("SELECT * FROM collection_control.collections WHERE collection_id='collection'")
        enrolled=await self.repo.prepare('collection','web-caption-membership','enroll_milvus',col['revision'],next_op['artifact_sha256'],col['contract_sha256'])
        await self.publish(enrolled['operation_id'])

    async def bridge_control_commit(self,e,lease,body):
        from collection_control.stage_bridge import StageCollectionRepository
        bridge=StageCollectionRepository(self.legacy,self.repo,'collection',self.contract,self.stage_root,self.raw_root,self.snapshots)
        await bridge.commit_stage(e,lease_token=lease,artifact=self.finalized(e,body),downstream=())

    async def test_superseded_caption_callback_is_terminal_without_embedding(self):
        key=await self.ready()
        await self.pool.execute("UPDATE public.documents SET status='superseded' WHERE document_id='image-doc'")
        self.assertIsNone(await self.admit(key))
        self.assertEqual(self.calls,[])

    async def test_changed_caption_model_result_or_manifest_blocks_writer(self):
        key=await self.ready();op=await self.admit(key)
        lease=await self.repo.claim(op['operation_id'],ttl_seconds=60)
        stage=await self.pool.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1',key)
        from dataclasses import replace
        envelope=json.loads(stage['input_envelope']);envelope['model_digest']='b'*64
        await self.pool.execute('UPDATE public.stage_units SET input_envelope=$2::jsonb WHERE idempotency_key=$1',key,json.dumps(envelope))
        with self.assertRaises(Conflict):await self.repo.verify_admission(lease,load_artifact(self.snapshots,op['artifact_sha256']))

    async def test_empty_caption_result_isolated_from_original_publication(self):
        key=await self.ready()
        stage=await self.pool.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1',key)
        path=Path(urlparse(stage['artifact_uri']).path);result=json.loads(path.read_text())
        result['documents'][0]['text']='';result['caption_text_sha256']=sha(b'')
        encoded=canonical(result);path.write_bytes(encoded)
        await self.pool.execute('UPDATE public.stage_units SET artifact_digest=$2 WHERE idempotency_key=$1',key,'sha256:'+sha(encoded))
        with self.assertRaises(Conflict):await self.admit(key)
        self.assertEqual(self.calls,[])
        self.assertEqual(await self.pool.fetchval("SELECT current_operation FROM collection_control.collections WHERE collection_id='collection'"),self.original_op['operation_id'])

    async def test_shared_image_rematerializes_each_current_parent_once(self):
        key=await self.ready(shared=True)
        original=load_artifact(self.snapshots,self.original_op['artifact_sha256'])
        op=await self.admit(key);_,snapshot=await self.publish(op['operation_id'])
        self.assertEqual(len(snapshot['records']),2)
        self.assertEqual({r['record_id'] for r in snapshot['records']},{r['record_id'] for r in original['records']})
        self.assertTrue(all('generated machine description' in r['text'] for r in snapshot['records']))
        self.assertEqual(len(self.calls),2)


    async def test_historical_caption_policy_remains_verifiable_at_publication(self):
        from unittest.mock import patch
        from collection_control.web_captions import derived_page,LEGACY_POLICY
        def legacy(*args,**kwargs):return derived_page(*args,**{'policy':LEGACY_POLICY,**kwargs})
        with patch('collection_control.web_captions.derived_page',side_effect=legacy):
            await self.test_callback_waits_for_original_receipts_then_replaces_same_page()
