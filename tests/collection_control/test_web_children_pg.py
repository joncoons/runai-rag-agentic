"""Root-serialized disposable PostgreSQL qualification for additive web v2."""
import copy
import json
import os
from pathlib import Path
from uuid import UUID
import unittest

from collection_control.artifacts import canonical,load_artifact
from collection_control.repository import Conflict
from collection_control.web_groups import resolve_graph
from tests.collection_control import test_web_groups as groups


@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'Requires disposable local PostgreSQL, serialized by root')
class WebChildrenTests(unittest.IsolatedAsyncioTestCase):
    fixture=groups.WebGroupTests.fixture
    finalized=groups.WebGroupTests.finalized
    publish=groups.WebGroupTests.publish
    group=groups.WebGroupTests.group
    materialize=groups.WebGroupTests.materialize
    candidate=groups.WebGroupTests.candidate

    async def asyncSetUp(self):
        await groups.WebGroupTests.asyncSetUp(self)
        await self.pool.execute(Path('deployment/collection-control/005_web_children.sql').read_text())

    async def asyncTearDown(self):await groups.WebGroupTests.asyncTearDown(self)

    def retain(self,body,suffix='.json'):
        data=canonical(body);digest=groups.sha(data);path=self.raw_root/(digest+suffix);path.write_bytes(data)
        return {'uri':path.as_uri(),'sha256':digest}

    async def page_group(self,*,document='page-a',revision=None,previous=None,source_name=None,request='request:1',asset_uri='https://qualification.invalid/binary-a',kind='binary'):
        from application.provenance import enrich_checkpoint
        item=await self.fixture(document,revision=revision,previous=previous,source_name=source_name)
        group,items=await self.group([item]);e,lease,body,raw=items[0]
        native=body['native_source_units'];p=body['documents'][0]['metadata']['provenance']
        row=await self.pool.fetchrow('SELECT * FROM collection_control.web_groups WHERE group_id=$1',group)
        manifest=json.loads(Path(__import__('urllib.parse',fromlist=['urlparse']).urlparse(row['manifest_uri']).path).read_bytes())
        manifest['schema_version']='web-page-group.v2';manifest['pages'][0].pop('assets')
        manifest['pages'][0]['relations']=[{'request_id':request,'canonical_uri':asset_uri,'kind':kind,'locator':'a:1','alt':''}]
        ref=self.retain(manifest)
        await self.pool.execute('UPDATE collection_control.web_groups SET manifest_uri=$2,manifest_sha256=$3 WHERE group_id=$1',group,ref['uri'],ref['sha256'])
        metadata=json.loads(await self.pool.fetchval('SELECT metadata FROM public.jobs WHERE job_id=$1',UUID(e.job_id)))
        rev={'source_id':'source-'+e.document_id,'source_revision_id':p['source_revision_id'],'collection_id':'collection','document_id':e.document_id,'job_id':e.job_id,'canonical_uri':p['canonical_uri'],'raw_uri':raw.as_uri(),'raw_sha256':groups.sha(raw.read_bytes()),'ingestion_run_id':metadata['ingestion_run_id'],'filename':'captured-page.html','source_kind':'html','source_units':native,'observed_at':'2026-11-08T00:00:00Z','previous_source_revision_id':previous,'web_group_id':group}
        acquisition=self.retain({'schema_version':'web-acquisition.v2','final_uri':p['canonical_uri'],'raw_sha256':groups.sha(raw.read_bytes()),'capture_complete':True,'asset_processing':'deferred','asset_acquisition_complete':False,'policy':manifest['policy'],'assets':[{'url':r['canonical_uri'],'kind':r['kind'],'locator':r['locator'],'alt':r['alt']} for r in manifest['pages'][0]['relations']]})
        rev['acquisition']=acquisition
        checkpoint={'schema_version':'1.0','payload_format':'es_docs','native_source_units':native,'documents':[{'text':native['units'][0]['text'],'vector':[1.0]+[0.0]*2047,'metadata':{'content_metadata':{'source_unit_id':'page:1','type':'text'}}}]}
        enriched,source_manifest=enrich_checkpoint(checkpoint,rev,raw_root=self.raw_root,model_digest='e'*64,config_digest='c'*64,ocr_model_digest='f'*64)
        retained=self.retain(source_manifest);enriched['retained_source_capture']['source_manifest']['uri']=retained['uri']
        metadata['source_manifests'][e.document_id]=retained
        await self.pool.execute('UPDATE public.jobs SET metadata=$2::jsonb WHERE job_id=$1',UUID(e.job_id),json.dumps(metadata))
        return group,(e,lease,enriched,raw),manifest

    async def association(self,group,page_manifest,item,request='request:1'):
        e,lease,body,raw=item;p=body['documents'][0]['metadata']['provenance']
        for document in body['documents']:document['metadata']['acl']=['local-operator']
        await self.pool.execute("UPDATE public.jobs SET metadata=jsonb_set(metadata,'{acl}','[\"local-operator\"]'::jsonb) WHERE job_id=$1",UUID(e.job_id))
        asset={'kind':'binary','document_id':e.document_id,'source_revision_id':p['source_revision_id'],'canonical_uri':p['canonical_uri'],'raw_uri':raw.as_uri(),'raw_sha256':groups.sha(raw.read_bytes()),'media_type':'application/pdf','model_digest':e.model_digest,'config_digest':e.config_digest}
        download=self.retain({'schema_version':'deferred-web-asset-acquisition.v1','collection_id':'collection','request_id':request,'requested_uri':asset['canonical_uri'],'kind':'binary','raw_uri':asset['raw_uri'],'raw_sha256':asset['raw_sha256'],'media_type':asset['media_type'],'policy_sha256':groups.sha(canonical(page_manifest['policy']))})
        parent={k:page_manifest['pages'][0][k] for k in ('document_id','source_revision_id','canonical_uri')};parent['group_id']=str(group)
        return self.retain({'schema_version':'web-asset-publication.v2','collection_id':'collection','request_id':request,'acquisition':download,'parent_pages':[parent],'asset':asset})

    async def test_pending_binary_cannot_block_html_receipts(self):
        group,item,manifest=await self.page_group()
        await self.candidate(group,item);op=await self.group_repo.admit_group(group)
        _,snapshot=await self.publish(op['operation_id'])
        self.assertEqual(len(snapshot['records']),1)
        async with self.pool.acquire() as c:
            col=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1','collection')
            graph=await resolve_graph(c,col)
        self.assertEqual(graph['schema_version'],'web-published-graph.v2')
        self.assertEqual(graph['pages'][0]['assets'],[])
        self.assertEqual(len(graph['pages'][0]['relations']),1)

    async def test_binary_child_is_independent_and_replayable(self):
        group,item,manifest=await self.page_group();await self.candidate(group,item)
        op=await self.group_repo.admit_group(group);await self.publish(op['operation_id'])
        binary=await self.fixture('binary-a');ref=await self.association(group,manifest,binary)
        e,lease,body,_=binary
        await self.group_repo.commit_child_candidate(e,lease_token=lease,artifact=self.finalized(e,body),acquisition=ref)
        child=await self.group_repo.admit_child(ref)
        self.assertNotEqual(child['operation_id'],op['operation_id'])
        _,snapshot=await self.publish(child['operation_id']);self.assertEqual(len(snapshot['records']),2)
        self.assertEqual((await self.group_repo.admit_child(ref))['operation_id'],child['operation_id'])

    async def test_binary_child_waits_for_parent_receipts(self):
        group,item,manifest=await self.page_group();await self.candidate(group,item)
        await self.group_repo.admit_group(group)
        binary=await self.fixture('binary-a');ref=await self.association(group,manifest,binary)
        e,lease,body,_=binary
        await self.group_repo.commit_child_candidate(e,lease_token=lease,artifact=self.finalized(e,body),acquisition=ref)
        with self.assertRaises(Conflict):await self.group_repo.admit_child(ref)

    async def image_ready(self,*,captions=False):
        from dataclasses import replace
        from datetime import timedelta
        from io import BytesIO
        from PIL import Image
        from nv_ingest_durable.contracts import Stage
        from nv_ingest_durable.artifacts import JsonResultStager,FileArtifactStore
        from nv_ingest_durable.caption_repository import CaptionManifestRepository
        from application.web_captions import stage_web_caption_handoff
        from types import SimpleNamespace
        group,item,manifest=await self.page_group(kind='image',asset_uri='https://qualification.invalid/image-doc')
        if captions:await self.pool.execute("UPDATE public.jobs SET metadata=metadata || '{\"ingestion_options\":{\"captions_enabled\":true}}'::jsonb WHERE job_id=$1",UUID(item[0].job_id))
        await self.candidate(group,item);base=await self.group_repo.admit_group(group);await self.publish(base['operation_id'])
        e,_,_,_=await self.fixture('image-doc')
        buffer=BytesIO();Image.new('RGB',(64,64)).save(buffer,format='PNG');raw=buffer.getvalue();raw_hash=groups.sha(raw)
        path=self.raw_root/(raw_hash+'.png');path.write_bytes(raw)
        models={'qualification_model':'checkpoint'};config={'qualified_image_adapter':'v1'}
        md=groups.sha(canonical(models));cd=groups.sha(canonical(config))
        e=replace(e,stage=Stage.EXTRACTION,artifact_uri=path.as_uri(),idempotency_key='sha256:'+groups.sha(b'v2-image-stage'),model_digest=md,config_digest=cd)
        await self.pool.execute('UPDATE public.documents SET source_digest=$2 WHERE document_id=$1',e.document_id,'sha256:'+raw_hash)
        await self.pool.execute("UPDATE public.jobs SET metadata=metadata || $2::jsonb WHERE job_id=$1",UUID(e.job_id),json.dumps({'acl':['local-operator'],'ingestion_options':{'captions_enabled':captions}}))
        lease=await self.legacy.acquire_lease(e,owner='qualification',ttl=timedelta(minutes=5))
        await self.pool.execute('UPDATE public.stage_units SET input_envelope=$2::jsonb WHERE idempotency_key=$1',e.idempotency_key,json.dumps(e.to_dict()))
        evidence={'schema_version':'native-web-image-evidence.v1','status':'complete','document_id':e.document_id,'source_revision_id':'image-rev','raw_uri':path.as_uri(),'raw_sha256':raw_hash,'native':{'model_digest':md,'config_digest':cd,'models':models,'config':config,'coordinate_frame':'normalized_image','internal_frame_number':1},'normalized':{'uri':path.as_uri(),'sha256':raw_hash},'text':'independent native OCR'}
        pending,downstream=await JsonResultStager(self.stage_root).stage(e,evidence)
        queue=None
        if captions:
            await self.pool.execute(Path('deployment/ingestion/captioning/001_caption_jobs.sql').read_text())
            await self.pool.execute(Path('deployment/ingestion/captioning/002_web_captions.sql').read_text())
            pending,downstream=await stage_web_caption_handoff(e,pending,downstream,stages=self.stage_root,raw_root=self.raw_root,caption_params=SimpleNamespace(model_name='qualified-caption-model'),model_digest='a'*64)
            queue=CaptionManifestRepository(self.legacy,self.stage_root)
        artifact=await FileArtifactStore(self.stage_root).finalize(pending)
        await (queue or self.legacy).commit_stage(e,lease_token=lease,artifact=artifact,downstream=downstream)
        asset={'kind':'image','document_id':e.document_id,'source_revision_id':'image-rev','canonical_uri':'https://qualification.invalid/image-doc','raw_uri':path.as_uri(),'raw_sha256':raw_hash,'media_type':'image/png','model_digest':md,'config_digest':cd,'extraction_stage_key':e.idempotency_key}
        acquisition=self.retain({'schema_version':'deferred-web-asset-acquisition.v1','collection_id':'collection','request_id':'request:1','requested_uri':asset['canonical_uri'],'kind':'image','raw_uri':path.as_uri(),'raw_sha256':raw_hash,'media_type':'image/png','policy_sha256':groups.sha(canonical(manifest['policy']))})
        parent={k:manifest['pages'][0][k] for k in ('document_id','source_revision_id','canonical_uri')};parent['group_id']=str(group)
        association=self.retain({'schema_version':'web-asset-publication.v2','collection_id':'collection','request_id':'request:1','acquisition':acquisition,'parent_pages':[parent],'asset':asset})
        return base,association,queue,downstream

    async def test_native_image_reembeds_the_same_selected_page(self):
        base,association,_,_=await self.image_ready()
        calls=[]
        def embed(text):calls.append(text);return [0.0,1.0]+[0.0]*2046
        child=await self.group_repo.admit_child(association,embed=embed,embedding_config_digest='c'*64)
        _,snapshot=await self.publish(child['operation_id'])
        self.assertEqual(len(snapshot['records']),1)
        self.assertIn('independent native OCR',snapshot['records'][0]['text'])
        original=load_artifact(self.snapshots,base['artifact_sha256'])['records'][0]
        self.assertEqual(snapshot['records'][0]['record_id'],original['record_id'])
        self.assertEqual(len(calls),1)
        self.assertEqual((await self.group_repo.admit_child(association,embed=embed,embedding_config_digest='c'*64))['operation_id'],child['operation_id'])
        self.assertEqual(len(calls),1)

    async def test_optional_caption_preserves_native_evidence_and_same_vector_identity(self):
        from datetime import timedelta
        from urllib.parse import urlparse
        from collection_control.web_captions import WebCaptionRepository
        base,association,queue,downstream=await self.image_ready(captions=True)
        embed=lambda text:[0.0,1.0]+[0.0]*2046
        native=await self.group_repo.admit_child(association,embed=embed,embedding_config_digest='c'*64);await self.publish(native['operation_id'])
        child=downstream[0][1]
        asset=await self.pool.fetchrow('SELECT * FROM caption_control.assets WHERE stage_key=$1',child.idempotency_key)
        manifest=json.loads(Path(urlparse(asset['manifest_uri']).path).read_text())
        lease=await queue.acquire_lease(child,owner='caption-qualified-fixture',ttl=timedelta(minutes=5));context=await queue.source_context(child)
        p={k:manifest[k] for k in ('parent_job_id','document_id','parent_stage_key','asset_sha256','asset_uri','source_metadata','model_digest')};p['config_digest']=child.config_digest;p['source_context']=context
        text='An optional generated image description'
        result={'payload_format':'es_docs','schema_version':'1.0','embedding_contract':self.contract,'embedding_config_digest':'c'*64,'caption_text_sha256':groups.sha(text.encode()),'caption_asset_sha256':asset['asset_sha256'],'documents':[{'text':text,'vector':[1.0]+[0.0]*2047,'metadata':{'acl':context['acl'],'caption_provenance':p,'content_metadata':{'type':'image_caption','generated_annotation':True,'page_number':1,'bbox':manifest['bbox']}}}]}
        await queue.commit_stage(child,lease_token=lease,artifact=self.finalized(child,result),downstream=())
        bridge=WebCaptionRepository(self.repo,'collection',self.contract,self.stage_root,self.raw_root,self.snapshots)
        caption=await bridge.admit(child.idempotency_key,embed=embed,embedding_config_digest='c'*64,expected_model_digest='a'*64)
        _,snapshot=await self.publish(caption['operation_id'])
        self.assertEqual(len(snapshot['records']),1)
        self.assertIn('independent native OCR',snapshot['records'][0]['text'])
        self.assertIn(text,snapshot['records'][0]['text'])
        self.assertEqual(snapshot['records'][0]['record_id'],load_artifact(self.snapshots,base['artifact_sha256'])['records'][0]['record_id'])

    async def committed_caption(self,queue,downstream):
        from datetime import timedelta
        from urllib.parse import urlparse
        child=downstream[0][1]
        asset=await self.pool.fetchrow('SELECT * FROM caption_control.assets WHERE stage_key=$1',child.idempotency_key)
        manifest=json.loads(Path(urlparse(asset['manifest_uri']).path).read_text())
        lease=await queue.acquire_lease(child,owner='caption-qualified-fixture',ttl=timedelta(minutes=5));context=await queue.source_context(child)
        p={k:manifest[k] for k in ('parent_job_id','document_id','parent_stage_key','asset_sha256','asset_uri','source_metadata','model_digest')};p['config_digest']=child.config_digest;p['source_context']=context
        text='An optional generated image description'
        result={'payload_format':'es_docs','schema_version':'1.0','embedding_contract':self.contract,'embedding_config_digest':'c'*64,'caption_text_sha256':groups.sha(text.encode()),'caption_asset_sha256':asset['asset_sha256'],'documents':[{'text':text,'vector':[1.0]+[0.0]*2047,'metadata':{'acl':context['acl'],'caption_provenance':p,'content_metadata':{'type':'image_caption','generated_annotation':True,'page_number':1,'bbox':manifest['bbox']}}}]}
        await queue.commit_stage(child,lease_token=lease,artifact=self.finalized(child,result),downstream=())
        return child.idempotency_key,text

    async def test_shared_image_caption_respects_each_parent_optin(self):
        from urllib.parse import urlparse
        from collection_control.web_captions import WebCaptionRepository
        _,association,queue,downstream=await self.image_ready(captions=True)
        embed=lambda text:[0.0,1.0]+[0.0]*2046
        native=await self.group_repo.admit_child(association,embed=embed,embedding_config_digest='c'*64);await self.publish(native['operation_id'])
        group,item,manifest=await self.page_group(document='page-b',kind='image',asset_uri='https://qualification.invalid/image-doc')
        await self.candidate(group,item);op=await self.group_repo.admit_group(group);await self.publish(op['operation_id'])
        proof=json.loads(Path(urlparse(association['uri']).path).read_text())
        proof['parent_pages'].append({'group_id':str(group),**{k:manifest['pages'][0][k] for k in ('document_id','source_revision_id','canonical_uri')}})
        combined=self.retain(proof)
        attached=await self.group_repo.admit_child(combined,embed=embed,embedding_config_digest='c'*64);await self.publish(attached['operation_id'])
        key,text=await self.committed_caption(queue,downstream)
        bridge=WebCaptionRepository(self.repo,'collection',self.contract,self.stage_root,self.raw_root,self.snapshots)
        op=await bridge.admit(key,embed=embed,embedding_config_digest='c'*64,expected_model_digest='a'*64)
        _,snapshot=await self.publish(op['operation_id'])
        selected={r['metadata']['provenance']['canonical_uri']:r for r in snapshot['records']}
        self.assertIn(text,selected['https://qualification.invalid/page-a']['text'])
        self.assertNotIn(text,selected['https://qualification.invalid/page-b']['text'])
        self.assertIn('independent native OCR',selected['https://qualification.invalid/page-b']['text'])
        # Replaying the old association after a newer combined association is a no-op.
        count=await self.pool.fetchval('SELECT count(*) FROM collection_control.operations')
        await self.group_repo.admit_child(association,embed=embed,embedding_config_digest='c'*64)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),count)

    async def test_parent_caption_optout_is_rechecked_before_receipts(self):
        from collection_control.web_captions import WebCaptionRepository
        _,association,queue,downstream=await self.image_ready(captions=True)
        embed=lambda text:[0.0,1.0]+[0.0]*2046
        native=await self.group_repo.admit_child(association,embed=embed,embedding_config_digest='c'*64);await self.publish(native['operation_id'])
        key,_=await self.committed_caption(queue,downstream)
        bridge=WebCaptionRepository(self.repo,'collection',self.contract,self.stage_root,self.raw_root,self.snapshots)
        op=await bridge.admit(key,embed=embed,embedding_config_digest='c'*64,expected_model_digest='a'*64)
        parent_job=await self.pool.fetchval("SELECT job_id FROM collection_control.web_groups WHERE group_id=(SELECT group_id FROM collection_control.web_group_members WHERE document_id='page-a')")
        await self.pool.execute("UPDATE public.jobs SET metadata=jsonb_set(metadata,'{ingestion_options,captions_enabled}','false'::jsonb) WHERE job_id=$1",parent_job)
        lease=await self.repo.claim(op['operation_id'],ttl_seconds=60)
        with self.assertRaises(Conflict):await self.repo.verify_admission(lease,load_artifact(self.snapshots,op['artifact_sha256']))

    async def test_native_replacement_drops_old_caption_and_stale_callback_is_terminal(self):
        from dataclasses import replace
        from datetime import timedelta
        from urllib.parse import urlparse
        from nv_ingest_durable.contracts import StageEnvelope
        from collection_control.web_captions import WebCaptionRepository
        _,association,queue,downstream=await self.image_ready(captions=True)
        embed=lambda text:[0.0,1.0]+[0.0]*2046
        native=await self.group_repo.admit_child(association,embed=embed,embedding_config_digest='c'*64);await self.publish(native['operation_id'])
        key,caption_text=await self.committed_caption(queue,downstream)
        bridge=WebCaptionRepository(self.repo,'collection',self.contract,self.stage_root,self.raw_root,self.snapshots)
        op=await bridge.admit(key,embed=embed,embedding_config_digest='c'*64,expected_model_digest='a'*64);await self.publish(op['operation_id'])
        proof=json.loads(Path(urlparse(association['uri']).path).read_text());old=proof['asset']
        stage=await self.pool.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1',old['extraction_stage_key'])
        original=StageEnvelope.from_stream_fields(json.loads(stage['input_envelope']))
        new=replace(original,document_id='image-doc-new',idempotency_key='sha256:'+groups.sha(b'new-native-image-stage'))
        await self.pool.execute("INSERT INTO public.documents(document_id,job_id,source_uri,source_digest,status) VALUES($1,$2,$3,$4,'processing')",new.document_id,UUID(new.job_id),old['canonical_uri'],'sha256:'+old['raw_sha256'])
        lease=await self.legacy.acquire_lease(new,owner='qualification',ttl=timedelta(minutes=5))
        await self.pool.execute('UPDATE public.stage_units SET input_envelope=$2::jsonb WHERE idempotency_key=$1',new.idempotency_key,json.dumps(new.to_dict()))
        evidence=json.loads(Path(urlparse(stage['artifact_uri']).path).read_text());evidence.update(document_id=new.document_id,source_revision_id='new-image-revision',text='replacement native OCR')
        evidence.pop('caption_jobs',None)
        await self.legacy.commit_stage(new,lease_token=lease,artifact=self.finalized(new,evidence),downstream=())
        proof['asset']={**old,'document_id':new.document_id,'source_revision_id':'new-image-revision','extraction_stage_key':new.idempotency_key}
        new_ref=self.retain(proof)
        op=await self.group_repo.admit_child(new_ref,embed=embed,embedding_config_digest='c'*64)
        _,snapshot=await self.publish(op['operation_id'])
        self.assertIn('replacement native OCR',snapshot['records'][0]['text'])
        self.assertNotIn(caption_text,snapshot['records'][0]['text'])
        self.assertEqual(snapshot['records'][0]['metadata']['web_enrichment_derivation']['captions'],[])
        self.assertIsNone(await bridge.admit(key,embed=embed,embedding_config_digest='c'*64,expected_model_digest='a'*64))
