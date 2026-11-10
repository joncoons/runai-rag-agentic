"""Group atomicity qualification uses only a guarded disposable local database."""
import copy
import json
import os
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from uuid import UUID,uuid4

from collection_control.artifacts import canonical,content_digest,load_artifact
from collection_control.repository import Conflict
from collection_control.web_groups import GroupRepository,merge_graph,register_group,resolve_graph
from tests.collection_control import test_stage_bridge as fixtures_module
sha=fixtures_module.sha


class GraphTests(unittest.TestCase):
    def asset(self,kind='binary',revision='r1',digest='a'*64):
        return {'canonical_uri':'https://qualification.invalid/asset','kind':kind,'source_revision_id':revision,'raw_sha256':digest,'document_id':'asset-'+revision,'raw_uri':'file:///test','media_type':'application/pdf'}

    def manifest(self,pages,coverage='complete'):
        return {'schema_version':'web-asset-group.v1','coverage':coverage,'pages':pages,'policy':{'include_images':True}}

    def page(self,name,assets):return {'canonical_uri':name,'assets':assets}

    def test_partial_manifest_cannot_unlink(self):
        with self.assertRaises(Conflict):merge_graph({'pages':[]},self.manifest([],coverage='partial'))

    def test_shared_link_survives_one_page_unlink(self):
        old={'pages':[self.page('a',[self.asset()]),self.page('b',[self.asset()])]}
        graph=merge_graph(old,self.manifest([self.page('a',[])]))
        self.assertEqual([len(p['assets']) for p in graph['pages']],[0,1])

    def test_changed_shared_image_requires_all_affected_pages(self):
        old={'pages':[self.page('a',[self.asset('image')]),self.page('b',[self.asset('image')])]}
        with self.assertRaises(Conflict):merge_graph(old,self.manifest([self.page('a',[self.asset('image','r2','b'*64)])]))

    def test_shared_binary_revision_changes_all_surviving_edges(self):
        old={'pages':[self.page('a',[self.asset()]),self.page('b',[self.asset()])]}
        graph=merge_graph(old,self.manifest([self.page('a',[self.asset('binary','r2','b'*64)])]))
        self.assertEqual({p['assets'][0]['source_revision_id'] for p in graph['pages']},{'r2'})

    def test_policy_change_does_not_prove_unlink(self):
        old={'pages':[self.page('a',[self.asset()])],'policy':{'include_images':False}}
        with self.assertRaises(Conflict):merge_graph(old,self.manifest([self.page('a',[])]))


    def test_unrelated_capture_cannot_change_another_pages_unlink_policy(self):
        policy_p={'included_extensions':['.pdf'],'include_images':True}
        policy_q={'included_extensions':[],'include_images':False}
        first=self.manifest([self.page('a',[self.asset()])]);first['policy']=policy_p
        graph=merge_graph({'pages':[]},first)
        second=self.manifest([self.page('b',[])]);second['policy']=policy_q
        graph=merge_graph(graph,second)
        self.assertEqual(graph['pages'][0]['policy'],policy_p)
        self.assertEqual(graph['pages'][1]['policy'],policy_q)
        third=self.manifest([self.page('a',[])]);third['policy']=policy_q
        with self.assertRaises(Conflict):merge_graph(graph,third)
        self.assertEqual(len(graph['pages'][0]['assets']),1)
        # Actual disappearance under A's original selection remains valid.
        third['policy']=policy_p
        reconciled=merge_graph(graph,third)
        self.assertEqual(reconciled['pages'][0]['assets'],[])
        self.assertEqual(reconciled['pages'][1]['policy'],policy_q)


@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'Requires disposable local PostgreSQL')
class WebGroupTests(unittest.IsolatedAsyncioTestCase):
    fixture=fixtures_module.StageBridgeTests.fixture
    finalized=fixtures_module.StageBridgeTests.finalized
    publish=fixtures_module.StageBridgeTests.publish

    async def asyncSetUp(self):
        # Reuse the qualification fixture's explicit hostname/db/role guards.
        await fixtures_module.StageBridgeTests.asyncSetUp(self)
        await self.pool.execute(Path('deployment/collection-control/004_web_groups.sql').read_text())
        self.group_repo=GroupRepository(self.legacy,self.repo,'collection',self.contract,self.stage_root,self.raw_root,self.snapshots)

    async def asyncTearDown(self):await fixtures_module.StageBridgeTests.asyncTearDown(self)

    async def group(self,fixtures,*,materialize=True):
        job=UUID(fixtures[0][0].job_id)
        base=json.loads(await self.pool.fetchval('SELECT metadata FROM public.jobs WHERE job_id=$1',job));base['acl']=['local-operator']
        adjusted=[]
        for e,lease,body,raw in fixtures:
            if UUID(e.job_id)!=job:
                metadata=json.loads(await self.pool.fetchval('SELECT metadata FROM public.jobs WHERE job_id=$1',UUID(e.job_id)))
                base['source_manifests'].update(metadata['source_manifests'])
                e=replace(e,job_id=str(job))
                await self.pool.execute('UPDATE public.documents SET job_id=$2 WHERE document_id=$1',e.document_id,job)
                await self.pool.execute('UPDATE public.stage_units SET job_id=$2,input_envelope=$3::jsonb WHERE idempotency_key=$1',e.idempotency_key,job,json.dumps(e.to_dict()))
                for d in body['documents']:
                    d['metadata']['provenance']['ingestion_run_id']=base['ingestion_run_id']
                    d['metadata']['content_metadata']['ingestion_run_id']=base['ingestion_run_id']
            for d in body['documents']:d['metadata']['acl']=['local-operator']
            adjusted.append((e,lease,body,raw))
        await self.pool.execute('UPDATE public.jobs SET metadata=$2::jsonb WHERE job_id=$1',job,json.dumps(base))
        group_id=uuid4();members=[];pages=[]
        for e,_,body,_ in adjusted:
            p=body['documents'][0]['metadata']['provenance']
            m={'document_id':e.document_id,'source_revision_id':p['source_revision_id'],'role':'binary' if e.document_id.startswith('binary') else 'page','canonical_uri':p['canonical_uri']};members.append(m)
            if m['role']=='page':pages.append({**{k:v for k,v in m.items() if k!='role'},'assets':[]})
        for e,_,body,_ in adjusted:
            if not e.document_id.startswith('binary'):continue
            p=body['documents'][0]['metadata']['provenance']
            for page in pages:page['assets'].append({'kind':'binary','canonical_uri':p['canonical_uri'],'document_id':e.document_id,'source_revision_id':p['source_revision_id'],'raw_uri':p['raw_artifact_uri'],'raw_sha256':p['hashes']['raw_sha256'],'media_type':'application/pdf'})
        manifest={'schema_version':'web-asset-group.v1','group_id':str(group_id),'collection_id':'collection','job_id':str(job),'pages':pages,'members':members,'coverage':'complete','policy':{}}
        data=canonical(manifest);path=self.raw_root/(sha(data)+'.group.json');path.write_bytes(data)
        async with self.pool.acquire() as c,c.transaction():
            await register_group(c,group_id=group_id,collection_id='collection',job_id=job,manifest_uri=path.as_uri(),manifest_sha256=sha(data),members=members)
        if materialize:adjusted=[await self.materialize(group_id,item) if not item[0].document_id.startswith('binary') else item for item in adjusted]
        return group_id,adjusted

    async def materialize(self,group_id,item):
        from ingestion.document_formats import inspect_document
        from application.provenance import enrich_checkpoint
        from application.web_materializer import page_materialization
        e,lease,body,_=item;p=body['documents'][0]['metadata']['provenance']
        previous=p.get('previous_source_revision_id');revision_id=p['source_revision_id'];uri=p['canonical_uri']
        # Retain real HTML and derive its exact qualified whole-page unit.
        raw=('<html><body><main><h1>Qualification</h1><p>Observed HTML '+e.document_id+' '+revision_id+'</p></main></body></html>').encode()
        raw_sha=sha(raw);raw_path=self.raw_root/(raw_sha+'.html');raw_path.write_bytes(raw)
        inspected=inspect_document('captured-page.html',raw)
        native={'schema_version':'native-source-units.v1','source_kind':'html','transform':inspected['transform'],'units':inspected['units']}
        metadata=json.loads(await self.pool.fetchval('SELECT metadata FROM public.jobs WHERE job_id=$1',UUID(e.job_id)))
        revision={'source_id':'source-'+e.document_id,'source_revision_id':revision_id,'collection_id':'collection','document_id':e.document_id,'job_id':e.job_id,'canonical_uri':uri,'raw_uri':raw_path.as_uri(),'raw_sha256':raw_sha,'ingestion_run_id':metadata['ingestion_run_id'],'filename':'captured-page.html','source_kind':'html','source_units':native,'observed_at':'2026-11-08T00:00:00Z','previous_source_revision_id':previous,'web_group_id':group_id}
        proof,text=await page_materialization(self.pool,revision,self.raw_root,native)
        checkpoint={'schema_version':'1.0','payload_format':'es_docs','native_source_units':native,'web_materialization':proof,'documents':[{'text':text,'vector':[1.0]+[0.0]*2047,'metadata':{'content_metadata':{'source_unit_id':'page:1','type':'text'}}}]}
        enriched,manifest=enrich_checkpoint(checkpoint,revision,raw_root=self.raw_root,model_digest='e'*64,config_digest='c'*64,ocr_model_digest='f'*64)
        encoded=canonical(manifest);manifest_sha=sha(encoded);manifest_path=self.raw_root/(manifest_sha+'.json');manifest_path.write_bytes(encoded)
        enriched['retained_source_capture']['source_manifest']['uri']=manifest_path.as_uri()
        metadata['source_manifests'][e.document_id]={'uri':manifest_path.as_uri(),'sha256':manifest_sha}
        await self.pool.execute('UPDATE public.jobs SET metadata=$2::jsonb WHERE job_id=$1',UUID(e.job_id),json.dumps(metadata))
        await self.pool.execute('UPDATE public.documents SET source_digest=$2 WHERE document_id=$1',e.document_id,'sha256:'+raw_sha)
        e=replace(e,artifact_uri=raw_path.as_uri())
        await self.pool.execute('UPDATE public.stage_units SET input_envelope=$2::jsonb WHERE idempotency_key=$1',e.idempotency_key,json.dumps(e.to_dict()))
        return e,lease,enriched,raw_path

    async def candidate(self,group_id,fixture):
        e,lease,body,_=fixture
        await self.group_repo.commit_candidate(e,lease_token=lease,artifact=self.finalized(e,body),group_id=group_id)

    async def test_candidates_withhold_operation_until_whole_group_ready(self):
        group,fixtures=await self.group([await self.fixture('page-a'),await self.fixture('page-b')])
        await self.candidate(group,fixtures[0])
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),0)
        with self.assertRaises(Conflict):await self.group_repo.admit_group(group)
        await self.candidate(group,fixtures[1])
        op=await self.group_repo.admit_group(group)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),1)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.stage_admissions'),2)
        self.assertEqual((await self.group_repo.admit_group(group))['operation_id'],op['operation_id'])
        _,snapshot=await self.publish(op['operation_id'])
        self.assertEqual(len(snapshot['records']),2)
        async with self.pool.acquire() as c:
            col=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1','collection')
            self.assertEqual(len((await resolve_graph(c,col))['pages']),2)

    async def test_candidate_replay_and_tamper(self):
        group,fixtures=await self.group([await self.fixture()])
        await self.candidate(group,fixtures[0]);await self.candidate(group,fixtures[0])
        e,lease,body,raw=fixtures[0];changed=copy.deepcopy(body);changed['documents'][0]['text']='tampered'
        with self.assertRaises(Conflict):await self.candidate(group,(e,lease,changed,raw))
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),0)

    async def test_one_store_receipt_does_not_publish_group(self):
        await self.pool.execute("UPDATE collection_control.collections SET milvus_enabled=true WHERE collection_id='collection'")
        group,fixtures=await self.group([await self.fixture()]);await self.candidate(group,fixtures[0]);op=await self.group_repo.admit_group(group)
        lease=await self.repo.claim(op['operation_id'],ttl_seconds=60);artifact=load_artifact(self.snapshots,op['artifact_sha256'])
        await self.repo.receipt(lease,'elasticsearch',content_digest(artifact['records']),len(artifact['records']))
        with self.assertRaises(Conflict):await self.repo.publish(lease)
        self.assertIsNone(await self.pool.fetchval("SELECT current_operation FROM collection_control.collections WHERE collection_id='collection'"))
        await self.repo.receipt(lease,'milvus',content_digest(artifact['records']),len(artifact['records']))
        await self.repo.publish(lease)

    async def test_shared_binary_only_retires_after_last_complete_page_unlink(self):
        group,items=await self.group([await self.fixture('page-a'),await self.fixture('page-b'),await self.fixture('binary-c')])
        for item in items:await self.candidate(group,item)
        op=await self.group_repo.admit_group(group);await self.publish(op['operation_id'])
        group,items=await self.group([await self.fixture('page-a-new',revision='a-next',previous='srcrev-page-a',source_name='page-a')])
        await self.candidate(group,items[0]);op=await self.group_repo.admit_group(group)
        _,snapshot=await self.publish(op['operation_id'])
        self.assertEqual(len(snapshot['records']),3)
        self.assertTrue(any(r['metadata']['provenance']['canonical_uri'].endswith('binary-c') for r in snapshot['records']))
        group,items=await self.group([await self.fixture('page-b-new',revision='b-next',previous='srcrev-page-b',source_name='page-b')])
        await self.candidate(group,items[0]);op=await self.group_repo.admit_group(group)
        _,snapshot=await self.publish(op['operation_id'])
        self.assertEqual(len(snapshot['records']),2)
        self.assertFalse(any(r['metadata']['provenance']['canonical_uri'].endswith('binary-c') for r in snapshot['records']))

    async def test_ungrouped_source_and_membership_inherit_graph(self):
        group,items=await self.group([await self.fixture('page-a')]);await self.candidate(group,items[0])
        op=await self.group_repo.admit_group(group);await self.publish(op['operation_id'])
        e,lease,body,_=await self.fixture('manual')
        await self.bridge.commit_stage(e,lease_token=lease,artifact=self.finalized(e,body),downstream=())
        next_op=await self.pool.fetchrow("SELECT * FROM collection_control.operations WHERE status='pending'")
        await self.publish(next_op['operation_id'])
        self.assertIsNotNone(await self.pool.fetchval('SELECT manifest_sha256 FROM collection_control.web_generation_manifests WHERE operation_id=$1',next_op['operation_id']))
        col=await self.pool.fetchrow("SELECT * FROM collection_control.collections WHERE collection_id='collection'")
        enrolled=await self.repo.prepare('collection','enroll','enroll_milvus',col['revision'],next_op['artifact_sha256'],col['contract_sha256'])
        await self.publish(enrolled['operation_id'])
        self.assertIsNotNone(await self.pool.fetchval('SELECT manifest_sha256 FROM collection_control.web_generation_manifests WHERE operation_id=$1',enrolled['operation_id']))

    async def add_image(self,group,fixtures,*,committed=False,image_size=(64,64)):
        from datetime import timedelta
        row=await self.pool.fetchrow('SELECT * FROM collection_control.web_groups WHERE group_id=$1',group)
        from urllib.parse import urlparse
        manifest=json.loads(Path(urlparse(row['manifest_uri']).path).read_bytes())
        from io import BytesIO
        from PIL import Image
        buffer=BytesIO();Image.new('RGB',image_size).save(buffer,format='PNG')
        image=buffer.getvalue();image_sha=sha(image);image_path=self.raw_root/image_sha;image_path.write_bytes(image)
        models={'qualification_model':'checkpoint'};config={'qualified_image_adapter':'v1'}
        asset={'document_id':'image-doc','source_revision_id':'image-rev','canonical_uri':'https://qualification.invalid/image','kind':'image','raw_sha256':image_sha,'raw_uri':image_path.as_uri(),'media_type':'image/png','model_digest':sha(canonical(models)),'config_digest':sha(canonical(config))}
        member={k:asset[k] for k in ('document_id','source_revision_id','canonical_uri')};member['role']='image'
        manifest['members'].append(member);manifest['pages'][0]['assets'].append(asset)
        data=canonical(manifest);path=self.raw_root/(sha(data)+'.group.json');path.write_bytes(data)
        await self.pool.execute("INSERT INTO public.documents(document_id,job_id,source_uri,source_digest,status) VALUES($1,$2,$3,$4,'processing')",asset['document_id'],row['job_id'],asset['canonical_uri'],'sha256:'+image_sha)
        await self.pool.execute('INSERT INTO collection_control.web_group_members(group_id,document_id,source_revision_id,role,canonical_uri) VALUES($1,$2,$3,$4,$5)',group,asset['document_id'],asset['source_revision_id'],'image',asset['canonical_uri'])
        await self.pool.execute('UPDATE collection_control.web_groups SET manifest_uri=$2,manifest_sha256=$3 WHERE group_id=$1',group,path.as_uri(),sha(data))
        e=replace(fixtures[0][0],document_id=asset['document_id'],stage=fixtures_module.Stage.EXTRACTION,artifact_uri=image_path.as_uri(),idempotency_key='sha256:'+sha(b'image-stage'),model_digest=asset['model_digest'],config_digest=asset['config_digest'])
        lease=await self.legacy.acquire_lease(e,owner='qualification',ttl=timedelta(minutes=5))
        await self.pool.execute('UPDATE public.stage_units SET input_envelope=$2::jsonb WHERE idempotency_key=$1',e.idempotency_key,json.dumps(e.to_dict()))
        if committed:
            evidence={'schema_version':'native-web-image-evidence.v1','status':'complete','document_id':asset['document_id'],'source_revision_id':asset['source_revision_id'],'raw_uri':asset['raw_uri'],'raw_sha256':image_sha,'native':{'model_digest':asset['model_digest'],'config_digest':asset['config_digest'],'models':models,'config':config,'coordinate_frame':'normalized_image','internal_frame_number':1},'normalized':{'uri':image_path.as_uri(),'sha256':image_sha},'text':'qualification OCR'}
            await self.legacy.commit_stage(e,lease_token=lease,artifact=self.finalized(e,evidence),downstream=())
            for index,item in enumerate(fixtures):
                if not item[0].document_id.startswith('binary'):fixtures[index]=await self.materialize(group,item)
        return image_path

    async def test_missing_image_completion_withholds_entire_group(self):
        group,items=await self.group([await self.fixture('page-a')]);await self.add_image(group,items)
        with self.assertRaises(Conflict):await self.candidate(group,items[0])
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),0)

    async def test_image_retention_is_revalidated_before_publication(self):
        group,items=await self.group([await self.fixture('page-a')]);path=await self.add_image(group,items,committed=True)
        await self.candidate(group,items[0]);op=await self.group_repo.admit_group(group)
        lease=await self.repo.claim(op['operation_id'],ttl_seconds=60);artifact=load_artifact(self.snapshots,op['artifact_sha256'])
        await self.repo.verify_admission(lease,artifact)
        await self.repo.receipt(lease,'elasticsearch',content_digest(artifact['records']),len(artifact['records']))
        path.unlink()
        with self.assertRaises(Conflict):await self.repo.publish(lease)
        self.assertIsNone(await self.pool.fetchval("SELECT current_operation FROM collection_control.collections WHERE collection_id='collection'"))

    async def test_missing_required_graph_proof_fails_closed(self):
        group,items=await self.group([await self.fixture('page-a')]);await self.candidate(group,items[0]);op=await self.group_repo.admit_group(group)
        lease=await self.repo.claim(op['operation_id'],ttl_seconds=60);artifact=load_artifact(self.snapshots,op['artifact_sha256'])
        await self.pool.execute('DELETE FROM collection_control.web_generation_manifests WHERE operation_id=$1',op['operation_id'])
        with self.assertRaises(Conflict):await self.repo.verify_admission(lease,artifact)

    async def test_empty_unadmitted_generation_cannot_bypass_strict_writer(self):
        from collection_control.provenance import store_snapshot
        from collection_control.writer import GenerationWriter
        digest=store_snapshot(self.snapshots,{'contract':self.contract,'records':[]})
        op=await self.repo.prepare('collection','unproved-empty','snapshot',0,digest,sha(canonical(self.contract)))
        class Store:
            async def write_verify(self,*args):raise AssertionError('Unproved empty generation reached VDB')
        writer=GenerationWriter(self.repo,self.snapshots,{'elasticsearch':Store()},allow_synthetic=True)
        with self.assertRaises(Conflict):await writer.run(op['operation_id'])
        self.assertEqual(await self.pool.fetchval('SELECT status FROM collection_control.operations WHERE operation_id=$1',op['operation_id']),'cancelled')

    async def test_non_html_source_cannot_fill_page_role(self):
        group,items=await self.group([await self.fixture('pdf-page')],materialize=False)
        e,lease,body,raw=items[0]
        body['native_source_units']={'schema_version':'native-source-units.v1','source_kind':'pdf','units':[{'unit_id':'page:1','status':'complete','text':'PDF text','anchors':{'section':'page'}}]}
        with self.assertRaisesRegex(Conflict,'native HTML'):await self.candidate(group,(e,lease,body,raw))
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.web_source_candidates'),0)

    async def test_plain_html_without_materialization_cannot_fill_page_role(self):
        group,items=await self.group([await self.fixture('page-a')])
        e,lease,body,raw=items[0];body=copy.deepcopy(body);body.pop('web_materialization')
        with self.assertRaises(Conflict):await self.candidate(group,(e,lease,body,raw))
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),0)

    async def test_page_materialization_must_bind_exact_registered_group(self):
        group,items=await self.group([await self.fixture('page-a')])
        e,lease,body,raw=items[0];body=copy.deepcopy(body)
        from urllib.parse import urlparse
        proof=body['web_materialization'];manifest=json.loads(Path(urlparse(proof['group_manifest_uri']).path).read_bytes())
        manifest['group_id']=str(uuid4());encoded=canonical(manifest);path=self.raw_root/(sha(encoded)+'.other-group.json');path.write_bytes(encoded)
        proof['group_manifest_uri']=path.as_uri();proof['group_manifest_sha256']=sha(encoded)
        with self.assertRaisesRegex(Conflict,'sealed group'):await self.candidate(group,(e,lease,body,raw))

    async def test_page_cannot_embed_uncommitted_image_evidence_file(self):
        group,items=await self.group([await self.fixture('page-a')]);await self.add_image(group,items,committed=True)
        e,lease,body,raw=items[0];body=copy.deepcopy(body)
        from urllib.parse import urlparse
        proof=body['web_materialization'];image=proof['images'][0]
        evidence=json.loads(Path(urlparse(image['artifact_uri']).path).read_bytes());evidence['text']='Forged OCR content without native commit'
        encoded=canonical(evidence);path=self.stage_root/(sha(encoded)+'.forged.json');path.write_bytes(encoded)
        image['artifact_uri']=path.as_uri();image['sha256']=sha(encoded)
        base=body['native_source_units']['units'][0]['text']
        forged=base+'\n\n[Grounded image evidence: '+image['canonical_uri']+']\n'+evidence['text']
        proof['text_sha256']=sha(forged.encode());body['documents'][0]['text']=forged
        # Pure artifact validation succeeds; the real committed-stage join must reject.
        from collection_control.web_materialization import verify_materialization
        self.assertEqual(verify_materialization(self.raw_root,proof,proof['source_revision_id'],base),forged)
        with self.assertRaisesRegex(Conflict,'same-group committed native stage'):await self.candidate(group,(e,lease,body,raw))
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.web_source_candidates'),0)

    async def test_candidate_readback_rejects_removed_materialization_marker(self):
        group,items=await self.group([await self.fixture('page-a')]);await self.candidate(group,items[0])
        e,lease,body,raw=items[0];body=copy.deepcopy(body);body.pop('web_materialization');artifact=self.finalized(e,body)
        # Simulate corruption of both SQL digests so the mandatory semantic gate,
        # rather than merely the outer artifact checksum, catches this regression.
        await self.pool.execute('UPDATE public.stage_units SET artifact_digest=$2,artifact_size_bytes=$3 WHERE idempotency_key=$1',e.idempotency_key,artifact.digest,artifact.size_bytes)
        await self.pool.execute("UPDATE collection_control.web_source_candidates SET stage_artifact_sha256=$2,proof=jsonb_set(proof,'{stage_artifact_size_bytes}',to_jsonb($3::bigint)) WHERE stage_key=$1",e.idempotency_key,artifact.digest.removeprefix('sha256:'),artifact.size_bytes)
        with self.assertRaises(Conflict):await self.group_repo.admit_group(group)
        self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations'),0)
