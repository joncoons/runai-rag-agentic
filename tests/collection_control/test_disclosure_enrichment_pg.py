"""Late image enrichment must preserve every selected legal audience record."""
import json,os,unittest
from dataclasses import replace
from uuid import UUID
from tests.collection_control import test_web_children_pg as children
from tests.collection_control import test_web_groups as groups
from application.provenance import enrich_checkpoint
from ingestion.document_formats import inspect_document
from collection_control.artifacts import record_digest,load_artifact

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'requires isolated qualification PostgreSQL')
class DisclosureEnrichmentTests(unittest.IsolatedAsyncioTestCase):
 fixture=children.WebChildrenTests.fixture
 finalized=children.WebChildrenTests.finalized
 publish=children.WebChildrenTests.publish
 group=children.WebChildrenTests.group
 materialize=children.WebChildrenTests.materialize
 candidate=children.WebChildrenTests.candidate
 retain=children.WebChildrenTests.retain
 image_ready=children.WebChildrenTests.image_ready
 committed_caption=children.WebChildrenTests.committed_caption
 asyncSetUp=children.WebChildrenTests.asyncSetUp
 asyncTearDown=children.WebChildrenTests.asyncTearDown

 async def page_group(self,**kwargs):
  group,item,manifest=await children.WebChildrenTests.page_group(self,**kwargs)
  e,lease,body,path=item;p=body['documents'][0]['metadata']['provenance']
  raw=path.read_bytes().replace(b'</main>',b'<section id="disclaimer"><div class="disclaimer-student">Student eligibility condition.</div><div class="disclaimer-new-to-canada">Newcomer eligibility condition.</div></section></main>')
  raw_sha=groups.sha(raw);raw_path=self.raw_root/(raw_sha+'.html');raw_path.write_bytes(raw)
  inspected=inspect_document('page.html',raw);native={'schema_version':'native-source-units.v1',**{k:inspected[k] for k in ('source_kind','transform','units')}}
  acquisition=json.loads(__import__('pathlib').Path(__import__('urllib.parse',fromlist=['urlparse']).urlparse(p['acquisition']['artifact_uri']).path).read_text())
  acquisition['raw_sha256']=raw_sha;ref=self.retain(acquisition)
  job=json.loads(await self.pool.fetchval('SELECT metadata FROM public.jobs WHERE job_id=$1',UUID(e.job_id)))
  revision={'source_id':'source-'+e.document_id,'source_revision_id':p['source_revision_id'],'collection_id':'collection','document_id':e.document_id,'job_id':e.job_id,'canonical_uri':p['canonical_uri'],'raw_uri':raw_path.as_uri(),'raw_sha256':raw_sha,'ingestion_run_id':job['ingestion_run_id'],'filename':'page.html','source_kind':'html','source_units':native,'observed_at':p['observed_at'],'previous_source_revision_id':p.get('previous_source_revision_id'),'web_group_id':group,'acquisition':ref}
  checkpoint={'schema_version':'1.0','payload_format':'es_docs','native_source_units':native,'documents':[{'text':u['text'],'vector':[1.0]+[0.0]*2047,'metadata':{'content_metadata':{'source_unit_id':u['unit_id'],'type':'text'}}} for u in native['units']]}
  enriched,source_manifest=enrich_checkpoint(checkpoint,revision,raw_root=self.raw_root,model_digest='e'*64,config_digest='c'*64,ocr_model_digest='f'*64)
  retained=self.retain(source_manifest);enriched['retained_source_capture']['source_manifest']['uri']=retained['uri'];job['source_manifests'][e.document_id]=retained
  e=replace(e,artifact_uri=raw_path.as_uri())
  await self.pool.execute('UPDATE public.documents SET source_digest=$2 WHERE document_id=$1',e.document_id,'sha256:'+raw_sha)
  await self.pool.execute('UPDATE public.jobs SET metadata=$2::jsonb WHERE job_id=$1',UUID(e.job_id),json.dumps(job))
  await self.pool.execute('UPDATE public.stage_units SET input_envelope=$2::jsonb WHERE idempotency_key=$1',e.idempotency_key,json.dumps(e.to_dict()))
  return group,(e,lease,enriched,raw_path),manifest

 async def test_image_publication_changes_only_main_and_keeps_legal_proof(self):
  base,association,_,_=await self.image_ready()
  before=load_artifact(self.snapshots,base['artifact_sha256'])
  legal={r['record_id']:record_digest(r) for r in before['records'] if r['metadata'].get('content_role')=='legal_disclosure'}
  self.assertEqual(len(legal),2)
  child=await self.group_repo.admit_child(association,embed=lambda text:[0.0,1.0]+[0.0]*2046,embedding_config_digest='c'*64)
  _,after=await self.publish(child['operation_id'])
  self.assertEqual(len(after['records']),3)
  self.assertEqual({r['record_id']:record_digest(r) for r in after['records'] if r['metadata'].get('content_role')=='legal_disclosure'},legal)
  main=next(r for r in after['records'] if r['metadata'].get('content_role')=='page')
  self.assertIn('independent native OCR',main['text'])
  async with self.pool.acquire() as c:
   from collection_control.web_groups import resolve_graph
   await resolve_graph(c,await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1','collection'))

 async def test_generated_caption_preserves_disclosures_and_native_text(self):
  from collection_control.web_captions import WebCaptionRepository
  base,association,queue,downstream=await self.image_ready(captions=True)
  before=load_artifact(self.snapshots,base['artifact_sha256'])
  legal={r['record_id']:record_digest(r) for r in before['records'] if r['metadata'].get('content_role')=='legal_disclosure'}
  embed=lambda text:[0.0,1.0]+[0.0]*2046
  native=await self.group_repo.admit_child(association,embed=embed,embedding_config_digest='c'*64)
  await self.publish(native['operation_id'])
  key,text=await self.committed_caption(queue,downstream)
  bridge=WebCaptionRepository(self.repo,'collection',self.contract,self.stage_root,self.raw_root,self.snapshots)
  op=await bridge.admit(key,embed=embed,embedding_config_digest='c'*64,expected_model_digest='a'*64)
  _,after=await self.publish(op['operation_id'])
  self.assertEqual(len(after['records']),3)
  self.assertEqual({r['record_id']:record_digest(r) for r in after['records'] if r['metadata'].get('content_role')=='legal_disclosure'},legal)
  main=next(r for r in after['records'] if r['metadata'].get('content_role')=='page')
  self.assertIn('independent native OCR',main['text']);self.assertIn(text,main['text'])
