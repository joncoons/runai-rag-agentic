import os,json,unittest
from pathlib import Path
from uuid import uuid4,UUID
from datetime import datetime,timedelta,timezone

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'disposable PostgreSQL required')
class PromotionControlTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  import asyncpg
  from tests.collection_control.test_stage_bridge import StageBridgeTests
  p=await asyncpg.connect(os.environ['COLLECTION_TEST_DSN'])
  self.assertEqual(tuple(await p.fetchrow('SELECT current_database(),current_user')),('qualification','qualification'))
  await p.execute('DROP SCHEMA IF EXISTS private_control CASCADE');await p.close()
  self.h=StageBridgeTests();await self.h.asyncSetUp();self.addAsyncCleanup(self.h.asyncTearDown)
  self.pool=self.h.pool
  for folder in ('deployment/collection-control','deployment/ingestion/captioning','deployment/application'):
   for path in sorted(Path(folder).glob('*.sql')):await self.pool.execute(path.read_text())
  await self.pool.execute("INSERT INTO application.collections(collection_id,name) VALUES('collection','Destination') ON CONFLICT DO NOTHING")
  self.owner=uuid4();self.username='writer-'+uuid4().hex;self.session=uuid4().hex;self.policy='p'*64
  await self.pool.execute("INSERT INTO application.local_principals(username,policy_version,collection_write) VALUES($1,$2,'[\"collection\"]')",self.username,self.policy)
  await self.pool.execute("INSERT INTO application.local_sessions(token_sha256,owner_id,principal,policy_version,csrf_token,expires_at) VALUES($1,$2,$3,$4,'csrf',clock_timestamp()+interval '1 day')",self.session,self.owner,self.username,self.policy)
  if 'both_selected' in self._testMethodName:await self.pool.execute("UPDATE collection_control.collections SET milvus_enabled=true WHERE collection_id='collection'")
  self.env,self.lease,self.body,_=await self.h.fixture()
  source=await self.h.commit(self.env,self.lease,self.body);self.op=source['operation_id']
  await self.pool.execute("INSERT INTO application.document_promotions(promotion_id,owner_id,workspace_id,destination,request_key,source_sha256,principal,session_hash,policy_version,copy_expires_at,authorization_expires_at,status,job_id) VALUES($1,$2,$3,'collection',$4,$5,$6,$7,$8,clock_timestamp()+interval '1 hour',clock_timestamp()+interval '1 day','submitted',$9)",uuid4(),self.owner,uuid4(),uuid4().hex,'a'*64,self.username,self.session,self.policy,UUID(self.env.job_id))
  from application.promotion_control import PromotionControl
  self.repo=PromotionControl(self.pool)
 async def test_authorized_promotion_completes_only_with_required_receipts(self):
  from collection_control.artifacts import load_artifact,content_digest
  lease=await self.repo.claim(self.op,ttl_seconds=60);artifact=load_artifact(self.h.snapshots,lease['artifact_sha256'])
  await self.repo.verify_admission(lease,artifact)
  for backend in lease['targets']:await self.repo.receipt(lease,backend,content_digest(artifact['records']),len(artifact['records']))
  await self.repo.publish(lease)
  self.assertEqual(await self.pool.fetchval('SELECT status FROM application.document_promotions WHERE job_id=$1',UUID(self.env.job_id)),'completed')
 async def test_revoked_writer_is_fenced_and_does_not_block_collection(self):
  from collection_control.artifacts import load_artifact
  from application.promotion_control import PromotionDenied
  lease=await self.repo.claim(self.op,ttl_seconds=60);artifact=load_artifact(self.h.snapshots,lease['artifact_sha256'])
  await self.pool.execute("UPDATE application.local_principals SET enabled=false WHERE username=$1",self.username)
  with self.assertRaises(PromotionDenied):await self.repo.verify_admission(lease,artifact)
  self.assertEqual(await self.pool.fetchval('SELECT status FROM collection_control.operations WHERE operation_id=$1',self.op),'cancelled')
  self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM collection_control.operations WHERE status=\'pending\''),0)
  self.assertIsNone(await self.pool.fetchval('SELECT current_operation FROM collection_control.collections WHERE collection_id=\'collection\''))
 async def test_revocation_after_store_receipts_still_prevents_publication(self):
  from collection_control.artifacts import load_artifact,content_digest
  from application.promotion_control import PromotionDenied
  lease=await self.repo.claim(self.op,ttl_seconds=60);artifact=load_artifact(self.h.snapshots,lease['artifact_sha256'])
  await self.repo.verify_admission(lease,artifact)
  for backend in lease['targets']:await self.repo.receipt(lease,backend,content_digest(artifact['records']),len(artifact['records']))
  await self.pool.execute('UPDATE application.local_sessions SET revoked_at=clock_timestamp() WHERE token_sha256=$1',self.session)
  with self.assertRaises(PromotionDenied):await self.repo.publish(lease)
  self.assertEqual(await self.pool.fetchval('SELECT status FROM collection_control.operations WHERE operation_id=$1',self.op),'cancelled')

 async def test_both_selected_store_receipts_required_after_private_copy_expiry(self):
  from collection_control.artifacts import load_artifact,content_digest
  from collection_control.repository import Conflict
  # The private source's lifetime ends after a durable copy was already accepted.
  await self.pool.execute("UPDATE application.document_promotions SET copy_expires_at=clock_timestamp()-interval '1 day'")
  lease=await self.repo.claim(self.op,ttl_seconds=60);artifact=load_artifact(self.h.snapshots,lease['artifact_sha256'])
  self.assertEqual(set(lease['targets']),{'elasticsearch','milvus'})
  await self.repo.verify_admission(lease,artifact)
  await self.repo.receipt(lease,'elasticsearch',content_digest(artifact['records']),len(artifact['records']))
  with self.assertRaises(Conflict):await self.repo.publish(lease)
  self.assertEqual(await self.pool.fetchval('SELECT status FROM application.document_promotions WHERE job_id=$1',UUID(self.env.job_id)),'submitted')
  await self.repo.receipt(lease,'milvus',content_digest(artifact['records']),len(artifact['records']))
  await self.repo.publish(lease)
  self.assertEqual(await self.pool.fetchval('SELECT status FROM application.document_promotions WHERE job_id=$1',UUID(self.env.job_id)),'completed')
 async def test_late_caption_revocation_preserves_already_published_source(self):
  from collection_control.artifacts import load_artifact
  from application.promotion_control import PromotionDenied
  await self.test_authorized_promotion_completes_only_with_required_receipts()
  first=self.op;key=self.env.idempotency_key
  op=await self.repo.prepare('collection','later-caption','snapshot',1,'b'*64,(await self.pool.fetchrow("SELECT * FROM collection_control.collections WHERE collection_id='collection'"))['contract_sha256'])
  self.op=op['operation_id']
  await self.pool.execute("INSERT INTO collection_control.generation_admissions SELECT $1,artifact_sha256,content_sha256,row_count,$2,policy_version FROM collection_control.generation_admissions WHERE operation_id=$2",self.op,first)
  await self.pool.execute('INSERT INTO collection_control.record_lineage SELECT $1,record_id,record_sha256,stage_key FROM collection_control.record_lineage WHERE operation_id=$2',self.op,first)
  await self.pool.execute("INSERT INTO collection_control.caption_admissions(stage_key,collection_id,document_id,parent_source_stage_key,source_revision_id,record_id,record_sha256,stage_artifact_sha256,envelope_sha256,proof) VALUES($1,'collection',$2,$1,'fixture-revision','caption-record',$3,$3,$3,'{}')",key,self.env.document_id,'c'*64)
  await self.pool.execute("INSERT INTO caption_control.assets(stage_key,parent_job_id,document_id,source_digest,source_uri,collection_id,manifest_uri,manifest_sha256,asset_sha256) VALUES($1,$2,$3,'fixture','https://fixture.invalid','collection','file:///fixture',$4,$4)",key,UUID(self.env.job_id),self.env.document_id,'c'*64)
  await self.pool.execute("INSERT INTO collection_control.caption_record_lineage VALUES($1,'caption-record',$2,$3)",self.op,'c'*64,key)
  await self.pool.execute('UPDATE application.local_sessions SET revoked_at=clock_timestamp() WHERE token_sha256=$1',self.session)
  import asyncpg
  with self.assertRaises(asyncpg.CheckViolationError):await self.pool.execute("UPDATE collection_control.operations SET status='published' WHERE operation_id=$1",self.op)
  lease=await self.repo.claim(self.op,ttl_seconds=60)
  with self.assertRaises(PromotionDenied):await self.repo.verify_admission(lease,{})
  self.assertEqual(await self.pool.fetchval("SELECT current_operation FROM collection_control.collections WHERE collection_id='collection'"),first)
  self.assertEqual(await self.pool.fetchval('SELECT status FROM application.document_promotions WHERE job_id=$1',UUID(self.env.job_id)),'completed')
  self.assertNotEqual(await self.pool.fetchval('SELECT status FROM public.documents WHERE document_id=$1',self.env.document_id),'cancelled')
 async def test_retained_promotion_does_not_block_unrelated_future_publication(self):
  from collection_control.artifacts import load_artifact,content_digest
  await self.test_authorized_promotion_completes_only_with_required_receipts()
  await self.pool.execute('UPDATE application.local_sessions SET revoked_at=clock_timestamp() WHERE token_sha256=$1',self.session)
  other=await self.h.fixture('unrelated-next-document');admitted=await self.h.commit(*other[:3])
  lease=await self.repo.claim(admitted['operation_id'],ttl_seconds=60);artifact=load_artifact(self.h.snapshots,lease['artifact_sha256'])
  await self.repo.verify_admission(lease,artifact)
  for backend in lease['targets']:await self.repo.receipt(lease,backend,content_digest(artifact['records']),len(artifact['records']))
  await self.repo.publish(lease)
  self.assertEqual(await self.pool.fetchval('SELECT status FROM collection_control.operations WHERE operation_id=$1',lease['operation_id']),'published')
