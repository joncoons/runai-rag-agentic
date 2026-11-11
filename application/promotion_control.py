"""Promotion revocation fences normal publication without blocking the corpus."""
from collection_control.admission_repository import ProvenanceRepository
from collection_control.repository import Fenced

PROMOTIONS='SELECT * FROM application.publication_promotions($1)'

class PromotionDenied(Fenced):pass

class PromotionControl(ProvenanceRepository):
 async def _proof(self,c,op):
  if await c.fetchval("SELECT to_regclass('application.document_promotions')"):
   for p in await c.fetch(PROMOTIONS,op['operation_id']):
    await c.fetch('SELECT s.token_sha256 FROM application.local_sessions s JOIN application.local_principals pr ON pr.username=s.principal WHERE s.token_sha256=$1 FOR SHARE OF s,pr',p['session_hash'])
    if not await c.fetchval('SELECT application.promotion_allowed(p) FROM application.document_promotions p WHERE promotion_id=$1',p['promotion_id']):raise PromotionDenied('Promotion authorization revoked')
  return await super()._proof(c,op)
 async def _abort(self,operation):
  async with self.pool.acquire() as c,c.transaction():
   _,op=await self._locked(c,operation)
   if op['status']!='pending':return
   promotions=await c.fetch(PROMOTIONS,op['operation_id'])
   await c.execute("UPDATE collection_control.operations SET status='cancelled',lease_token=NULL,lease_until=NULL WHERE operation_id=$1",op['operation_id'])
   await c.execute("UPDATE collection_control.outbox SET delivered_at=clock_timestamp() WHERE operation_id=$1",op['operation_id'])
   for p in promotions:
    if p['status']=='completed':
     # Only reject newly pending annotations; published source evidence and its
     # completed promotion retain their independent durable ownership.
     await c.execute("UPDATE caption_control.assets SET terminal_reason='promotion_authorization_revoked',updated_at=clock_timestamp() WHERE parent_job_id=$1 AND stage_key IN (SELECT l.stage_key FROM collection_control.caption_record_lineage l WHERE l.operation_id=$2 AND NOT EXISTS(SELECT 1 FROM collection_control.caption_record_lineage old JOIN collection_control.collections col ON col.current_operation=old.operation_id WHERE col.collection_id=$3 AND old.stage_key=l.stage_key AND old.record_sha256=l.record_sha256 AND old.record_id=l.record_id))",p['job_id'],op['operation_id'],op['collection_id'])
     await c.execute("UPDATE application.caption_publications SET status='failed',last_error_type='PromotionDenied',updated_at=clock_timestamp() WHERE operation_id=$1",op['operation_id'])
     continue
    await c.execute("UPDATE application.document_promotions SET status='cancelled',revoked_at=COALESCE(revoked_at,clock_timestamp()) WHERE promotion_id=$1",p['promotion_id'])
    await c.execute("UPDATE public.jobs SET status='cancelled' WHERE job_id=$1",p['job_id'])
    await c.execute("UPDATE public.documents SET status='cancelled' WHERE job_id=$1",p['job_id'])
    await c.execute("UPDATE application.source_revisions SET status='cancelled' WHERE job_id=$1",p['job_id'])
    await c.execute("UPDATE application.jobs SET status='cancelled' WHERE job_id=$1",p['job_id'])
 async def verify_admission(self,lease,artifact):
  try:return await super().verify_admission(lease,artifact)
  except PromotionDenied:
   await self._abort(lease['operation_id']);raise
 async def publish(self,lease):
  try:return await super().publish(lease)
  except Exception as error:
   if isinstance(error,PromotionDenied) or (getattr(error,'sqlstate',None)=='23514' and 'Promotion authorization revoked' in str(error)):
    await self._abort(lease['operation_id']);raise PromotionDenied('Promotion authorization revoked') from None
   raise
