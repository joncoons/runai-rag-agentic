"""Application-scoped audited replay preserving monotonic attempt identities."""
import json
from uuid import UUID, NAMESPACE_URL, uuid5

from collection_control.pinned_connection import PinnedPool
from collection_control.repository import Conflict, decoded
from nv_ingest_durable.contracts import Stage, StageEnvelope
from nv_ingest_durable.operations_postgres import PostgresOperationsRepository


async def _proven_native_replay(connection, stage, audits):
    """Bind a regressed pending unit to the exact native UUID5 replay recipe."""
    try:
        current=StageEnvelope.from_stream_fields(decoded(stage['input_envelope'])).to_dict()
        if (current['job_id']!=str(stage['job_id']) or current['document_id']!=stage['document_id']
                or current['idempotency_key']!=stage['idempotency_key'] or current['stage']!='extraction'):
            return False
        events=await connection.fetch('SELECT target_stream,envelope FROM public.outbox_events WHERE idempotency_key=$1',stage['idempotency_key'])
        envelopes=[(event['target_stream'],StageEnvelope.from_stream_fields(decoded(event['envelope'])).to_dict()) for event in events]
        if not any(stream=='rag.agentic.extraction.pending' and envelope==current for stream,envelope in envelopes):return False
        stable=lambda envelope:{key:value for key,value in envelope.items() if key not in ('message_id','attempt','created_at')}
        for _,original in envelopes:
            if stable(original)!=stable(current):continue
            for audit in audits:
                replay_message=str(uuid5(NAMESPACE_URL,f"replay:{audit['replay_id']}:{original['message_id']}"))
                if replay_message==current['message_id']:return True
        return False
    except (KeyError,TypeError,ValueError,AttributeError):
        return False


async def replay_extraction(pool, job_id, *, requested_by, reason):
    """Replay failed app extraction; audit before restoring operational lifecycle.

    Native counter resets stay inside this outer transaction. Repair of an
    already-regressed pending unit requires its exact audited replay/outbox
    chain and no active lease. Published history and source identities persist.
    """
    if (not isinstance(requested_by,str) or not requested_by.strip()
            or not isinstance(reason,str) or not reason.strip()):
        raise ValueError('requested_by and reason are required for replay')
    identity=UUID(str(job_id))
    async with pool.acquire() as connection, connection.transaction():
        for table in ('jobs','stage_units','stage_attempts','outbox_events','deadletters','replay_requests'):
            if not await connection.fetchval('SELECT to_regclass($1)=to_regclass($2)',table,'public.'+table):raise Conflict('Native replay schema is shadowed or unavailable')
        collection_id=await connection.fetchval('SELECT collection_id FROM application.jobs WHERE job_id=$1',identity)
        if collection_id is None:raise Conflict('Extraction replay requires an application-owned job')
        collection=await connection.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',collection_id)
        owned=await connection.fetchrow('SELECT * FROM application.jobs WHERE job_id=$1 FOR UPDATE',identity)
        if not collection or collection['deleted'] or not owned or owned['collection_id']!=collection_id or owned['status'] in ('cancelled','canceled','deleted'):raise Conflict('Cancelled or deleted application scope cannot be replayed')
        stages=await connection.fetch("""SELECT s.*,
                COALESCE((SELECT MAX(a.attempt_number) FROM public.stage_attempts a WHERE a.idempotency_key=s.idempotency_key),0) AS audit_max
            FROM public.stage_units s WHERE s.job_id=$1 AND s.stage='extraction'
            ORDER BY s.idempotency_key FOR UPDATE OF s""",identity)
        public_job=await connection.fetchrow('SELECT * FROM public.jobs WHERE job_id=$1 FOR UPDATE',identity)
        if not public_job or public_job['corpus_namespace']!=collection_id or public_job['status'] in ('cancelled','canceled','deleted'):raise Conflict('Cancelled, deleted or mismatched durable job cannot be replayed')
        audits=await connection.fetch("SELECT replay_id FROM public.replay_requests WHERE job_id=$1 AND replayed_units>0 AND (stage IS NULL OR stage='extraction')",identity)
        retained=[];documents=set()
        for stage in stages:
            key=stage['idempotency_key'];count=max(stage['attempt_count'],stage['audit_max']);repaired=False
            if (stage['status']=='pending' and stage['attempt_count']<stage['audit_max']
                    and stage['lease_token'] is None and audits
                    and await _proven_native_replay(connection,stage,audits)):
                repaired=await connection.fetchval("""UPDATE public.stage_units SET status='failed',attempt_count=$2
                    WHERE idempotency_key=$1 AND lease_token IS NULL
                      AND (lease_expires_at IS NULL OR lease_expires_at<=clock_timestamp()) RETURNING true""",key,count) or False
            if repaired or stage['status'] in ('failed','deadlettered'):
                await connection.execute('UPDATE public.stage_units SET attempt_count=$2 WHERE idempotency_key=$1',key,count)
                retained.append((key,count))
                if stage['input_envelope'] is not None:documents.add(stage['document_id'])
        document_rows=await connection.fetch('SELECT document_id,status FROM public.documents WHERE document_id=ANY($1::text[]) ORDER BY document_id FOR UPDATE',sorted(documents))
        source_rows=await connection.fetch('SELECT source_revision_id,status FROM application.source_revisions WHERE job_id=$1 AND document_id=ANY($2::text[]) ORDER BY source_revision_id FOR UPDATE',identity,sorted(documents))
        if any(row['status'] in ('cancelled','canceled','deleted','superseded') for row in [*document_rows,*source_rows]):raise Conflict('Cancelled, deleted or superseded source cannot be replayed')
        result=await PostgresOperationsRepository(PinnedPool(connection)).replay_failed(str(identity),stage=Stage.EXTRACTION,target_stream='rag.agentic.extraction.pending',requested_by=requested_by,reason=reason)
        if retained:await connection.executemany('UPDATE public.stage_units SET attempt_count=GREATEST(attempt_count,$2) WHERE idempotency_key=$1',retained)
        if result.replayed_units:
            payload={'replay_id':result.replay_id,'replayed_units':result.replayed_units,'requested_by':requested_by,'reason':reason,
                'previous_status':owned['status'],'previous_stage':owned['stage'],'previous_errors':decoded(owned['errors']),
                'previous_public_status':public_job['status'],'previous_documents':[dict(row) for row in document_rows],
                'previous_source_revisions':[dict(row) for row in source_rows]}
            await connection.execute("INSERT INTO application.events(job_id,event_type,payload) VALUES($1,'extraction_replay_requested',$2::jsonb)",identity,json.dumps(payload))
            await connection.execute("UPDATE application.jobs SET status='queued',stage='queued',errors='[]'::jsonb,updated_at=clock_timestamp() WHERE job_id=$1",identity)
            await connection.execute("UPDATE public.jobs SET status='accepted',updated_at=clock_timestamp() WHERE job_id=$1 AND status='failed'",identity)
            await connection.execute("UPDATE public.documents SET status='accepted',updated_at=clock_timestamp() WHERE job_id=$1 AND document_id=ANY($2::text[]) AND status='failed'",identity,sorted(documents))
            await connection.execute("UPDATE application.source_revisions SET status='queued' WHERE job_id=$1 AND document_id=ANY($2::text[]) AND status='failed'",identity,sorted(documents))
        return result
