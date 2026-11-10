"""Atomic local durable EMBEDDING commit and provenance-admitted domain intent.

Local legacy source supplies the commit implementation; it is not copied here.
One pinned connection makes its nested transaction a savepoint of our outer one.
"""
import asyncio
import hashlib
import json
from pathlib import Path
from datetime import datetime
from uuid import UUID

from .admission_repository import ProvenanceRepository
from .artifacts import canonical,content_digest,load_artifact,record_digest,_unique_object
from .pinned_connection import PinnedPool
from .provenance import checkpoint_records,envelope_digest,full_hash,store_snapshot,verified_file
from .repository import Conflict,Fenced,decoded


class StageCollectionRepository:
    def __init__(self,legacy,control,collection_id,contract,stage_root,raw_root,snapshot_root):
        if legacy.pool is not control.pool or not isinstance(control,ProvenanceRepository):raise Conflict('Bridge requires one shared pool and provenance-aware control repository')
        self.legacy=legacy;self.control=control;self.collection_id=collection_id;self.contract=contract
        self.stage_root=Path(stage_root);self.raw_root=Path(raw_root);self.snapshot_root=Path(snapshot_root)

    def __getattr__(self,name):
        return getattr(self.legacy,name)

    async def commit_stage(self,envelope,*,lease_token,artifact,downstream):
        if envelope.stage.value!='embedding':
            return await self.legacy.commit_stage(envelope,lease_token=lease_token,artifact=artifact,downstream=downstream)
        if any(e.stage.value!='index' for _,e in downstream):raise Conflict('Unexpected downstream stage on bridged embedding path')
        if artifact.metadata.get('payload_format')!='es_docs':raise Conflict('Finalized es_docs handoff required')
        stage_sha=full_hash(artifact.digest)
        raw=await asyncio.to_thread(verified_file,self.stage_root,artifact.uri,stage_sha,artifact.size_bytes)
        try:checkpoint=json.loads(raw,object_pairs_hook=_unique_object)
        except (ValueError,UnicodeError) as e:raise Conflict('Invalid finalized checkpoint JSON') from e
        signature=envelope_digest(envelope)
        async with self.control.pool.acquire() as c,c.transaction():
            # Legacy code uses unqualified table names. Reject shadowing rather
            # than change shared search_path or bind a different legacy schema.
            for table in ('stage_units','stage_attempts','jobs','documents','outbox_events'):
                if not await c.fetchval('SELECT to_regclass($1)=to_regclass($2)',table,'public.'+table):raise Conflict('Legacy schema is shadowed')
            col=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',self.collection_id)
            if not col or col['deleted'] or col['contract_sha256']!=hashlib.sha256(canonical(self.contract)).hexdigest():raise Conflict('Collection unavailable or contract changed')
            stage=await c.fetchrow('SELECT *,lease_expires_at>clock_timestamp() AS live FROM public.stage_units WHERE idempotency_key=$1 FOR UPDATE',envelope.idempotency_key)
            if not stage or str(stage['job_id'])!=envelope.job_id or stage['document_id']!=envelope.document_id or stage['stage']!='embedding':raise Conflict('Stage identity does not match handoff')
            stored_input=decoded(stage.get('input_envelope'))
            try:
                if not stored_input or envelope_digest(stored_input)!=signature:raise Conflict('Persisted input processing contract differs from worker handoff')
            except (KeyError,TypeError,AttributeError) as e:raise Conflict('Missing persisted input envelope') from e
            job=await c.fetchrow('SELECT * FROM public.jobs WHERE job_id=$1 FOR SHARE',UUID(envelope.job_id))
            document=await c.fetchrow('SELECT * FROM public.documents WHERE document_id=$1 FOR SHARE',envelope.document_id)
            if not job or not document or job['status'] in ('cancelled','canceled','deleted','failed','superseded') or document['status'] in ('cancelled','canceled','deleted','failed','superseded') or document['job_id']!=stage['job_id'] or job['corpus_namespace']!=self.collection_id or job['pipeline_version']!=envelope.pipeline_version:raise Conflict('Job/document/collection scope mismatch')
            if checkpoint.get('documents')==[]:
                from .provenance import empty_source_capture
                captured=await asyncio.to_thread(empty_source_capture,checkpoint,stage,document,job,self.raw_root)
                capture=captured['capture'];observed=datetime.fromisoformat(capture['observed_at'].replace('Z','+00:00'))
                await c.execute('INSERT INTO collection_control.source_revisions(collection_id,source_revision_id,canonical_uri,raw_sha256,observed_at,ingestion_run_id) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING',self.collection_id,captured['source_revision_id'],capture['canonical_uri'],captured['raw_sha256'],observed,capture['ingestion_run_id'])
                bound=await c.fetchrow('SELECT * FROM collection_control.source_revisions WHERE collection_id=$1 AND source_revision_id=$2',self.collection_id,captured['source_revision_id'])
                if (bound['canonical_uri'],bound['raw_sha256'],bound['observed_at'],bound['ingestion_run_id'])!=(capture['canonical_uri'],captured['raw_sha256'],observed,capture['ingestion_run_id']):raise Conflict('Empty source revision identity was reused')
                if stage['status']=='committed':
                    if full_hash(stage['artifact_digest'])!=stage_sha:raise Conflict('Empty source replay differs')
                    return
                if stage['status']!='running' or not stage['live'] or str(stage['lease_token'])!=str(lease_token):raise Fenced('Empty source stage lease expired')
                # This commits complete extraction/capture evidence only. It
                # creates no empty generation and no fabricated grounded row.
                await type(self.legacy)(PinnedPool(c)).commit_stage(envelope,lease_token=lease_token,artifact=artifact,downstream=())
                return
            records,identity,provenance_sha=await asyncio.to_thread(checkpoint_records,checkpoint,envelope,document,job,self.contract,self.raw_root)
            prior=await c.fetchrow('SELECT * FROM collection_control.stage_admissions WHERE stage_key=$1',envelope.idempotency_key)
            if stage['status']=='committed':
                if not prior or prior['collection_id']!=self.collection_id or prior['stage_artifact_sha256']!=stage_sha or full_hash(stage['artifact_digest'])!=stage_sha or prior['envelope_sha256']!=signature or prior['provenance_sha256']!=provenance_sha:raise Conflict('Committed stage replay does not match original admission')
                return
            if prior:raise Conflict('Admission exists without a committed legacy stage')
            if stage['status']!='running' or not stage['live'] or str(stage['lease_token'])!=str(lease_token):raise Fenced('Stage lease expired or lost before handoff')
            base={'contract':self.contract,'records':[]};lineage={};caption_lineage={}
            if col['current_operation']:
                previous=await c.fetchrow('SELECT * FROM collection_control.operations WHERE operation_id=$1',col['current_operation'])
                base=await asyncio.to_thread(load_artifact,self.snapshot_root,previous['artifact_sha256'])
                rows=await c.fetch('SELECT * FROM collection_control.record_lineage WHERE operation_id=$1',previous['operation_id'])
                lineage={r['record_id']:(r['record_sha256'],r['stage_key']) for r in rows}
                if await c.fetchval("SELECT to_regclass('collection_control.caption_record_lineage')"):
                    caption_rows=await c.fetch('SELECT * FROM collection_control.caption_record_lineage WHERE operation_id=$1',previous['operation_id'])
                    caption_lineage={r['record_id']:(r['record_sha256'],r['stage_key']) for r in caption_rows}
                    lineage.update(caption_lineage)
                if len(lineage)!=len(base['records']) or any(lineage.get(r['record_id'],(None,None))[0]!=record_digest(r) for r in base['records']):raise Conflict('Previous snapshot has no complete admitted lineage')
            uri,revision,raw_sha,previous_revision=identity
            provenance=records[0]['metadata']['provenance']
            observed=datetime.fromisoformat(provenance['observed_at'].replace('Z','+00:00'))
            await c.execute('INSERT INTO collection_control.source_revisions(collection_id,source_revision_id,canonical_uri,raw_sha256,observed_at,ingestion_run_id) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING',self.collection_id,revision,uri,raw_sha,observed,provenance['ingestion_run_id'])
            bound=await c.fetchrow('SELECT * FROM collection_control.source_revisions WHERE collection_id=$1 AND source_revision_id=$2',self.collection_id,revision)
            if (bound['canonical_uri'],bound['raw_sha256'],bound['observed_at'],bound['ingestion_run_id'])!=(uri,raw_sha,observed,provenance['ingestion_run_id']):raise Conflict('Upstream source revision identity was reused for another observation')
            for record in records:
                text_sha=record['metadata']['content_sha256']
                await c.execute('INSERT INTO collection_control.source_chunks(collection_id,source_chunk_id,source_revision_id,text_sha256) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING',self.collection_id,record['record_id'],revision,text_sha)
                bound=await c.fetchrow('SELECT * FROM collection_control.source_chunks WHERE collection_id=$1 AND source_chunk_id=$2',self.collection_id,record['record_id'])
                if (bound['source_revision_id'],bound['text_sha256'])!=(revision,text_sha):raise Conflict('Upstream chunk identity was reused for another source/text')
            replaced=[r for r in base['records'] if r['metadata']['provenance']['canonical_uri']==uri]
            revisions={r['metadata']['source_revision_id'] for r in replaced}
            if replaced and (len(revisions)!=1 or (revision not in revisions and previous_revision not in revisions)):
                raise Conflict('Changed source revision requires explicit previous-revision linkage')
            merged=[r for r in base['records'] if r['metadata']['provenance']['canonical_uri']!=uri]+records
            snapshot={'contract':self.contract,'records':sorted(merged,key=lambda r:r['record_id'])}
            snapshot_sha=await asyncio.to_thread(store_snapshot,self.snapshot_root,snapshot)
            # Recheck wall-clock expiry after byte validation/snapshot work.
            if not await c.fetchval('SELECT lease_expires_at>clock_timestamp() FROM public.stage_units WHERE idempotency_key=$1',envelope.idempotency_key):raise Fenced('Stage lease expired during handoff admission')
            pinned=PinnedPool(c)
            # Use the actual local class's commit behavior, not copied SQL. Its
            # transaction nests on this connection; no legacy INDEX is emitted.
            await type(self.legacy)(pinned).commit_stage(envelope,lease_token=lease_token,artifact=artifact,downstream=())
            op=await ProvenanceRepository(pinned).prepare(self.collection_id,'stage:'+envelope.idempotency_key,'snapshot',col['revision'],snapshot_sha,col['contract_sha256'])
            proof={'policy':'retained-source-stage.v1','source_uri':uri,'raw_artifact_uri':records[0]['metadata']['provenance']['raw_artifact_uri'],'pipeline_version':envelope.pipeline_version,'config_digest':envelope.config_digest,'model_digest':envelope.model_digest,'actor':'durable-stage-worker','acl':records[0]['metadata']['acl'],'ingestion_run_id':records[0]['metadata']['provenance']['ingestion_run_id'],'source_manifest':decoded(job['metadata'])['source_manifests'][envelope.document_id]}
            proof.update(stage_root=str(self.stage_root.resolve()),raw_root=str(self.raw_root.resolve()),stage_artifact_uri=artifact.uri,stage_artifact_size_bytes=artifact.size_bytes)
            await c.execute('INSERT INTO collection_control.stage_admissions(stage_key,operation_id,collection_id,document_id,stage_artifact_sha256,snapshot_sha256,provenance_sha256,envelope_sha256,raw_sha256,source_revision_id,record_count,proof) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)',envelope.idempotency_key,op['operation_id'],self.collection_id,envelope.document_id,stage_sha,snapshot_sha,provenance_sha,signature,raw_sha,revision,len(records),json.dumps(proof))
            await c.execute("INSERT INTO collection_control.generation_admissions(operation_id,artifact_sha256,content_sha256,row_count,policy_version) VALUES($1,$2,$3,$4,'retained-source-stage.v1')",op['operation_id'],snapshot_sha,content_digest(snapshot['records']),len(snapshot['records']))
            for record in records:lineage[record['record_id']]=(record_digest(record),envelope.idempotency_key)
            await c.executemany('INSERT INTO collection_control.record_lineage(operation_id,record_id,record_sha256,stage_key) VALUES($1,$2,$3,$4)',[(op['operation_id'],r['record_id'],lineage[r['record_id']][0],lineage[r['record_id']][1]) for r in snapshot['records'] if r['record_id'] not in caption_lineage])
            inherited=[(op['operation_id'],r['record_id'],caption_lineage[r['record_id']][0],caption_lineage[r['record_id']][1]) for r in snapshot['records'] if r['record_id'] in caption_lineage]
            if inherited:
                await c.executemany('INSERT INTO collection_control.caption_record_lineage(operation_id,record_id,record_sha256,stage_key) VALUES($1,$2,$3,$4)',inherited)


class CaptionCollectionRepository:
    """Admit a committed caption as one derivative in a complete snapshot.

    Conflict is retryable by the result consumer, particularly while the source
    generation is pending. Admission never cancels or mutates the parent job.
    """
    def __init__(self,control,collection_id,contract,stage_root,snapshot_root,*,expected_embedding_config_digest=None,raw_root=None):
        if not isinstance(control,ProvenanceRepository):raise Conflict('Caption admission requires provenance-aware control')
        self.control=control;self.collection_id=collection_id;self.contract=contract
        self.stage_root=Path(stage_root).resolve();self.snapshot_root=Path(snapshot_root).resolve()
        self.embedding_config_digest=full_hash(expected_embedding_config_digest) if expected_embedding_config_digest else None
        self.raw_root=Path(raw_root).resolve() if raw_root else None

    async def admit(self,caption_stage_key,result_artifact=None,*,expected_model_digest):
        from .provenance import caption_record
        expected_model_digest=full_hash(expected_model_digest)
        async with self.control.pool.acquire() as c,c.transaction():
            col=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',self.collection_id)
            if not col or col['deleted'] or col['contract_sha256']!=hashlib.sha256(canonical(self.contract)).hexdigest():raise Conflict('Caption collection unavailable or contract changed')
            stage=await c.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1 FOR SHARE',caption_stage_key)
            asset=await c.fetchrow('SELECT * FROM caption_control.assets WHERE stage_key=$1 FOR SHARE',caption_stage_key)
            if not stage or not asset or stage['stage']!='caption' or stage['status']!='committed' or asset['collection_id']!=self.collection_id:raise Conflict('Committed caption and retained asset required')
            envelope=decoded(stage['input_envelope'])
            if not isinstance(envelope,dict):raise Conflict('Caption stage requires persisted processing input')
            if full_hash(envelope['model_digest'])!=expected_model_digest:raise Conflict('Caption model differs from configured model')
            stage_sha=full_hash(stage['artifact_digest'])
            if result_artifact is not None and (result_artifact.uri!=stage['artifact_uri'] or full_hash(result_artifact.digest)!=stage_sha):raise Conflict('Caption notification differs from committed result')
            prior=await c.fetchrow('SELECT o.* FROM collection_control.operations o WHERE collection_id=$1 AND idempotency_key=$2',self.collection_id,'caption:'+hashlib.sha256(caption_stage_key.encode()).hexdigest())
            if prior:
                admitted=await c.fetchrow('SELECT * FROM collection_control.caption_admissions WHERE stage_key=$1',caption_stage_key)
                if not admitted or admitted['stage_artifact_sha256']!=stage_sha or admitted['envelope_sha256']!=envelope_digest(envelope):raise Conflict('Caption replay differs from admission')
                await ProvenanceRepository(PinnedPool(c))._proof(c,prior)
                return dict(prior)
            if await c.fetchval("SELECT EXISTS(SELECT 1 FROM collection_control.operations WHERE collection_id=$1 AND status='pending')",self.collection_id):raise Conflict('Caption waits for source/current collection mutation publication')
            previous=await c.fetchrow('SELECT * FROM collection_control.operations WHERE operation_id=$1',col['current_operation']) if col['current_operation'] else None
            base={'contract':self.contract,'records':[]};parents=[];capture_proof=None;supersession=None
            if previous:
                await ProvenanceRepository(PinnedPool(c))._proof(c,previous)
                base=await asyncio.to_thread(load_artifact,self.snapshot_root,previous['artifact_sha256'])
                parents=await c.fetch('SELECT DISTINCT a.* FROM collection_control.stage_admissions a JOIN collection_control.record_lineage l USING(stage_key) WHERE l.operation_id=$1 AND a.document_id=$2 AND a.collection_id=$3',previous['operation_id'],asset['document_id'],self.collection_id)
            if len(parents)==1:
                parent=parents[0]
            elif not parents and self.raw_root:
                from .provenance import empty_source_capture
                captures=await c.fetch("SELECT * FROM public.stage_units WHERE document_id=$1 AND job_id=$2 AND stage='embedding' AND status='committed' FOR SHARE",asset['document_id'],asset['parent_job_id'])
                if len(captures)!=1:raise Conflict('Caption awaits a unique committed empty-source capture')
                capture_stage=captures[0]
                capture_checkpoint=json.loads(await asyncio.to_thread(verified_file,self.stage_root,capture_stage['artifact_uri'],full_hash(capture_stage['artifact_digest'])),object_pairs_hook=_unique_object)
                document=await c.fetchrow('SELECT * FROM public.documents WHERE document_id=$1 FOR SHARE',asset['document_id'])
                job=await c.fetchrow('SELECT * FROM public.jobs WHERE job_id=$1 FOR SHARE',asset['parent_job_id'])
                parent=await asyncio.to_thread(empty_source_capture,capture_checkpoint,capture_stage,document,job,self.raw_root)
                capture_proof={'stage_key':capture_stage['idempotency_key'],'artifact_sha256':full_hash(capture_stage['artifact_digest']),'envelope_sha256':envelope_digest(decoded(capture_stage['input_envelope'])),'raw_root':str(self.raw_root)}
                replaced=[r for r in base['records'] if r['metadata']['provenance']['canonical_uri']==asset['source_uri']]
                revisions={r['metadata']['source_revision_id'] for r in replaced}
                if replaced and revisions!={parent['source_revision_id']}:
                    if len(revisions)!=1 or parent['capture'].get('previous_source_revision_id') not in revisions:raise Conflict('Empty source replacement requires explicit source revision transition')
                    supersession={'source_revision_id':next(iter(revisions)),'canonical_uri':asset['source_uri'],'operation_id':str(previous['operation_id'])}
                    base={'contract':self.contract,'records':[r for r in base['records'] if r['metadata']['provenance']['canonical_uri']!=asset['source_uri']]}
            else:raise Conflict('Caption requires current admitted source revision or qualified empty capture')
            if parent['raw_sha256']!=full_hash(asset['source_digest']):raise Conflict('Caption source revision differs from current generation')
            checkpoint=json.loads(await asyncio.to_thread(verified_file,self.stage_root,stage['artifact_uri'],stage_sha),object_pairs_hook=_unique_object)
            manifest=json.loads(await asyncio.to_thread(verified_file,self.stage_root,asset['manifest_uri'],asset['manifest_sha256']),object_pairs_hook=_unique_object)
            if self.embedding_config_digest and full_hash(checkpoint['embedding_config_digest'])!=self.embedding_config_digest:raise Conflict('Caption embedding configuration differs from configured embedding')
            record=await asyncio.to_thread(caption_record,checkpoint,stage,asset,manifest,parent,self.contract,self.stage_root)
            records=[r for r in base['records'] if r['record_id']!=record['record_id']]+[record]
            snapshot={'contract':self.contract,'records':sorted(records,key=lambda r:r['record_id'])}
            snapshot_sha=await asyncio.to_thread(store_snapshot,self.snapshot_root,snapshot)
            op=await ProvenanceRepository(PinnedPool(c)).prepare(self.collection_id,'caption:'+hashlib.sha256(caption_stage_key.encode()).hexdigest(),'snapshot',col['revision'],snapshot_sha,col['contract_sha256'])
            await c.execute('INSERT INTO collection_control.caption_admissions(stage_key,collection_id,parent_source_stage_key,source_revision_id,record_id,record_sha256,stage_artifact_sha256,envelope_sha256,proof,document_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10)',caption_stage_key,self.collection_id,parent['stage_key'] if not capture_proof else None,parent['source_revision_id'],record['record_id'],record_digest(record),stage_sha,envelope_digest(envelope),json.dumps({'stage_root':str(self.stage_root),'contract':self.contract,'capture_only':capture_proof,'supersession':supersession,'admission_operation_id':str(op['operation_id'])}),asset['document_id'])
            await c.execute("INSERT INTO collection_control.generation_admissions(operation_id,artifact_sha256,content_sha256,row_count,policy_version) VALUES($1,$2,$3,$4,'retained-source-stage.v1')",op['operation_id'],snapshot_sha,content_digest(records),len(records))
            await c.execute('INSERT INTO collection_control.record_lineage(operation_id,record_id,record_sha256,stage_key) SELECT $1,record_id,record_sha256,stage_key FROM collection_control.record_lineage WHERE operation_id=$2 AND record_id=ANY($3::text[])',op['operation_id'],previous['operation_id'],[r['record_id'] for r in records]) if previous else None
            await c.execute('INSERT INTO collection_control.caption_record_lineage(operation_id,record_id,record_sha256,stage_key) SELECT $1,record_id,record_sha256,stage_key FROM collection_control.caption_record_lineage WHERE operation_id=$2 AND record_id<>$3 AND record_id=ANY($4::text[])',op['operation_id'],previous['operation_id'],record['record_id'],[r['record_id'] for r in records]) if previous else None
            await c.execute('INSERT INTO collection_control.caption_record_lineage(operation_id,record_id,record_sha256,stage_key) VALUES($1,$2,$3,$4)',op['operation_id'],record['record_id'],record_digest(record),caption_stage_key)
            # Validate current ACL/URL/opt-in and retained extraction parent in
            # the same transaction that creates the full immutable intent.
            await ProvenanceRepository(PinnedPool(c))._proof(c,op)
            return op
