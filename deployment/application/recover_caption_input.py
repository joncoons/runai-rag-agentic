"""Recover missing caption input from an actual immutable extraction checkpoint.

Defaults to a read-only dry run. --apply replays CaptionManifestRepository's
validated committed-parent path and changes only missing child input_envelope.
No leases, model calls, attempts, artifacts, statuses or outbox events are reset.
Run in an operator-controlled container using the qualified application image.
"""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import sys

import asyncpg
from collection_control.pinned_connection import PinnedPool
from collection_control.provenance import verified_file,full_hash
from nv_ingest_durable.caption_repository import CaptionManifestRepository,processing_identity
from nv_ingest_durable.contracts import StageEnvelope,CommittedArtifact
from nv_ingest_durable.postgres import PostgresManifestRepository
from nv_ingest_durable.streams import stream_name


def decoded(value):return json.loads(value) if isinstance(value,str) else value

def fingerprint(rows,*,exclude_input=False):
    values=[]
    for row in rows:
        value=dict(row)
        if exclude_input:value.pop('input_envelope',None)
        values.append(value)
    return hashlib.sha256(json.dumps(values,default=str,sort_keys=True,separators=(',',':')).encode()).hexdigest()

async def run(args):
    os.environ.setdefault('INGEST_STREAM_PREFIX','rag.agentic')
    root=Path(os.environ['ARTIFACT_ROOT'])/'stages'
    pool=await asyncpg.create_pool(os.environ['POSTGRES_DSN'],min_size=1,max_size=1)
    try:
        async with pool.acquire() as c,c.transaction(readonly=not args.apply):
            asset=await c.fetchrow('SELECT a.* FROM caption_control.assets a JOIN application.jobs j ON j.job_id=a.parent_job_id WHERE a.stage_key=$1',args.caption_stage_key)
            if not asset:raise ValueError('Caption is outside registered application scope')
            manifest=json.loads(verified_file(root,asset['manifest_uri'],full_hash(asset['manifest_sha256'])))
            lock=' FOR UPDATE' if args.apply else ''
            parent=await c.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1'+lock,manifest['parent_stage_key'])
            if not parent or parent['stage']!='extraction' or parent['status']!='committed' or parent['job_id']!=asset['parent_job_id'] or parent['document_id']!=asset['document_id'] or parent['input_envelope'] is None:raise ValueError('Exact committed extraction input/artifact scope required')
            raw=verified_file(root,parent['artifact_uri'],full_hash(parent['artifact_digest']),parent['artifact_size_bytes']);checkpoint=json.loads(raw)
            jobs=checkpoint.get('caption_jobs',[]);keys=[j['envelope']['idempotency_key'] for j in jobs]
            if args.caption_stage_key not in keys or len(set(keys))!=len(keys):raise ValueError('Committed extraction does not uniquely bind requested caption')
            # Recovery is restricted to existing associations/stages, so replay
            # cannot create fresh pending model work for a lost child record.
            assets=await c.fetch('SELECT * FROM caption_control.assets WHERE stage_key=ANY($1::text[]) ORDER BY stage_key'+lock,keys)
            before=await c.fetch('SELECT * FROM public.stage_units WHERE idempotency_key=ANY($1::text[]) ORDER BY idempotency_key'+lock,keys)
            if len(assets)!=len(keys) or len(before)!=len(keys):raise ValueError('Recovery requires existing child stages and retained assets')
            by_key={j['envelope']['idempotency_key']:j for j in jobs}
            source=await c.fetchrow('SELECT d.source_digest,d.source_uri,d.status AS document_status,j.corpus_namespace,j.status AS job_status,j.metadata,c.deleted FROM public.documents d JOIN public.jobs j ON j.job_id=d.job_id JOIN collection_control.collections c ON c.collection_id=j.corpus_namespace WHERE d.document_id=$1 AND j.job_id=$2',asset['document_id'],asset['parent_job_id'])
            if not source or source['deleted'] or source['document_status'] in ('cancelled','canceled','deleted','failed','superseded') or source['job_status'] in ('cancelled','canceled','deleted','failed','superseded'):raise ValueError('Source is no longer eligible for caption recovery')
            metadata=decoded(source['metadata'])
            if metadata.get('ingestion_options',{}).get('captions_enabled') is not True:raise ValueError('Source caption opt-in is no longer valid')
            original=StageEnvelope.from_stream_fields(decoded(parent['input_envelope']))
            assets_by_key={a['stage_key']:a for a in assets}
            missing=[]
            for row in before:
                child=StageEnvelope.from_stream_fields(by_key[row['idempotency_key']]['envelope'])
                association=by_key[row['idempotency_key']];bound=assets_by_key[row['idempotency_key']]
                retained=json.loads(verified_file(root,association['manifest_uri'],full_hash(association['manifest_sha256'])))
                verified_file(root,retained['asset_uri'],full_hash(association['asset_sha256']))
                scope={'acl':metadata.get('acl',[]),'ingestion_run_id':metadata.get('ingestion_run_id')}
                expected={'parent_job_id':parent['job_id'],'document_id':parent['document_id'],'source_digest':source['source_digest'],'source_uri':source['source_uri'],'collection_id':source['corpus_namespace'],'manifest_uri':association['manifest_uri'],'manifest_sha256':association['manifest_sha256'],'asset_sha256':association['asset_sha256']}
                if any(bound[key]!=value for key,value in expected.items()) or decoded(bound['source_metadata'])!=scope:raise ValueError('Retained source/asset binding differs from committed extraction')
                config_digest='sha256:'+hashlib.sha256(json.dumps(retained['caption_config'],sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
                if child.artifact_uri!=association['manifest_uri'] or child.config_digest!=config_digest or child.model_digest!=retained['model_digest'] or retained['asset_sha256']!=association['asset_sha256'] or retained['parent_stage_key']!=parent['idempotency_key'] or retained['parent_job_id']!=str(parent['job_id']) or retained['document_id']!=parent['document_id'] or retained['pipeline_version']!=original.pipeline_version or retained['parent_config_digest']!=original.config_digest or retained['parent_model_digest']!=original.model_digest:raise ValueError('Retained child processing/manifest identity differs')
                if row['stage']!='caption' or str(row['job_id'])!=child.job_id or row['document_id']!=child.document_id:raise ValueError('Child stage scope differs from retained checkpoint')
                if row['input_envelope'] is None:missing.append(row['idempotency_key'])
                elif processing_identity(row['input_envelope'])!=processing_identity(child):raise ValueError('Existing child computation differs from retained checkpoint')
            outbox_before=await c.fetch('SELECT * FROM public.outbox_events WHERE idempotency_key=ANY($1::text[]) ORDER BY event_id',keys+[parent['idempotency_key']])
            attempts_before=await c.fetch('SELECT * FROM public.stage_attempts WHERE idempotency_key=ANY($1::text[]) ORDER BY attempt_id',keys+[parent['idempotency_key']])
            if args.apply:
                envelope=StageEnvelope.from_stream_fields(decoded(parent['input_envelope']))
                artifact=CommittedArtifact(uri=parent['artifact_uri'],digest=parent['artifact_digest'],size_bytes=parent['artifact_size_bytes'],metadata=decoded(parent['artifact_metadata']))
                downstream=tuple((stream_name('caption.pending'),StageEnvelope.from_stream_fields(j['envelope'])) for j in jobs)
                repository=CaptionManifestRepository(PostgresManifestRepository(PinnedPool(c)),root)
                await repository.commit_stage(envelope,lease_token=None,artifact=artifact,downstream=downstream)
                after=await c.fetch('SELECT * FROM public.stage_units WHERE idempotency_key=ANY($1::text[]) ORDER BY idempotency_key',keys)
                outbox_after=await c.fetch('SELECT * FROM public.outbox_events WHERE idempotency_key=ANY($1::text[]) ORDER BY event_id',keys+[parent['idempotency_key']])
                attempts_after=await c.fetch('SELECT * FROM public.stage_attempts WHERE idempotency_key=ANY($1::text[]) ORDER BY attempt_id',keys+[parent['idempotency_key']])
                if fingerprint(before,exclude_input=True)!=fingerprint(after,exclude_input=True) or fingerprint(outbox_before)!=fingerprint(outbox_after) or fingerprint(attempts_before)!=fingerprint(attempts_after):raise ValueError('Recovery changed stage/attempt/outbox state beyond missing input')
                for row in after:
                    if processing_identity(row['input_envelope'])!=processing_identity(by_key[row['idempotency_key']]['envelope']):raise ValueError('Recovered child input does not match retained checkpoint')
            return {'status':'applied' if args.apply else 'dry_run','parent_stage_key':parent['idempotency_key'],'caption_stage_key':args.caption_stage_key,'bound_child_count':len(keys),'missing_input_count':len(missing),'recovered_input_count':len(missing) if args.apply else 0,'attempts_artifacts_statuses_outbox_preserved':True,'model_calls':0,'source':'committed-extraction-checkpoint.caption_jobs'}
    finally:await pool.close()

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--caption-stage-key',required=True);parser.add_argument('--apply',action='store_true');args=parser.parse_args()
    try:print(json.dumps(asyncio.run(run(args))))
    except Exception as error:
        print(json.dumps({'status':'failed','error_type':type(error).__name__,'reason':str(error) if isinstance(error,ValueError) else 'Recovery validation failed; inspect privately'}));sys.exit(1)
