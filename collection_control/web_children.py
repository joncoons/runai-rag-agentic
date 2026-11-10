"""Independent child publication proofs for text-first webpage observations.

PostgreSQL admissions authorize publication. Download task state and Redis
notifications never substitute for retained source/stage and selected-parent proof.
"""
import asyncio
import copy
import hashlib
import json
from datetime import datetime
from pathlib import Path
from uuid import UUID

from .artifacts import canonical, content_digest, record_digest, load_artifact, admit
from .provenance import full_hash, verified_file, checkpoint_records, envelope_digest, store_snapshot
from .repository import Conflict, Fenced, decoded
from .pinned_connection import PinnedPool


def sha(value):return hashlib.sha256(canonical(value)).hexdigest()


async def verify_html_candidate(c,group,member,checkpoint,raw_root,manifest):
    """v2 base text is exactly one retained original HTML unit; no image wait."""
    try:
        native=checkpoint['native_source_units'];units=native['units']
        from application.html_disclosures import validate_html_units
        if native['schema_version']!='native-source-units.v1':raise Conflict('Invalid HTML unit schema')
        validate_html_units(native)
        if 'web_materialization' in checkpoint:raise Conflict('Text-first base cannot include a legacy image materialization')
        capture=checkpoint['retained_source_capture']
        if capture['canonical_uri']!=member['canonical_uri'] or capture['source_revision_id']!=member['source_revision_id']:raise Conflict('HTML capture differs from discovery member')
        raw=await asyncio.to_thread(verified_file,raw_root,capture['raw_artifact_uri'],full_hash(capture['raw_sha256']))
        from .html_normalization import verify_normalization
        await verify_normalization(c,raw,native)
        from .provenance import verify_web_acquisition
        acquired=verify_web_acquisition(raw_root,capture['acquisition'],member['canonical_uri'],full_hash(capture['raw_sha256']))
        if acquired['schema_version']!='web-acquisition.v2' or canonical(acquired['policy'])!=canonical(manifest['policy']):raise Conflict('Discovery requires exact text-first acquisition policy')
        pages=[p for p in manifest['pages'] if p['document_id']==member['document_id']]
        if len(pages)!=1:raise Conflict('Discovery page identity missing')
        expected_relations=sorted((a['url'],a['kind'],a['locator'],a.get('alt','')) for a in acquired['assets'])
        actual_relations=sorted((r['canonical_uri'],r['kind'],r['locator'],r['alt']) for r in pages[0]['relations'])
        if expected_relations!=actual_relations:raise Conflict('Discovery inventory differs from complete retained HTML acquisition')
        documents=checkpoint['documents']
        expected={u['unit_id']:u['text'] for u in units if u['text'].strip()}
        actual={d['metadata']['content_metadata']['source_unit_id']:d['text'] for d in documents}
        if len(actual)!=len(documents) or actual!=expected:raise Conflict('Discovery page must retain every main/disclosure unit exactly once')
    except (KeyError,TypeError,ValueError,AttributeError) as error:raise Conflict('Invalid text-first HTML candidate proof') from error


def attach_asset(graph,proof,reference):
    """Apply only exact selected edges; never resurrect a disappeared parent."""
    if graph.get('schema_version')!='web-published-graph.v2':raise Conflict('Child requires a text-first published graph')
    result=copy.deepcopy(graph);asset=proof['asset'];parents=proof['parent_pages']
    if not parents or len({(p['document_id'],p['source_revision_id']) for p in parents})!=len(parents):raise Conflict('Child requires unique exact parent observations')
    affected=set()
    for parent in parents:
        pages=[p for p in result['pages'] if all(p.get(k)==parent.get(k) for k in ('group_id','document_id','source_revision_id','canonical_uri'))]
        if not pages:continue
        if len(pages)!=1:raise Conflict('Child parent observation is ambiguous')
        page=pages[0]
        if not any(r['request_id']==proof['request_id'] and r['canonical_uri']==asset['canonical_uri'] and r['kind']==asset['kind'] for r in page['relations']):raise Conflict('Child relationship is no longer selected')
        selected={**copy.deepcopy(asset),'publication_ref':copy.deepcopy(reference)}
        existing=next((a for a in page['assets'] if (a['canonical_uri'],a['kind'])==(asset['canonical_uri'],asset['kind'])),None)
        if existing and canonical({k:v for k,v in existing.items() if k!='publication_ref'})==canonical(asset):selected=copy.deepcopy(existing)
        page['assets']=[a for a in page['assets'] if (a['canonical_uri'],a['kind'])!=(asset['canonical_uri'],asset['kind'])]+[selected]
        page['assets'].sort(key=lambda a:(a['canonical_uri'],a['kind'],a['source_revision_id']))
        affected.add(page['document_id'])
    if not affected:raise Conflict('Child parent observation is no longer selected')
    if asset['kind']=='binary':
        for page in result['pages']:
            for other in page['assets']:
                if other['kind']=='binary' and other['canonical_uri']==asset['canonical_uri'] and other['source_revision_id']!=asset['source_revision_id']:raise Conflict('Shared binary replacement requires every selected reference')
    return result


async def publication_proof(c,reference,collection_id,raw_root):
    """Validate retained bytes and exact original discovery independently of queue state."""
    from .web_groups import _json,_verify_manifest
    proof=await asyncio.to_thread(_json,raw_root,reference['uri'],reference['sha256'])
    if proof.get('schema_version')!='web-asset-publication.v2' or proof.get('collection_id')!=collection_id:raise Conflict('Unknown or out-of-scope child publication proof')
    acquisition=proof['acquisition'];asset=proof['asset']
    capture=await asyncio.to_thread(_json,raw_root,acquisition['uri'],acquisition['sha256'])
    if capture.get('schema_version')!='deferred-web-asset-acquisition.v1' or capture.get('collection_id')!=collection_id or str(capture.get('request_id'))!=str(proof['request_id']):raise Conflict('Child acquisition scope differs')
    if asset['kind'] not in ('binary','image') or any(capture.get(k)!=asset[k] for k in ('kind','raw_uri','media_type')) or capture.get('requested_uri')!=asset['canonical_uri'] or full_hash(capture['raw_sha256'])!=full_hash(asset['raw_sha256']):raise Conflict('Acquired child bytes/identity differs')
    await asyncio.to_thread(verified_file,raw_root,asset['raw_uri'],full_hash(asset['raw_sha256']))
    child=await c.fetchrow('SELECT d.source_uri,d.source_digest,j.corpus_namespace,j.metadata FROM public.documents d JOIN public.jobs j USING(job_id) WHERE d.document_id=$1 FOR SHARE OF d,j',asset['document_id'])
    if not child or child['corpus_namespace']!=collection_id or child['source_uri']!=asset['canonical_uri'] or full_hash(child['source_digest'])!=full_hash(asset['raw_sha256']):raise Conflict('Child retained document scope differs from acquisition')
    child_acl=decoded(child['metadata']).get('acl')
    for parent in proof['parent_pages']:
        group=await c.fetchrow('SELECT * FROM collection_control.web_groups WHERE group_id=$1',UUID(parent['group_id']))
        if not group or group['collection_id']!=collection_id:raise Conflict('Child discovery group unavailable')
        metadata=await c.fetchval('SELECT metadata FROM public.jobs WHERE job_id=$1 FOR SHARE',group['job_id'])
        if decoded(metadata).get('acl')!=child_acl:raise Conflict('Child acquisition ACL differs from parent discovery')
        manifest=await _verify_manifest(c,group,raw_root,verify_assets=False)
        if manifest['schema_version']!='web-page-group.v2' or sha(manifest['policy'])!=full_hash(capture['policy_sha256']):raise Conflict('Child acquisition policy differs from discovery')
        pages=[p for p in manifest['pages'] if all(p.get(k)==parent[k] for k in ('document_id','source_revision_id','canonical_uri'))]
        if len(pages)!=1 or not any(str(r['request_id'])==str(proof['request_id']) and r['kind']==asset['kind'] and r['canonical_uri']==asset['canonical_uri'] for r in pages[0]['relations']):raise Conflict('Child lacks exact retained discovery relationship')
    return proof


async def source_records(c,stage_key,asset,contract,raw_root,stage_root,*,candidate=None):
    """Reconstruct a binary candidate from the exact committed native stage."""
    from .web_groups import _json
    from nv_ingest_durable.contracts import StageEnvelope
    stage=await c.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1 FOR SHARE',stage_key)
    document=await c.fetchrow('SELECT * FROM public.documents WHERE document_id=$1 FOR SHARE',asset['document_id'])
    job=await c.fetchrow('SELECT * FROM public.jobs WHERE job_id=$1 FOR SHARE',document['job_id']) if document else None
    if not stage or not document or not job or stage['stage']!='embedding' or stage['document_id']!=asset['document_id'] or stage['job_id']!=document['job_id'] or stage['status']!='committed' or document['source_uri']!=asset['canonical_uri'] or full_hash(document['source_digest'])!=full_hash(asset['raw_sha256']):raise Conflict('Child source/stage proof differs')
    if any(x in ('cancelled','canceled','deleted','failed','superseded') for x in (document['status'],job['status'])):raise Conflict('Child source inactive')
    envelope=StageEnvelope.from_stream_fields(decoded(stage['input_envelope']))
    if full_hash(envelope.model_digest)!=contract['embedding_checkpoint_sha256'] or full_hash(asset['model_digest'])!=full_hash(envelope.model_digest) or full_hash(asset['config_digest'])!=full_hash(envelope.config_digest) or envelope.pipeline_version!=job['pipeline_version']:raise Conflict('Child processing contract differs')
    checkpoint=await asyncio.to_thread(_json,stage_root,stage['artifact_uri'],stage['artifact_digest'])
    records,identity,digest=await asyncio.to_thread(checkpoint_records,checkpoint,envelope,document,job,contract,raw_root)
    if identity[:3]!=(asset['canonical_uri'],asset['source_revision_id'],full_hash(asset['raw_sha256'])):raise Conflict('Child candidate revision differs from acquisition')
    if candidate and (candidate['stage_artifact_sha256']!=full_hash(stage['artifact_digest']) or candidate['envelope_sha256']!=envelope_digest(envelope) or candidate['provenance_sha256']!=digest or candidate['record_count']!=len(records)):raise Conflict('Child candidate immutable content differs')
    return records,identity,digest,stage,document,job


async def native_dependencies(c,page,raw_root,stage_root):
    from .web_groups import _verify_images,_json
    images=sorted((a for a in page['assets'] if a['kind']=='image'),key=lambda a:a['document_id'])
    await _verify_images(c,{'collection_id':page['_collection_id']},{'pages':[page]},raw_root,stage_root)
    dependencies=[]
    for asset in images:
        stage=await c.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1 FOR SHARE',asset['extraction_stage_key']) if asset.get('extraction_stage_key') else await c.fetchrow("SELECT * FROM public.stage_units WHERE document_id=$1 AND stage='extraction' AND status='committed' FOR SHARE",asset['document_id'])
        if not stage or stage['stage']!='extraction' or stage['status']!='committed' or stage['document_id']!=asset['document_id']:raise Conflict('Selected native image stage differs')
        body=await asyncio.to_thread(_json,stage_root,stage['artifact_uri'],stage['artifact_digest'])
        dependencies.append({'document_id':asset['document_id'],'source_revision_id':asset['source_revision_id'],'canonical_uri':asset['canonical_uri'],'stage_key':stage['idempotency_key'],'stage_artifact_sha256':full_hash(stage['artifact_digest']),'text':body['text']})
    return dependencies


def enriched_page(parent,images,captions,vector,contract,embedding_config_digest,parent_stage_key,*,policy='retained-web-enrichment.v3'):
    from .web_captions import captioned_text
    text=parent['text']
    if [x['document_id'] for x in images]!=sorted({x['document_id'] for x in images}):raise Conflict('Native dependencies must be unique and deterministic')
    for image in images:
        if image['text'].strip():text+='\n\n[Grounded image evidence: '+image['canonical_uri']+']\n'+image['text']
    if policy not in ('retained-web-enrichment.v2','retained-web-enrichment.v3'):raise Conflict('Unknown enrichment composition policy')
    if captions:text=captioned_text(text,captions,deduplicate=policy=='retained-web-enrichment.v3')
    result=copy.deepcopy(parent);result.update(text=text,vector=list(vector))
    result['metadata'].update(origin='derived',generated_annotation=bool(captions),content_sha256=hashlib.sha256(text.encode()).hexdigest(),web_enrichment_derivation={'policy':policy,'parent_stage_key':parent_stage_key,'parent_record_id':parent['record_id'],'source_revision_id':parent['metadata']['source_revision_id'],'text_sha256':hashlib.sha256(text.encode()).hexdigest(),'parent_record_sha256':record_digest(parent),'images':copy.deepcopy(images),'captions':copy.deepcopy(captions),'embedding_config_digest':full_hash(embedding_config_digest),'embedding_checkpoint_sha256':contract['embedding_checkpoint_sha256']})
    admit({'contract':contract,'records':[result]})
    return result


async def verify_enriched(c,op,page,candidate,parent,selected,contract,raw_root,stage_root):
    from .web_groups import _json
    from .web_captions import caption_dependency
    row=await c.fetchrow('SELECT * FROM collection_control.web_page_derivations WHERE record_sha256=$1',record_digest(selected))
    if not row or row['collection_id']!=op['collection_id'] or row['parent_stage_key']!=candidate['stage_key']:raise Conflict('Selected page lacks immutable enrichment derivation')
    proof=await asyncio.to_thread(_json,raw_root,row['proof_uri'],row['proof_sha256'])
    if proof.get('schema_version')!='web-page-enrichment.v2' or record_digest(proof['record'])!=record_digest(selected):raise Conflict('Selected page enrichment proof differs')
    metadata=selected['metadata']['web_enrichment_derivation']
    if metadata['captions']:
        parent_metadata=await c.fetchval('SELECT j.metadata FROM public.jobs j JOIN collection_control.web_groups g USING(job_id) WHERE g.group_id=$1',candidate['group_id'])
        if decoded(parent_metadata).get('ingestion_options',{}).get('captions_enabled') is not True:raise Conflict('Selected parent observation did not opt in to generated captions')
    scoped={**page,'_collection_id':op['collection_id']}
    images=await native_dependencies(c,scoped,raw_root,stage_root)
    if canonical(images)!=canonical(metadata['images']):raise Conflict('Selected native image dependencies differ')
    by_id={a['document_id']:a for a in page['assets'] if a['kind']=='image'};captions=[]
    for retained in metadata['captions']:
        image=by_id.get(retained['document_id'])
        if not image:raise Conflict('Enriched caption image no longer selected')
        actual=await caption_dependency(c,retained['stage_key'],image,contract,stage_root)
        if canonical(actual)!=canonical(retained):raise Conflict('Selected caption dependency differs')
        captions.append(actual)
    derived=enriched_page(parent,images,captions,selected['vector'],contract,metadata['embedding_config_digest'],candidate['stage_key'],policy=metadata['policy'])
    if canonical(derived)!=canonical(selected):raise Conflict('Selected enrichment is not reproducible')


async def commit_child_candidate(repo,envelope,*,lease_token,artifact,acquisition):
    """Commit a binary source candidate without preparing a collection operation."""
    if envelope.stage.value!='embedding' or artifact.metadata.get('payload_format')!='es_docs':raise Conflict('Child candidate requires finalized EMBEDDING es_docs')
    async with repo.control.pool.acquire() as c,c.transaction():
        col=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',repo.collection_id)
        if not col or col['deleted'] or col['contract_sha256']!=sha(repo.contract):raise Conflict('Child collection unavailable or changed')
        proof=await publication_proof(c,acquisition,repo.collection_id,repo.raw_root);asset=proof['asset']
        if asset['kind']!='binary' or asset['document_id']!=envelope.document_id:raise Conflict('Child candidate belongs to another acquisition')
        for table in ('stage_units','stage_attempts','jobs','documents','outbox_events'):
            if not await c.fetchval('SELECT to_regclass($1)=to_regclass($2)',table,'public.'+table):raise Conflict('Legacy schema is shadowed')
        stage=await c.fetchrow('SELECT *,lease_expires_at>clock_timestamp() AS live FROM public.stage_units WHERE idempotency_key=$1 FOR UPDATE',envelope.idempotency_key)
        if not stage or stage['stage']!='embedding' or stage['document_id']!=envelope.document_id or str(stage['job_id'])!=envelope.job_id or envelope_digest(decoded(stage['input_envelope']))!=envelope_digest(envelope):raise Conflict('Child stage handoff differs')
        prior=await c.fetchrow('SELECT * FROM collection_control.web_child_candidates WHERE stage_key=$1',envelope.idempotency_key)
        if prior:
            if prior['stage_artifact_sha256']!=full_hash(artifact.digest):raise Conflict('Child candidate replay differs')
            original=await publication_proof(c,{'uri':prior['publication_uri'],'sha256':prior['publication_sha256']},repo.collection_id,repo.raw_root)
            if canonical(original['asset'])!=canonical(asset) or canonical(original['acquisition'])!=canonical(proof['acquisition']) or original['request_id']!=proof['request_id']:raise Conflict('Child replay uses another acquisition')
            await source_records(c,envelope.idempotency_key,asset,repo.contract,repo.raw_root,repo.stage_root,candidate=prior)
            return
        if stage['status']!='running' or not stage['live'] or str(stage['lease_token'])!=str(lease_token):raise Fenced('Child candidate lease unavailable')
        await type(repo.legacy)(PinnedPool(c)).commit_stage(envelope,lease_token=lease_token,artifact=artifact,downstream=())
        records,identity,digest,stage,document,job=await source_records(c,envelope.idempotency_key,asset,repo.contract,repo.raw_root,repo.stage_root)
        if job['corpus_namespace']!=repo.collection_id:raise Conflict('Child candidate collection differs')
        metadata=decoded(job['metadata'])
        source_proof={'policy':'retained-source-stage.v1','source_uri':identity[0],'raw_artifact_uri':asset['raw_uri'],'pipeline_version':envelope.pipeline_version,'config_digest':envelope.config_digest,'model_digest':envelope.model_digest,'actor':'durable-web-child-worker','acl':metadata['acl'],'ingestion_run_id':metadata['ingestion_run_id'],'source_manifest':metadata['source_manifests'][asset['document_id']],'stage_root':str(repo.stage_root),'raw_root':str(repo.raw_root),'stage_artifact_uri':artifact.uri,'stage_artifact_size_bytes':artifact.size_bytes}
        await c.execute('INSERT INTO collection_control.web_child_candidates(stage_key,collection_id,document_id,source_revision_id,publication_uri,publication_sha256,stage_artifact_sha256,envelope_sha256,provenance_sha256,record_count,proof) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)',envelope.idempotency_key,repo.collection_id,asset['document_id'],identity[1],acquisition['uri'],full_hash(acquisition['sha256']),full_hash(artifact.digest),envelope_digest(envelope),digest,len(records),json.dumps(source_proof))


async def register_binary(c,repo,op,candidate,records,identity,snapshot_sha):
    p=records[0]['metadata']['provenance'];observed=datetime.fromisoformat(p['observed_at'].replace('Z','+00:00'))
    await c.execute('INSERT INTO collection_control.source_revisions(collection_id,source_revision_id,canonical_uri,raw_sha256,observed_at,ingestion_run_id) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING',repo.collection_id,identity[1],identity[0],identity[2],observed,p['ingestion_run_id'])
    bound=await c.fetchrow('SELECT * FROM collection_control.source_revisions WHERE collection_id=$1 AND source_revision_id=$2',repo.collection_id,identity[1])
    if (bound['canonical_uri'],bound['raw_sha256'],bound['observed_at'],bound['ingestion_run_id'])!=(identity[0],identity[2],observed,p['ingestion_run_id']):raise Conflict('Child source revision reused')
    await c.execute('INSERT INTO collection_control.stage_admissions(stage_key,operation_id,collection_id,document_id,stage_artifact_sha256,snapshot_sha256,provenance_sha256,envelope_sha256,raw_sha256,source_revision_id,record_count,proof) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb) ON CONFLICT(stage_key) DO NOTHING',candidate['stage_key'],op['operation_id'],repo.collection_id,candidate['document_id'],candidate['stage_artifact_sha256'],snapshot_sha,candidate['provenance_sha256'],candidate['envelope_sha256'],identity[2],identity[1],len(records),json.dumps(decoded(candidate['proof'])))
    admitted=await c.fetchrow('SELECT * FROM collection_control.stage_admissions WHERE stage_key=$1',candidate['stage_key'])
    if any(admitted[k]!=candidate[k] for k in ('collection_id','document_id','source_revision_id','stage_artifact_sha256','provenance_sha256','envelope_sha256','record_count')):raise Conflict('Child source admission replay differs')
    for r in records:
        await c.execute('INSERT INTO collection_control.source_chunks(collection_id,source_chunk_id,source_revision_id,text_sha256) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING',repo.collection_id,r['record_id'],identity[1],r['metadata']['content_sha256'])
        bound=await c.fetchrow('SELECT * FROM collection_control.source_chunks WHERE collection_id=$1 AND source_chunk_id=$2',repo.collection_id,r['record_id'])
        if (bound['source_revision_id'],bound['text_sha256'])!=(identity[1],r['metadata']['content_sha256']):raise Conflict('Child source chunk reused')


async def admit_child(repo,acquisition,*,embed=None,embedding_config_digest=None,caption_stage_key=None):
    """Prepare a child operation against exact current parents; models run outside locks.

    A missing initial parent publication raises a retryable Conflict. A caller
    should discard superseded observations by checking selected parent identity;
    no path silently attaches stale work to a newer parent revision.
    """
    from .web_groups import resolve_graph,_verify_candidate
    from .web_captions import caption_dependency
    from .admission_repository import ProvenanceRepository
    from nv_ingest_durable.deferred_caption import retain
    async with repo.control.pool.acquire() as c,c.transaction():
        col=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',repo.collection_id)
        if not col or col['deleted']:return None
        if col['contract_sha256']!=sha(repo.contract):raise Conflict('Child collection contract changed')
        if await c.fetchval("SELECT EXISTS(SELECT 1 FROM collection_control.operations WHERE collection_id=$1 AND status='pending')",repo.collection_id):raise Conflict('Collection publication pending')
        proof=await publication_proof(c,acquisition,repo.collection_id,repo.raw_root);asset=proof['asset']
        if not col['current_operation']:raise Conflict('Child awaits initial parent receipts')
        graph=await resolve_graph(c,col)
        if caption_stage_key:
            if asset['kind']!='image':raise Conflict('Caption enrichment requires a selected image')
            selected_pages=[p for p in graph['pages'] if any(a['kind']=='image' and all(a[k]==asset[k] for k in ('document_id','source_revision_id','raw_sha256','canonical_uri')) for a in p['assets'])]
            if not selected_pages:return None
            parents=[]
            for page in selected_pages:
                metadata=await c.fetchval('SELECT j.metadata FROM public.jobs j JOIN collection_control.web_groups g USING(job_id) WHERE g.group_id=$1',UUID(page['group_id']))
                if decoded(metadata).get('ingestion_options',{}).get('captions_enabled') is True:parents.append({k:page[k] for k in ('group_id','document_id','source_revision_id','canonical_uri')})
            if not parents:return None
            new_graph=copy.deepcopy(graph)
        else:
            parents=[parent for parent in proof['parent_pages'] if any(all(p.get(k)==parent[k] for k in ('group_id','document_id','source_revision_id','canonical_uri')) for p in graph['pages'])]
            if not parents:
                for parent in proof['parent_pages']:
                    group_op=await c.fetchval('SELECT operation_id FROM collection_control.web_groups WHERE group_id=$1',UUID(parent['group_id']))
                    status=await c.fetchval('SELECT status FROM collection_control.operations WHERE operation_id=$1',group_op) if group_op else None
                    if status!='published':raise Conflict('Child awaits initial exact parent receipts')
                return None
            new_graph=attach_asset(graph,proof,acquisition)
        if await c.fetchval("SELECT EXISTS(SELECT 1 FROM collection_control.operations WHERE collection_id=$1 AND status='pending')",repo.collection_id):raise Conflict('Collection publication pending')
        prior=await c.fetchrow('SELECT * FROM collection_control.operations WHERE operation_id=$1',col['current_operation'])
        await repo.control._proof(c,prior)
        base=await asyncio.to_thread(load_artifact,repo.snapshot_root,prior['artifact_sha256'])
        lineage={r['record_id']:(r['record_sha256'],r['stage_key']) for r in await c.fetch('SELECT * FROM collection_control.record_lineage WHERE operation_id=$1',prior['operation_id'])}
        captions_lineage={}
        if await c.fetchval("SELECT to_regclass('collection_control.caption_record_lineage')"):
            captions_lineage={r['record_id']:(r['record_sha256'],r['stage_key']) for r in await c.fetch('SELECT * FROM collection_control.caption_record_lineage WHERE operation_id=$1',prior['operation_id'])}
        updates=[];binary=None
        if asset['kind']=='binary':
            if caption_stage_key:raise Conflict('Web native caption enrichment requires an image')
            rows=await c.fetch('SELECT * FROM collection_control.web_child_candidates WHERE collection_id=$1 AND document_id=$2 AND source_revision_id=$3',repo.collection_id,asset['document_id'],asset['source_revision_id'])
            if len(rows)!=1:raise Conflict('Child awaits a unique committed binary candidate')
            candidate=rows[0]
            original_proof=await publication_proof(c,{'uri':candidate['publication_uri'],'sha256':candidate['publication_sha256']},repo.collection_id,repo.raw_root)
            if canonical(original_proof['asset'])!=canonical(asset) or canonical(original_proof['acquisition'])!=canonical(proof['acquisition']) or original_proof['request_id']!=proof['request_id']:raise Conflict('Binary candidate acquisition binding differs')
            records,identity,_,_,_,job=await source_records(c,candidate['stage_key'],asset,repo.contract,repo.raw_root,repo.stage_root,candidate=candidate)
            if job['corpus_namespace']!=repo.collection_id:raise Conflict('Child source collection differs')
            current={r['metadata']['source_revision_id'] for r in base['records'] if r['metadata']['provenance']['canonical_uri']==identity[0]}
            if current and (len(current)!=1 or (identity[1] not in current and identity[3] not in current)):raise Conflict('Binary child requires exact previous selected revision')
            binary=(candidate,records,identity)
        else:
            if not embed or embedding_config_digest is None:raise Conflict('Native image publication requires parent embedding')
            for parent in parents:
                page=next(p for p in new_graph['pages'] if p['document_id']==parent['document_id'] and p['source_revision_id']==parent['source_revision_id'])
                candidate=await c.fetchrow('SELECT * FROM collection_control.web_source_candidates WHERE group_id=$1 AND document_id=$2 AND source_revision_id=$3',UUID(parent['group_id']),parent['document_id'],parent['source_revision_id'])
                if not candidate:raise Conflict('Child current HTML candidate missing')
                originals,_=await _verify_candidate(c,candidate,repo.contract,repo.raw_root,repo.stage_root)
                from application.html_disclosures import main_records
                originals=main_records(originals)
                if len(originals)!=1:raise Conflict('Child page requires exactly one main-page vector')
                original=originals[0];selected=next((r for r in base['records'] if r['record_id']==original['record_id']),None)
                if not selected:raise Conflict('Child current HTML record missing')
                images=await native_dependencies(c,{**page,'_collection_id':repo.collection_id},repo.raw_root,repo.stage_root)
                existing=selected['metadata'].get('web_enrichment_derivation',selected['metadata'].get('web_caption_derivation',{})).get('captions',[])
                by_id={a['document_id']:a for a in page['assets'] if a['kind']=='image'};captions=[]
                keys=retained_caption_keys(existing,by_id,caption_stage_key)
                for key in keys:
                    cap=await c.fetchrow('SELECT document_id FROM caption_control.assets WHERE stage_key=$1',key)
                    image=by_id.get(cap['document_id']) if cap else None
                    if not image:raise Conflict('Retained caption image is no longer selected')
                    captions.append(await caption_dependency(c,key,image,repo.contract,repo.stage_root))
                updates.append((original,candidate,images,captions,selected))
        base_operation=col['current_operation'];base_revision=col['revision'];contract_sha=col['contract_sha256']
    replacements=[]
    for original,candidate,images,captions,selected in updates:
        # Build text without invoking embedding to detect already selected dependencies.
        preview=enriched_page(original,images,captions,selected['vector'],repo.contract,embedding_config_digest,candidate['stage_key'])
        if canonical(preview)==canonical(selected):continue
        vector=await asyncio.to_thread(embed,preview['text'])
        record=enriched_page(original,images,captions,vector,repo.contract,embedding_config_digest,candidate['stage_key'])
        uri,digest=await asyncio.to_thread(retain,repo.raw_root,canonical({'schema_version':'web-page-enrichment.v2','record':record}),'.web-enrichment.json')
        replacements.append((record,candidate['stage_key'],uri,full_hash(digest)))
    records=list(base['records'])
    if binary:
        candidate,incoming,identity=binary
        records=[r for r in records if r['metadata']['provenance']['canonical_uri']!=identity[0]]+incoming
        for r in incoming:lineage[r['record_id']]=(record_digest(r),candidate['stage_key'])
    by_id={r['record_id']:r for r,_,_,_ in replacements}
    records=sorted((by_id.get(r['record_id'],r) for r in records),key=lambda r:r['record_id'])
    if canonical(records)==canonical(base['records']) and canonical(graph)==canonical(new_graph):return dict(prior)
    snapshot={'contract':repo.contract,'records':records};snapshot_sha=await asyncio.to_thread(store_snapshot,repo.snapshot_root,snapshot)
    graph_uri,graph_sha=await asyncio.to_thread(retain,repo.raw_root,canonical(new_graph),'.web-graph.json')
    async with repo.control.pool.acquire() as c,c.transaction():
        locked=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',repo.collection_id)
        if not locked or locked['deleted']:return None
        if locked['current_operation']!=base_operation or locked['revision']!=base_revision or locked['contract_sha256']!=contract_sha:raise Conflict('Child parent publication changed during embedding')
        op=await ProvenanceRepository(PinnedPool(c)).prepare(repo.collection_id,'web-child:'+full_hash(acquisition['sha256'])+':'+str(caption_stage_key or '')+':'+str(base_operation),'snapshot',base_revision,snapshot_sha,contract_sha)
        await c.execute("INSERT INTO collection_control.generation_admissions(operation_id,artifact_sha256,content_sha256,row_count,policy_version) VALUES($1,$2,$3,$4,'retained-source-stage.v1')",op['operation_id'],snapshot_sha,content_digest(records),len(records))
        if binary:await register_binary(c,repo,op,*binary,snapshot_sha)
        for record,parent_key,uri,digest in replacements:
            lineage[record['record_id']]=(record_digest(record),parent_key)
            await c.execute('INSERT INTO collection_control.web_page_derivations(record_sha256,collection_id,parent_stage_key,proof_uri,proof_sha256) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING',record_digest(record),repo.collection_id,parent_key,uri,digest)
        for record in records:
            caption=record['record_id'] in captions_lineage and record['record_id'] not in by_id
            pair=(captions_lineage if caption else lineage).get(record['record_id'])
            if not pair or pair[0]!=record_digest(record):raise Conflict('Missing child operation per-record lineage')
            table='caption_record_lineage' if caption else 'record_lineage'
            await c.execute('INSERT INTO collection_control.'+table+'(operation_id,record_id,record_sha256,stage_key) VALUES($1,$2,$3,$4)',op['operation_id'],record['record_id'],pair[0],pair[1])
        await c.execute('INSERT INTO collection_control.web_child_publications(operation_id,base_operation_id,publication_uri,publication_sha256,caption_stage_key) VALUES($1,$2,$3,$4,$5)',op['operation_id'],base_operation,acquisition['uri'],full_hash(acquisition['sha256']),caption_stage_key)
        await c.execute('UPDATE collection_control.web_generation_manifests SET group_id=NULL,manifest_uri=$2,manifest_sha256=$3,inherited_from=$4 WHERE operation_id=$1',op['operation_id'],graph_uri,full_hash(graph_sha),base_operation)
        await c.execute('UPDATE collection_control.operations SET web_graph_required=true WHERE operation_id=$1',op['operation_id'])
        op['web_graph_required']=True
        await repo.control._proof(c,op)
        return dict(op)


async def verify_graph_transition(c,op,row,graph):
    """A v2 graph change requires a group reconciliation or an immutable child transition."""
    from .web_groups import _json
    if graph['schema_version']!='web-published-graph.v2':return
    child=None
    if await c.fetchval("SELECT to_regclass('collection_control.web_child_publications')"):
        child=await c.fetchrow('SELECT * FROM collection_control.web_child_publications WHERE operation_id=$1',op['operation_id'])
    if row['group_id'] is None:
        if not row['inherited_from']:raise Conflict('Text-first graph has no immutable parent publication')
        previous=await c.fetchrow('SELECT * FROM collection_control.web_generation_manifests WHERE operation_id=$1',row['inherited_from'])
        if not previous:raise Conflict('Text-first graph parent missing')
        previous_graph=await asyncio.to_thread(_json,previous['raw_root'],previous['manifest_uri'],previous['manifest_sha256'])
        expected=previous_graph
        if child:
            if child['base_operation_id']!=row['inherited_from']:raise Conflict('Child transition base differs')
            reference={'uri':child['publication_uri'],'sha256':child['publication_sha256']}
            proof=await publication_proof(c,reference,op['collection_id'],row['raw_root'])
            if child.get('caption_stage_key'):
                caption=await c.fetchrow('SELECT document_id FROM caption_control.assets WHERE stage_key=$1',child['caption_stage_key'])
                if not caption or caption['document_id']!=proof['asset']['document_id'] or not any(a['kind']=='image' and a['document_id']==caption['document_id'] and a['source_revision_id']==proof['asset']['source_revision_id'] for p in previous_graph['pages'] for a in p['assets']):raise Conflict('Caption transition image is not selected')
            else:expected=attach_asset(previous_graph,proof,reference)
            if proof['asset']['kind']=='image' and not child.get('caption_stage_key'):
                selected_snapshot=await asyncio.to_thread(load_artifact,Path(row['raw_root']).parent/'snapshots',op['artifact_sha256'])
                for parent in proof['parent_pages']:
                    page=next((p for p in graph['pages'] if all(p.get(k)==parent[k] for k in ('group_id','document_id','source_revision_id','canonical_uri'))),None)
                    if not page:continue
                    from application.html_disclosures import main_records
                    selected=main_records([r for r in selected_snapshot['records'] if r['metadata']['provenance']['canonical_uri']==page['canonical_uri']])
                    if len(selected)!=1 or 'web_enrichment_derivation' not in selected[0]['metadata'] or proof['asset']['document_id'] not in {i['document_id'] for i in selected[0]['metadata']['web_enrichment_derivation']['images']}:raise Conflict('Native image transition lacks selected parent enrichment')
            if op['status']!='published':
                current=await c.fetchval('SELECT current_operation FROM collection_control.collections WHERE collection_id=$1',op['collection_id'])
                if current!=child['base_operation_id']:raise Conflict('Child current parent publication fence changed')
        if canonical(expected)!=canonical(graph):raise Conflict('Text-first graph differs from immutable admitted transition')
    snapshot=await asyncio.to_thread(load_artifact,Path(row['raw_root']).parent/'snapshots',op['artifact_sha256'])
    for page in graph['pages']:
        for asset in page['assets']:
            reference=asset.get('publication_ref')
            if reference:
                proof=await publication_proof(c,reference,op['collection_id'],row['raw_root'])
                expected={**proof['asset'],'publication_ref':reference}
                if canonical(asset)!=canonical(expected):raise Conflict('Selected asset differs from immutable acquisition association')
                # An inherited selected revision may survive a new observation
                # of the same URI; the original association remains historical.
                if asset['kind']=='binary':
                    candidates=await c.fetch('SELECT * FROM collection_control.web_child_candidates WHERE collection_id=$1 AND document_id=$2 AND source_revision_id=$3',op['collection_id'],asset['document_id'],asset['source_revision_id'])
                    if len(candidates)!=1:raise Conflict('Selected binary child candidate missing')
                    originals,*_=await source_records(c,candidates[0]['stage_key'],asset,decoded(row['contract']),row['raw_root'],row['stage_root'],candidate=candidates[0])
                    selected=[r for r in snapshot['records'] if r['metadata']['provenance']['canonical_uri']==asset['canonical_uri'] and 'caption_derivation' not in r['metadata']]
                    if {record_digest(r) for r in selected}!={record_digest(r) for r in originals}:raise Conflict('Selected binary vector revision differs from graph asset')
            if asset['kind']=='binary':
                selected=[r for r in snapshot['records'] if r['metadata']['provenance']['canonical_uri']==asset['canonical_uri']]
                if not selected or {r['metadata']['source_revision_id'] for r in selected}!={asset['source_revision_id']}:raise Conflict('Selected binary graph revision absent from snapshot')


def retained_caption_keys(dependencies,selected_images,new_stage_key):
    """A replaced image cannot keep its old generated annotation in the parent."""
    return sorted({x['stage_key'] for x in dependencies if x['document_id'] in selected_images}|({new_stage_key} if new_stage_key else set()))


def caption_needs_native_publication(graph,proof,published_groups):
    """Wait only for live exact observations or parents lacking initial receipts."""
    for parent in proof['parent_pages']:
        pages=[p for p in graph['pages'] if all(p.get(k)==parent[k] for k in ('group_id','document_id','source_revision_id','canonical_uri'))]
        if pages:
            if any(r['request_id']==proof['request_id'] and r['canonical_uri']==proof['asset']['canonical_uri'] and r['kind']=='image' for r in pages[0]['relations']):return True
        elif parent['group_id'] not in published_groups:return True
    return False


async def unselected_caption_pending(c,asset,graph,collection_id,raw_root):
    """Historical source rows are not evidence that an unlinked callback can publish."""
    from .web_groups import _json
    if not await c.fetchval("SELECT to_regclass('application.web_asset_downloads')"):return False
    row=await c.fetchrow('SELECT d.* FROM application.web_asset_downloads d JOIN application.source_revisions s ON s.web_asset_download_id=d.download_id WHERE s.document_id=$1',asset['document_id'])
    if not row or row['state'] in ('failed','cancelled'):return False
    if not row['publication_manifest_uri']:return True
    proof=await asyncio.to_thread(_json,raw_root,row['publication_manifest_uri'],row['publication_manifest_sha256'])
    if proof.get('schema_version')!='web-asset-publication.v2' or proof.get('collection_id')!=collection_id or proof['asset']['document_id']!=asset['document_id']:return False
    published=set()
    for parent in proof['parent_pages']:
        status=await c.fetchval('SELECT o.status FROM collection_control.web_groups g LEFT JOIN collection_control.operations o USING(operation_id) WHERE g.group_id=$1',UUID(parent['group_id']))
        if status=='published':published.add(parent['group_id'])
    return caption_needs_native_publication(graph,proof,published)
