"""Durable source candidates and one immutable publication for a web asset group."""
import asyncio
import hashlib
import json
from datetime import datetime
from pathlib import Path
from uuid import UUID

from .admission_repository import ProvenanceRepository
from .artifacts import canonical,content_digest,load_artifact,record_digest,_unique_object
from .pinned_connection import PinnedPool
from .provenance import checkpoint_records,empty_source_capture,envelope_digest,full_hash,store_snapshot,verified_file
from .repository import Conflict,Fenced,decoded


def _sha(value):return hashlib.sha256(canonical(value)).hexdigest()


def _json(root,uri,sha):
    return json.loads(verified_file(root,uri,full_hash(sha)),object_pairs_hook=_unique_object)


async def register_group(c,*,group_id,collection_id,job_id,manifest_uri,manifest_sha256,members):
    """Caller owns the intake transaction, including bytes, documents and outbox."""
    group_id=UUID(str(group_id));job_id=UUID(str(job_id));manifest_sha256=full_hash(manifest_sha256)
    await c.execute('INSERT INTO collection_control.web_groups(group_id,collection_id,job_id,manifest_uri,manifest_sha256) VALUES($1,$2,$3,$4,$5)',group_id,collection_id,job_id,manifest_uri,manifest_sha256)
    for m in members:
        await c.execute('INSERT INTO collection_control.web_group_members(group_id,document_id,source_revision_id,role,canonical_uri) VALUES($1,$2,$3,$4,$5)',group_id,m['document_id'],m['source_revision_id'],m['role'],m['canonical_uri'])


async def resolve_graph(c,col):
    """Select asset authorization by the same immutable operation as vector reads."""
    if not col.get('current_operation'):return {'schema_version':'web-published-graph.v1','pages':[]}
    row=await c.fetchrow('SELECT * FROM collection_control.web_generation_manifests WHERE operation_id=$1',col['current_operation'])
    if not row:
        required=await c.fetchval('SELECT web_graph_required FROM collection_control.operations WHERE operation_id=$1',col['current_operation'])
        if required:raise Conflict('Required published graph is missing')
        return {'schema_version':'web-published-graph.v1','pages':[]}
    op=await c.fetchrow("SELECT * FROM collection_control.operations WHERE operation_id=$1 AND collection_id=$2 AND status='published'",col['current_operation'],col['collection_id'])
    if not op or col.get('deleted'):raise Conflict('No authorized published web graph')
    await verify_group(c,op)
    return await asyncio.to_thread(_json,row['raw_root'],row['manifest_uri'],row['manifest_sha256'])


def merge_graph(previous,manifest):
    """Pure reference reconciliation; only complete comparable page observations unlink."""
    if manifest.get('schema_version')=='web-page-group.v2':
        from .web_discovery import merge_discovery
        return merge_discovery(previous,manifest)
    if previous.get('schema_version')=='web-published-graph.v2':raise Conflict('Legacy group cannot replace a text-first discovery graph')
    if manifest.get('schema_version')!='web-asset-group.v1' or manifest.get('coverage')!='complete':raise Conflict('Complete web asset group required')
    old={p['canonical_uri']:p for p in previous['pages']};new={p['canonical_uri']:{**p,'policy':manifest['policy']} for p in manifest['pages']}
    if len(new)!=len(manifest['pages']):raise Conflict('Duplicate page in web asset group')
    # A changed policy cannot establish disappearance of formerly selected assets.
    for uri,page in new.items():
        if uri not in old:continue
        previous_policy=old[uri].get('policy',previous.get('policy'))
        if previous_policy is not None and canonical(previous_policy)!=canonical(page['policy']):
            if {a['canonical_uri'] for a in old[uri]['assets']}-{a['canonical_uri'] for a in page['assets']}:raise Conflict('Changed asset policy cannot authorize unlink')
    incoming={a['canonical_uri']:a for p in new.values() for a in p['assets']}
    for page in new.values():
        for asset in page['assets']:
            if asset['kind'] not in ('image','binary'):raise Conflict('Unknown web asset role')
            other=incoming[asset['canonical_uri']]
            if tuple(asset.get(k) for k in ('kind','raw_sha256','source_revision_id','model_digest','config_digest'))!=tuple(other.get(k) for k in ('kind','raw_sha256','source_revision_id','model_digest','config_digest')):raise Conflict('Conflicting shared asset revision')
    for uri,page in old.items():
        if uri in new:continue
        for asset in page['assets']:
            changed=incoming.get(asset['canonical_uri'])
            if changed and asset['kind']=='image' and tuple(asset.get(k) for k in ('raw_sha256','model_digest','config_digest'))!=tuple(changed.get(k) for k in ('raw_sha256','model_digest','config_digest')):raise Conflict('Changed shared image requires every affected page')
    merged={**old,**new}
    # Binary source updates are explicit global document changes; retain each edge context.
    for uri,page in list(merged.items()):
        copied=dict(page);copied['assets']=[]
        if 'policy' not in copied and 'policy' in previous:copied['policy']=previous['policy']
        for asset in page['assets']:
            replacement=incoming.get(asset['canonical_uri'])
            if replacement and asset['kind']=='binary':
                asset={**asset,**{k:replacement[k] for k in ('document_id','source_revision_id','raw_uri','raw_sha256','media_type','model_digest','config_digest','requested_uri','final_uri','http') if k in replacement}}
            copied['assets'].append(asset)
        merged[uri]=copied
    return {'schema_version':'web-published-graph.v1','pages':[merged[k] for k in sorted(merged)],'policy':manifest['policy']}


async def _verify_page_candidate(c,group,member,checkpoint,raw_root):
    """Every grouped page is a reproducible HTML+image materialization."""
    manifest=await _verify_manifest(c,group,raw_root,verify_assets=False)
    if manifest.get('schema_version')=='web-page-group.v2':
        from .web_children import verify_html_candidate
        return await verify_html_candidate(c,group,member,checkpoint,raw_root,manifest)
    try:
        native=checkpoint['native_source_units'];units=native['units']
        from application.html_disclosures import validate_html_units
        if native['schema_version']!='native-source-units.v1':raise Conflict('Invalid HTML source schema')
        validate_html_units(native)
        proof=checkpoint['web_materialization']
        if proof['group_manifest_uri']!=group['manifest_uri'] or full_hash(proof['group_manifest_sha256'])!=group['manifest_sha256'] or proof['source_revision_id']!=member['source_revision_id']:raise Conflict('Page materialization is not bound to its sealed group')
        manifest=await _verify_manifest(c,group,raw_root,verify_assets=False)
        pages=[p for p in manifest['pages'] if p['document_id']==member['document_id']]
        if len(pages)!=1 or any(pages[0][k]!=member[k] for k in ('canonical_uri','source_revision_id')):raise Conflict('Materialized page identity differs from its group member')
        capture=checkpoint['retained_source_capture']
        if capture['canonical_uri']!=member['canonical_uri'] or capture['source_revision_id']!=member['source_revision_id']:raise Conflict('Page capture differs from sealed identity')
        raw=await asyncio.to_thread(verified_file,raw_root,capture['raw_artifact_uri'],full_hash(capture['raw_sha256']))
        from .html_normalization import verify_normalization
        await verify_normalization(c,raw,native)
        from .web_materialization import verify_materialization
        text=await asyncio.to_thread(verify_materialization,raw_root,proof,member['source_revision_id'],units[0]['text'])
        expected={u['unit_id']:(text if u['unit_id']=='page:1' else u['text']) for u in units if u['text'].strip()}
        actual={d['metadata']['content_metadata']['source_unit_id']:d['text'] for d in checkpoint['documents']}
        if len(actual)!=len(checkpoint['documents']) or actual!=expected:raise Conflict('Page vectors must cover main materialization and all legal disclosures')
        expected={a['document_id']:a for a in pages[0]['assets'] if a['kind']=='image'}
        if {i['document_id'] for i in proof['images']}!=set(expected):raise Conflict('Page image evidence differs from complete group inventory')
        for image in proof['images']:
            stage=await c.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1 FOR SHARE',image['stage_key'])
            if not stage or stage['status']!='committed' or stage['stage']!='extraction' or stage['job_id']!=group['job_id'] or stage['document_id']!=image['document_id'] or stage['artifact_uri']!=image['artifact_uri'] or full_hash(stage['artifact_digest'])!=full_hash(image['sha256']):raise Conflict('Page image proof lacks its exact same-group committed native stage')
            input_envelope=decoded(stage['input_envelope']);asset=expected[image['document_id']]
            if input_envelope.get('document_id')!=image['document_id'] or input_envelope.get('job_id')!=str(group['job_id']) or input_envelope.get('artifact_uri')!=asset['raw_uri']:raise Conflict('Materialized image stage input differs from same-group asset')
            if any(full_hash(input_envelope[k])!=full_hash(asset[k]) for k in ('model_digest','config_digest')):raise Conflict('Materialized image processing contract differs from sealed asset')
    except (KeyError,TypeError,ValueError,AttributeError) as error:
        raise Conflict('Missing or invalid grouped HTML materialization proof') from error


class GroupRepository:
    def __init__(self,legacy,control,collection_id,contract,stage_root,raw_root,snapshot_root):
        if legacy.pool is not control.pool or not isinstance(control,ProvenanceRepository):raise Conflict('Grouped admission requires one shared provenance pool')
        self.legacy=legacy;self.control=control;self.collection_id=collection_id;self.contract=contract
        self.stage_root=Path(stage_root).resolve();self.raw_root=Path(raw_root).resolve();self.snapshot_root=Path(snapshot_root).resolve()

    async def _member(self,c,group_id,document_id):
        group=await c.fetchrow('SELECT * FROM collection_control.web_groups WHERE group_id=$1 FOR UPDATE',UUID(str(group_id)))
        member=await c.fetchrow('SELECT * FROM collection_control.web_group_members WHERE group_id=$1 AND document_id=$2',UUID(str(group_id)),document_id)
        if not group or not member or group['collection_id']!=self.collection_id:raise Conflict('Unknown web group member')
        return group,member

    async def commit_candidate(self,envelope,*,lease_token,artifact,group_id):
        if envelope.stage.value!='embedding' or artifact.metadata.get('payload_format')!='es_docs':raise Conflict('Candidate requires finalized EMBEDDING es_docs')
        stage_sha=full_hash(artifact.digest);signature=envelope_digest(envelope)
        checkpoint=await asyncio.to_thread(_json,self.stage_root,artifact.uri,stage_sha)
        async with self.control.pool.acquire() as c,c.transaction():
            col=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',self.collection_id)
            if not col or col['deleted'] or col['contract_sha256']!=_sha(self.contract):raise Conflict('Collection unavailable or contract changed')
            group,member=await self._member(c,group_id,envelope.document_id)
            if member['role']=='image' or str(group['job_id'])!=envelope.job_id:raise Conflict('Image evidence cannot become an indexed candidate')
            stage=await c.fetchrow('SELECT *,lease_expires_at>clock_timestamp() AS live FROM public.stage_units WHERE idempotency_key=$1 FOR UPDATE',envelope.idempotency_key)
            job=await c.fetchrow('SELECT * FROM public.jobs WHERE job_id=$1 FOR SHARE',UUID(envelope.job_id))
            document=await c.fetchrow('SELECT * FROM public.documents WHERE document_id=$1 FOR SHARE',envelope.document_id)
            if not stage or not job or not document or stage['stage']!='embedding' or stage['document_id']!=envelope.document_id or str(stage['job_id'])!=envelope.job_id or document['job_id']!=job['job_id'] or job['corpus_namespace']!=self.collection_id or job['pipeline_version']!=envelope.pipeline_version or document['source_uri']!=member['canonical_uri'] or envelope_digest(decoded(stage['input_envelope']))!=signature:raise Conflict('Candidate source/stage scope differs')
            if job['status'] in ('cancelled','canceled','deleted','failed','superseded') or document['status'] in ('cancelled','canceled','deleted','failed','superseded'):raise Conflict('Candidate source is inactive')
            if member['role']=='page':await _verify_page_candidate(c,group,member,checkpoint,self.raw_root)
            if checkpoint.get('documents')==[]:
                captured=await asyncio.to_thread(empty_source_capture,checkpoint,stage,document,job,self.raw_root)
                identity=(captured['capture']['canonical_uri'],captured['source_revision_id'],captured['raw_sha256'],captured['capture'].get('previous_source_revision_id'))
                records=[];provenance_sha=_sha(captured)
            else:records,identity,provenance_sha=await asyncio.to_thread(checkpoint_records,checkpoint,envelope,document,job,self.contract,self.raw_root)
            if identity[1]!=member['source_revision_id']:raise Conflict('Candidate source revision differs from sealed membership')
            proof={'policy':'retained-source-stage.v1','source_uri':identity[0],'raw_artifact_uri':document.get('source_uri'),'pipeline_version':envelope.pipeline_version,'config_digest':envelope.config_digest,'model_digest':envelope.model_digest,'actor':'durable-web-group-worker','acl':decoded(job['metadata'])['acl'],'ingestion_run_id':decoded(job['metadata'])['ingestion_run_id'],'source_manifest':decoded(job['metadata'])['source_manifests'][envelope.document_id],'stage_root':str(self.stage_root),'raw_root':str(self.raw_root),'stage_artifact_uri':artifact.uri,'stage_artifact_size_bytes':artifact.size_bytes}
            proof['raw_artifact_uri']=records[0]['metadata']['provenance']['raw_artifact_uri'] if records else captured['capture']['raw_artifact_uri']
            prior=await c.fetchrow('SELECT * FROM collection_control.web_source_candidates WHERE stage_key=$1',envelope.idempotency_key)
            if prior:
                if stage['status']!='committed' or full_hash(stage['artifact_digest'])!=stage_sha or (prior['stage_artifact_sha256'],prior['envelope_sha256'],prior['provenance_sha256'],prior['group_id'])!=(stage_sha,signature,provenance_sha,UUID(str(group_id))):raise Conflict('Candidate replay differs')
                return
            if group['state']!='pending' or stage['status']!='running' or not stage['live'] or str(stage['lease_token'])!=str(lease_token):raise Fenced('Candidate lease/group unavailable')
            for table in ('stage_units','stage_attempts','jobs','documents','outbox_events'):
                if not await c.fetchval('SELECT to_regclass($1)=to_regclass($2)',table,'public.'+table):raise Conflict('Legacy schema is shadowed')
            await type(self.legacy)(PinnedPool(c)).commit_stage(envelope,lease_token=lease_token,artifact=artifact,downstream=())
            await c.execute('INSERT INTO collection_control.web_source_candidates(stage_key,group_id,document_id,source_revision_id,stage_artifact_sha256,envelope_sha256,provenance_sha256,raw_sha256,record_count,proof) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)',envelope.idempotency_key,UUID(str(group_id)),envelope.document_id,identity[1],stage_sha,signature,provenance_sha,identity[2],len(records),json.dumps(proof))

    async def commit_child_candidate(self,envelope,*,lease_token,artifact,acquisition):
        from .web_children import commit_child_candidate
        return await commit_child_candidate(self,envelope,lease_token=lease_token,artifact=artifact,acquisition=acquisition)

    async def admit_child(self,acquisition,*,embed=None,embedding_config_digest=None,caption_stage_key=None):
        from .web_children import admit_child
        return await admit_child(self,acquisition,embed=embed,embedding_config_digest=embedding_config_digest,caption_stage_key=caption_stage_key)

    async def admit_group(self,group_id,*,batch=False):
        async with self.control.pool.acquire() as c,c.transaction():
            col=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',self.collection_id)
            if not col or col['deleted'] or col['contract_sha256']!=_sha(self.contract):raise Conflict('Collection unavailable or changed')
            group=await c.fetchrow('SELECT * FROM collection_control.web_groups WHERE group_id=$1 FOR UPDATE',UUID(str(group_id)))
            if not group or group['collection_id']!=self.collection_id:raise Conflict('Unknown web group')
            if group['operation_id']:
                op=await c.fetchrow('SELECT * FROM collection_control.operations WHERE operation_id=$1',group['operation_id'])
                await verify_group(c,op)
                return dict(op)
            if group['state']!='pending':raise Conflict('Web group inactive')
            if await c.fetchval("SELECT EXISTS(SELECT 1 FROM collection_control.operations WHERE collection_id=$1 AND status='pending')",self.collection_id):raise Conflict('Collection publication pending')
            manifest=await _verify_manifest(c,group,self.raw_root)
            selected_groups=[group];manifests=[manifest]
            if batch and manifest['schema_version']=='web-page-group.v2':
                others=await c.fetch("SELECT g.* FROM collection_control.web_groups g WHERE g.collection_id=$1 AND g.state='pending' AND g.group_id<>$2 AND NOT EXISTS(SELECT 1 FROM collection_control.web_group_members m JOIN public.documents d USING(document_id) JOIN public.jobs j ON j.job_id=d.job_id WHERE m.group_id=g.group_id AND (d.status IN ('cancelled','canceled','deleted','failed','superseded') OR j.status IN ('cancelled','canceled','deleted','failed','superseded'))) AND NOT EXISTS(SELECT 1 FROM collection_control.web_group_members m WHERE m.group_id=g.group_id AND (m.role<>'page' OR NOT EXISTS(SELECT 1 FROM collection_control.web_source_candidates x WHERE x.group_id=m.group_id AND x.document_id=m.document_id))) ORDER BY g.group_id LIMIT 63",self.collection_id,group['group_id'])
                urls={p['canonical_uri'] for p in manifest['pages']}
                for other in others:
                    candidate_manifest=await _verify_manifest(c,other,self.raw_root)
                    candidate_urls={p['canonical_uri'] for p in candidate_manifest['pages']}
                    if candidate_manifest['schema_version']!='web-page-group.v2' or urls & candidate_urls:continue
                    selected_groups.append(other);manifests.append(candidate_manifest);urls.update(candidate_urls)
                manifest={**manifest,'members':[m for x in manifests for m in x['members']],'pages':[p for x in manifests for p in x['pages']]}
            candidates=await c.fetch('SELECT * FROM collection_control.web_source_candidates WHERE group_id=ANY($1::uuid[]) ORDER BY document_id',[g['group_id'] for g in selected_groups])
            expected={m['document_id'] for m in manifest['members'] if m['role']!='image'}
            if {r['document_id'] for r in candidates}!=expected:raise Conflict('Web group awaits all source candidates')
            if manifest['schema_version']=='web-asset-group.v1':await _verify_images(c,group,manifest,self.raw_root,self.stage_root)
            previous=await resolve_graph(c,col);graph=previous
            for original_manifest in manifests:graph=merge_graph(graph,original_manifest)
            base={'contract':self.contract,'records':[]};lineage={};caption_lineage={}
            if col['current_operation']:
                prior=await c.fetchrow('SELECT * FROM collection_control.operations WHERE operation_id=$1',col['current_operation'])
                await self.control._proof(c,prior)
                base=await asyncio.to_thread(load_artifact,self.snapshot_root,prior['artifact_sha256'])
                lineage={r['record_id']:(r['record_sha256'],r['stage_key']) for r in await c.fetch('SELECT * FROM collection_control.record_lineage WHERE operation_id=$1',prior['operation_id'])}
                if await c.fetchval("SELECT to_regclass('collection_control.caption_record_lineage')"):
                    caption_lineage={r['record_id']:(r['record_sha256'],r['stage_key']) for r in await c.fetch('SELECT * FROM collection_control.caption_record_lineage WHERE operation_id=$1',prior['operation_id'])}
            incoming=[];validated=[];replaced=set()
            for candidate in candidates:
                records,identity=await _verify_candidate(c,candidate,self.contract,self.raw_root,self.stage_root)
                uri,revision,raw_sha,previous_revision=identity
                current={r['metadata']['source_revision_id'] for r in base['records'] if r['metadata']['provenance']['canonical_uri']==uri}
                if current and (len(current)!=1 or (revision not in current and previous_revision not in current)):raise Conflict('Changed group source requires exact previous revision')
                replaced.add(uri);incoming.extend(records);validated.append((candidate,records,identity))
            old_binary={a['canonical_uri'] for p in previous['pages'] for a in p['assets'] if a['kind']=='binary'}
            live_binary={a['canonical_uri'] for p in graph['pages'] for a in p['assets'] if a['kind']=='binary'}
            retired=old_binary-live_binary
            # Only binary revisions demonstrably owned by the old web graph can retire.
            owned={(a['canonical_uri'],a['source_revision_id']) for p in previous['pages'] for a in p['assets'] if a['kind']=='binary' and a['canonical_uri'] in retired}
            retained=[r for r in base['records'] if r['metadata']['provenance']['canonical_uri'] not in replaced and (r['metadata']['provenance']['canonical_uri'],r['metadata']['source_revision_id']) not in owned]
            snapshot={'contract':self.contract,'records':sorted(retained+incoming,key=lambda r:r['record_id'])}
            snapshot_sha=await asyncio.to_thread(store_snapshot,self.snapshot_root,snapshot)
            op=await ProvenanceRepository(PinnedPool(c)).prepare(self.collection_id,'web-group:'+str(group['group_id']),'snapshot',col['revision'],snapshot_sha,col['contract_sha256'])
            await c.execute("INSERT INTO collection_control.generation_admissions(operation_id,artifact_sha256,content_sha256,row_count,policy_version) VALUES($1,$2,$3,$4,'retained-source-stage.v1')",op['operation_id'],snapshot_sha,content_digest(snapshot['records']),len(snapshot['records']))
            for candidate,records,identity in validated:
                if not records:continue
                p=records[0]['metadata']['provenance'];observed=datetime.fromisoformat(p['observed_at'].replace('Z','+00:00'))
                await c.execute('INSERT INTO collection_control.source_revisions(collection_id,source_revision_id,canonical_uri,raw_sha256,observed_at,ingestion_run_id) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING',self.collection_id,identity[1],identity[0],identity[2],observed,p['ingestion_run_id'])
                bound=await c.fetchrow('SELECT * FROM collection_control.source_revisions WHERE collection_id=$1 AND source_revision_id=$2',self.collection_id,identity[1])
                if (bound['canonical_uri'],bound['raw_sha256'],bound['observed_at'],bound['ingestion_run_id'])!=(identity[0],identity[2],observed,p['ingestion_run_id']):raise Conflict('Source revision identity reused')
                await c.execute('INSERT INTO collection_control.stage_admissions(stage_key,operation_id,collection_id,document_id,stage_artifact_sha256,snapshot_sha256,provenance_sha256,envelope_sha256,raw_sha256,source_revision_id,record_count,proof) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)',candidate['stage_key'],op['operation_id'],self.collection_id,candidate['document_id'],candidate['stage_artifact_sha256'],snapshot_sha,candidate['provenance_sha256'],candidate['envelope_sha256'],candidate['raw_sha256'],identity[1],len(records),json.dumps(decoded(candidate['proof'])))
                for r in records:
                    await c.execute('INSERT INTO collection_control.source_chunks(collection_id,source_chunk_id,source_revision_id,text_sha256) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING',self.collection_id,r['record_id'],identity[1],r['metadata']['content_sha256'])
                    chunk=await c.fetchrow('SELECT * FROM collection_control.source_chunks WHERE collection_id=$1 AND source_chunk_id=$2',self.collection_id,r['record_id'])
                    if (chunk['source_revision_id'],chunk['text_sha256'])!=(identity[1],r['metadata']['content_sha256']):raise Conflict('Source chunk identity reused')
                    lineage[r['record_id']]=(record_digest(r),candidate['stage_key'])
            for r in snapshot['records']:
                caption=r['record_id'] in caption_lineage
                pair=(caption_lineage if caption else lineage).get(r['record_id'])
                if not pair or pair[0]!=record_digest(r):raise Conflict('Missing admitted group record lineage')
                table='caption_record_lineage' if caption else 'record_lineage'
                await c.execute('INSERT INTO collection_control.'+table+'(operation_id,record_id,record_sha256,stage_key) VALUES($1,$2,$3,$4)',op['operation_id'],r['record_id'],pair[0],pair[1])
            from nv_ingest_durable.deferred_caption import retain
            graph_uri,graph_sha=await asyncio.to_thread(retain,self.raw_root,canonical(graph),'.web-graph.json')
            await c.execute('INSERT INTO collection_control.web_generation_manifests(operation_id,group_id,manifest_uri,manifest_sha256,raw_root,stage_root,contract,inherited_from) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8) ON CONFLICT(operation_id) DO UPDATE SET group_id=EXCLUDED.group_id,manifest_uri=EXCLUDED.manifest_uri,manifest_sha256=EXCLUDED.manifest_sha256,raw_root=EXCLUDED.raw_root,stage_root=EXCLUDED.stage_root,contract=EXCLUDED.contract,inherited_from=EXCLUDED.inherited_from',op['operation_id'],group['group_id'],graph_uri,full_hash(graph_sha),str(self.raw_root),str(self.stage_root),json.dumps(self.contract),col['current_operation'])
            await c.execute('UPDATE collection_control.operations SET web_graph_required=true WHERE operation_id=$1',op['operation_id'])
            op['web_graph_required']=True
            for ordinal,selected_group in enumerate(selected_groups):
                await c.execute("UPDATE collection_control.web_groups SET state='prepared',operation_id=$2 WHERE group_id=$1",selected_group['group_id'],op['operation_id'])
                if batch:await c.execute('INSERT INTO collection_control.web_operation_groups(operation_id,group_id,ordinal) VALUES($1,$2,$3)',op['operation_id'],selected_group['group_id'],ordinal)
            await verify_group(c,op)
            return op


async def _verify_manifest(c,group,raw_root,*,verify_assets=True):
    manifest=await asyncio.to_thread(_json,raw_root,group['manifest_uri'],group['manifest_sha256'])
    if manifest.get('schema_version') not in ('web-asset-group.v1','web-page-group.v2') or manifest.get('coverage')!='complete' or manifest.get('group_id')!=str(group['group_id']) or manifest.get('collection_id')!=group['collection_id'] or manifest.get('job_id')!=str(group['job_id']):raise Conflict('Web group retained manifest differs')
    rows=await c.fetch('SELECT document_id,source_revision_id,role,canonical_uri FROM collection_control.web_group_members WHERE group_id=$1',group['group_id'])
    required=('document_id','source_revision_id','role','canonical_uri')
    expected=sorted(tuple(m[k] for k in required) for m in manifest['members'])
    if expected!=sorted(tuple(r[k] for k in required) for r in rows) or len({m['document_id'] for m in manifest['members']})!=len(expected):raise Conflict('Web group membership differs from retained manifest')
    pages={m['document_id']:m for m in manifest['members'] if m['role']=='page'}
    if {p['document_id'] for p in manifest['pages']}!=set(pages):raise Conflict('Page manifest coverage differs from members')
    if manifest['schema_version']=='web-page-group.v2':
        from .web_discovery import validate_relations
        if any(m['role']!='page' for m in manifest['members']):raise Conflict('Discovery group members must be HTML pages')
        for page in manifest['pages']:
            member=pages[page['document_id']]
            if any(page[k]!=member[k] for k in ('canonical_uri','source_revision_id')):raise Conflict('Discovery page identity differs')
            if page.get('assets'):raise Conflict('Discovery group cannot assert acquired assets')
            validate_relations(page)
        return manifest
    by_document={m['document_id']:m for m in manifest['members']}
    seen=set(pages)
    for page in manifest['pages']:
        member=pages[page['document_id']]
        if any(page[k]!=member[k] for k in ('canonical_uri','source_revision_id')):raise Conflict('Page identity differs from sealed member')
        for asset in page['assets']:
            member=by_document.get(asset['document_id'])
            if not member or member['role']!=asset['kind'] or any(asset[k]!=member[k] for k in ('canonical_uri','source_revision_id')):raise Conflict('Asset identity differs from sealed member')
            if verify_assets:await asyncio.to_thread(verified_file,raw_root,asset['raw_uri'],full_hash(asset['raw_sha256']))
            seen.add(asset['document_id'])
    if seen!=set(by_document):raise Conflict('Unreferenced member in web asset group')
    return manifest


async def _verify_candidate(c,candidate,contract,raw_root,stage_root):
    from nv_ingest_durable.contracts import StageEnvelope
    stage=await c.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1 FOR SHARE',candidate['stage_key'])
    document=await c.fetchrow('SELECT * FROM public.documents WHERE document_id=$1 FOR SHARE',candidate['document_id'])
    job=await c.fetchrow('SELECT * FROM public.jobs WHERE job_id=$1 FOR SHARE',stage['job_id']) if stage else None
    if not stage or not document or not job or stage['status']!='committed' or stage['stage']!='embedding' or stage['document_id']!=candidate['document_id'] or document['job_id']!=stage['job_id'] or full_hash(stage['artifact_digest'])!=candidate['stage_artifact_sha256'] or envelope_digest(decoded(stage['input_envelope']))!=candidate['envelope_sha256']:raise Conflict('Group candidate stage proof changed')
    if job['status'] in ('cancelled','canceled','deleted','failed','superseded') or document['status'] in ('cancelled','canceled','deleted','failed','superseded'):raise Conflict('Group candidate source inactive')
    member=await c.fetchrow('SELECT m.*,g.collection_id,g.job_id AS group_job FROM collection_control.web_group_members m JOIN collection_control.web_groups g USING(group_id) WHERE m.group_id=$1 AND m.document_id=$2',candidate['group_id'],candidate['document_id'])
    input_envelope=decoded(stage['input_envelope'])
    if not member or job['corpus_namespace']!=member['collection_id'] or stage['job_id']!=member['group_job'] or member['canonical_uri']!=document['source_uri'] or member['source_revision_id']!=candidate['source_revision_id'] or input_envelope['pipeline_version']!=job['pipeline_version'] or full_hash(input_envelope['model_digest'])!=contract['embedding_checkpoint_sha256']:raise Conflict('Candidate sealed source scope changed')
    checkpoint=await asyncio.to_thread(_json,stage_root,stage['artifact_uri'],candidate['stage_artifact_sha256'])
    proof=decoded(candidate['proof'])
    if input_envelope['config_digest']!=proof['config_digest'] or input_envelope['model_digest']!=proof['model_digest']:raise Conflict('Candidate processing identity changed')
    if stage['artifact_uri']!=proof['stage_artifact_uri'] or stage['artifact_size_bytes']!=proof['stage_artifact_size_bytes']:raise Conflict('Candidate retained location differs')
    if member['role']=='page':
        group=await c.fetchrow('SELECT * FROM collection_control.web_groups WHERE group_id=$1',candidate['group_id'])
        await _verify_page_candidate(c,group,member,checkpoint,raw_root)
    if checkpoint.get('documents')==[]:
        captured=await asyncio.to_thread(empty_source_capture,checkpoint,stage,document,job,raw_root)
        records=[];identity=(captured['capture']['canonical_uri'],captured['source_revision_id'],captured['raw_sha256'],captured['capture'].get('previous_source_revision_id'));sha=_sha(captured)
    else:
        envelope=StageEnvelope.from_stream_fields(decoded(stage['input_envelope']))
        records,identity,sha=await asyncio.to_thread(checkpoint_records,checkpoint,envelope,document,job,contract,raw_root)
    if sha!=candidate['provenance_sha256'] or len(records)!=candidate['record_count'] or identity[1]!=candidate['source_revision_id'] or identity[2]!=candidate['raw_sha256']:raise Conflict('Candidate retained content differs')
    return records,identity


async def _verify_images(c,group,manifest,raw_root,stage_root):
    assets={a['document_id']:a for p in manifest['pages'] for a in p['assets'] if a['kind']=='image'}
    for document_id,asset in assets.items():
        rows=await c.fetch("SELECT s.*,d.source_digest,d.source_uri,d.status AS document_status,j.status AS job_status,j.corpus_namespace FROM public.stage_units s JOIN public.documents d ON d.document_id=s.document_id AND d.job_id=s.job_id JOIN public.jobs j ON j.job_id=s.job_id WHERE s.document_id=$1 AND s.stage='extraction' AND s.status='committed' FOR SHARE OF s,d,j",document_id)
        if len(rows)!=1:raise Conflict('Web image awaits unique committed native extraction')
        row=rows[0];envelope=decoded(row['input_envelope'])
        if row['document_status'] in ('cancelled','canceled','deleted','failed','superseded') or row['job_status'] in ('cancelled','canceled','deleted','failed','superseded') or row['source_uri']!=asset['canonical_uri'] or full_hash(row['source_digest'])!=full_hash(asset['raw_sha256']) or row['corpus_namespace']!=group['collection_id'] or envelope.get('document_id')!=document_id or envelope.get('job_id')!=str(row['job_id']) or envelope.get('artifact_uri')!=asset['raw_uri']:raise Conflict('Image source scope differs')
        # Historical graph references retain the processing contract selected
        # at capture, independently of later deployment/model configuration.
        model,config=asset.get('model_digest'),asset.get('config_digest')
        if full_hash(envelope.get('model_digest'))!=full_hash(model) or full_hash(envelope.get('config_digest'))!=full_hash(config):raise Conflict('Image processing identity differs')
        evidence=await asyncio.to_thread(_json,stage_root,row['artifact_uri'],row['artifact_digest'])
        if evidence.get('schema_version')!='native-web-image-evidence.v1' or evidence.get('status')!='complete' or evidence.get('document_id')!=document_id or evidence.get('source_revision_id')!=asset['source_revision_id'] or full_hash(evidence.get('raw_sha256'))!=full_hash(asset['raw_sha256']) or evidence.get('raw_uri')!=asset['raw_uri']:raise Conflict('Image extraction evidence differs from exact asset revision')
        native=evidence.get('native',{})
        if full_hash(native.get('model_digest'))!=full_hash(model) or full_hash(native.get('config_digest'))!=full_hash(config) or _sha(native.get('models'))!=full_hash(model) or _sha(native.get('config'))!=full_hash(config):raise Conflict('Native image output processing lineage differs')
        normalized=evidence.get('normalized',{})
        await asyncio.to_thread(verified_file,raw_root,normalized.get('uri'),full_hash(normalized.get('sha256')))
        await asyncio.to_thread(verified_file,raw_root,asset['raw_uri'],full_hash(asset['raw_sha256']))


async def verify_group(c,op):
    """Additional strict gate; never replaces source/caption admission checks."""
    if not await c.fetchval("SELECT to_regclass('collection_control.web_generation_manifests')"):return
    row=await c.fetchrow('SELECT * FROM collection_control.web_generation_manifests WHERE operation_id=$1',op['operation_id'])
    if not row:
        if op.get('web_graph_required') or op.get('idempotency_key','').startswith('web-group:'):raise Conflict('Required web graph proof is missing')
        return
    graph=await asyncio.to_thread(_json,row['raw_root'],row['manifest_uri'],row['manifest_sha256'])
    if graph.get('schema_version') not in ('web-published-graph.v1','web-published-graph.v2'):raise Conflict('Unknown published web graph')
    await _verify_images(c,{'collection_id':op['collection_id']},graph,row['raw_root'],row['stage_root'])
    # Every retained binary and image remains reachable and integrity checked.
    for page in graph['pages']:
        for asset in page['assets']:
            await asyncio.to_thread(verified_file,row['raw_root'],asset['raw_uri'],full_hash(asset['raw_sha256']))
    from .web_children import verify_graph_transition
    await verify_graph_transition(c,op,row,graph)
    checked_documents=set();verified_pages={}
    if row['group_id'] is not None:
        groups=[]
        if await c.fetchval("SELECT to_regclass('collection_control.web_operation_groups')"):
            groups=await c.fetch('SELECT g.* FROM collection_control.web_operation_groups b JOIN collection_control.web_groups g USING(group_id) WHERE b.operation_id=$1 ORDER BY b.ordinal',op['operation_id'])
        if not groups:
            group=await c.fetchrow('SELECT * FROM collection_control.web_groups WHERE group_id=$1',row['group_id'])
            groups=[group]
        if not groups[0] or groups[0]['group_id']!=row['group_id']:raise Conflict('Original batch group binding differs')
        previous={'schema_version':'web-published-graph.v1','pages':[]}
        if row['inherited_from']:
            prior=await c.fetchrow('SELECT * FROM collection_control.web_generation_manifests WHERE operation_id=$1',row['inherited_from'])
            if prior:previous=await asyncio.to_thread(_json,prior['raw_root'],prior['manifest_uri'],prior['manifest_sha256'])
        manifests=[]
        for group in groups:
            if not group or group['operation_id']!=op['operation_id']:raise Conflict('Group publication binding differs')
            manifest=await _verify_manifest(c,group,row['raw_root']);manifests.append(manifest)
            candidates=await c.fetch('SELECT * FROM collection_control.web_source_candidates WHERE group_id=$1',group['group_id'])
            expected={m['document_id'] for m in manifest['members'] if m['role']!='image'}
            if {r['document_id'] for r in candidates}!=expected:raise Conflict('Incomplete group publication candidates')
            for candidate in candidates:
                records,_=await _verify_candidate(c,candidate,decoded(row['contract']),row['raw_root'],row['stage_root'])
                verified_pages[candidate['document_id']]=(candidate,records)
                checked_documents.add(candidate['document_id'])
        reconciled=previous
        for original_manifest in manifests:reconciled=merge_graph(reconciled,original_manifest)
        if canonical(reconciled)!=canonical(graph):raise Conflict('Group graph differs from complete admitted reconciliation')
    for page in graph['pages']:
        if page['document_id'] in checked_documents:continue
        candidate=await c.fetchrow('SELECT a.* FROM collection_control.web_source_candidates a JOIN collection_control.web_groups g USING(group_id) WHERE a.document_id=$1 AND a.source_revision_id=$2 AND g.collection_id=$3',page['document_id'],page['source_revision_id'],op['collection_id'])
        if not candidate:raise Conflict('Published page has no original grouped HTML candidate proof')
        records,_=await _verify_candidate(c,candidate,decoded(row['contract']),row['raw_root'],row['stage_root'])
        verified_pages[candidate['document_id']]=(candidate,records)

    # Original candidate retention alone cannot authorize a changed parent
    # vector. Compare the actual selected snapshot at every writer/read gate.
    snapshot=await asyncio.to_thread(load_artifact,Path(row['raw_root']).parent/'snapshots',op['artifact_sha256'])
    selected={r['record_id']:r for r in snapshot['records']}
    from .web_captions import verify_page_record
    for page in graph['pages']:
        candidate,originals=verified_pages[page['document_id']]
        from application.html_disclosures import main_records
        mains=main_records(originals)
        if len(mains)!=1 or any(r['record_id'] not in selected for r in originals):raise Conflict('Selected page must retain main and every disclosure record')
        matching=[r for r in snapshot['records'] if r['metadata']['provenance'].get('canonical_uri')==page['canonical_uri']]
        if {r['record_id'] for r in matching}!={r['record_id'] for r in originals}:raise Conflict('Selected webpage source-unit set differs')
        for original in originals:
            if original is mains[0]:
                await verify_page_record(c,op,page,candidate,original,selected[original['record_id']],decoded(row['contract']),row['raw_root'],row['stage_root'])
            elif record_digest(selected[original['record_id']])!=record_digest(original):raise Conflict('Disclosure differs from original grounded evidence')



NewGroupRepository=GroupRepository
