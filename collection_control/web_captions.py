"""Immutable caption derivations replacing the same current webpage record.

Original source-stage lineage stays authoritative for the retained page. This
additional proof authorizes the changed text/vector only after checking every
caption against the exact committed image extraction and complete web graph.
"""
import asyncio
import copy
import hashlib
import json
from pathlib import Path
from uuid import UUID

from .artifacts import canonical, record_digest, content_digest, load_artifact, admit
from .provenance import full_hash, verified_file, caption_record, store_snapshot
from .repository import Conflict, decoded
from .pinned_connection import PinnedPool

POLICY='retained-web-caption-page.v1'

def sha(value):return hashlib.sha256(canonical(value)).hexdigest()

def captioned_text(base,captions):
    if not captions or [x['stage_key'] for x in captions]!=sorted({x['stage_key'] for x in captions}):
        raise Conflict('Web captions require unique deterministic complete dependencies')
    text=base
    for item in captions:
        if not isinstance(item['text'],str) or not item['text'].strip():raise Conflict('Empty web caption dependency')
        for key in ('asset_sha256','model_digest','config_digest','stage_artifact_sha256'):full_hash(item[key])
        text+='\n\n[Machine-generated image caption: '+item['canonical_uri']+']\n'+item['text']
    return text

def derived_page(parent,captions,vector,contract,embedding_config_digest,parent_stage_key):
    result=copy.deepcopy(parent);text=captioned_text(parent['text'],captions)
    text_sha=hashlib.sha256(text.encode()).hexdigest()
    proof={'policy':POLICY,'parent_stage_key':parent_stage_key,'parent_record_id':parent['record_id'],
        'parent_record_sha256':record_digest(parent),'source_revision_id':parent['metadata']['source_revision_id'],
        'text_sha256':text_sha,'embedding_checkpoint_sha256':contract['embedding_checkpoint_sha256'],
        'embedding_config_digest':full_hash(embedding_config_digest),'captions':copy.deepcopy(captions)}
    result.update(text=text,vector=list(vector))
    result['metadata'].update(origin='derived',generated_annotation=True,content_sha256=text_sha,web_caption_derivation=proof)
    admit({'contract':contract,'records':[result]})
    return result

async def caption_dependency(c,stage_key,image,contract,stage_root):
    """Validate a historical committed caption without consulting live model config."""
    stage=await c.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1 FOR SHARE',stage_key)
    asset=await c.fetchrow('SELECT * FROM caption_control.assets WHERE stage_key=$1 FOR SHARE',stage_key)
    if not stage or not asset or stage['status']!='committed' or stage['stage']!='caption' or asset['document_id']!=image['document_id'] or asset['source_uri']!=image['canonical_uri'] or full_hash(asset['source_digest'])!=full_hash(image['raw_sha256']):raise Conflict('Web caption does not belong to current image revision')
    job=await c.fetchrow('SELECT * FROM public.jobs WHERE job_id=$1 FOR SHARE',asset['parent_job_id'])
    document=await c.fetchrow('SELECT * FROM public.documents WHERE document_id=$1 FOR SHARE',asset['document_id'])
    metadata=decoded(job['metadata']) if job else {};scope=decoded(asset['source_metadata'])
    if not document or document['job_id']!=asset['parent_job_id'] or stage['job_id']!=asset['parent_job_id'] or stage['document_id']!=asset['document_id'] or document['source_uri']!=asset['source_uri'] or document['source_digest']!=asset['source_digest'] or job['corpus_namespace']!=asset['collection_id'] or metadata.get('ingestion_options',{}).get('captions_enabled') is not True or any(metadata.get(k)!=scope.get(k) for k in ('acl','ingestion_run_id')):raise Conflict('Web caption retained source scope differs')
    if any(x in ('cancelled','canceled','deleted','failed','superseded') for x in (document['status'],job['status'])):raise Conflict('Web caption source inactive')
    from .web_groups import _json
    manifest=await asyncio.to_thread(_json,stage_root,asset['manifest_uri'],asset['manifest_sha256'])
    extraction=await c.fetchrow('SELECT * FROM public.stage_units WHERE idempotency_key=$1 FOR SHARE',manifest['parent_stage_key'])
    if not extraction or extraction['status']!='committed' or extraction['stage']!='extraction' or extraction['job_id']!=asset['parent_job_id'] or extraction['document_id']!=asset['document_id']:raise Conflict('Web caption extraction parent differs')
    retained=await asyncio.to_thread(_json,stage_root,extraction['artifact_uri'],extraction['artifact_digest'])
    associated=[j for j in retained.get('caption_jobs',[]) if j['stage_key']==stage_key]
    if len(associated)!=1 or any(associated[0][k]!=asset[k] for k in ('manifest_uri','manifest_sha256','asset_sha256')):raise Conflict('Web caption lacks atomic extraction handoff')
    envelope=decoded(extraction['input_envelope'])
    if retained.get('source_revision_id')!=image['source_revision_id'] or retained.get('raw_uri')!=image['raw_uri'] or full_hash(retained.get('raw_sha256'))!=full_hash(image['raw_sha256']) or any(manifest['parent_'+k]!=envelope[k] for k in ('model_digest','config_digest')):raise Conflict('Web caption original image lineage differs')
    normalized=retained['normalized'];source=manifest['source_metadata']
    if source.get('web_image_source_revision_id')!=image['source_revision_id'] or source.get('normalized_sha256')!=normalized['sha256'] or source.get('original_raw_sha256')!=image['raw_sha256'] or asset['asset_sha256']!=normalized['sha256'] or manifest.get('page_number')!=1 or manifest.get('bbox')!=[0.0,0.0,1.0,1.0]:raise Conflict('Web caption is not bound to retained normalized image')
    result=await asyncio.to_thread(_json,stage_root,stage['artifact_uri'],stage['artifact_digest'])
    caption=await asyncio.to_thread(caption_record,result,stage,asset,manifest,{'stage_key':extraction['idempotency_key'],'source_revision_id':image['source_revision_id']},contract,stage_root)
    p=caption['metadata']['caption_derivation']
    return {'stage_key':stage_key,'document_id':image['document_id'],'canonical_uri':image['canonical_uri'],'text':caption['text'],**{k:p[k] for k in ('asset_sha256','model_digest','config_digest','stage_artifact_sha256')}}

async def verify_derived(c,row,parent,candidate,page,contract,raw_root,stage_root):
    from .web_groups import _json
    proof=await asyncio.to_thread(_json,raw_root,row['proof_uri'],row['proof_sha256'])
    if proof.get('schema_version')!='web-caption-page-derivation.v1':raise Conflict('Unknown web caption derivation schema')
    if row['parent_stage_key']!=candidate['stage_key'] or row['record_sha256']!=record_digest(proof['record']) or proof['parent_record_sha256']!=record_digest(parent) or proof['parent_stage_key']!=candidate['stage_key']:raise Conflict('Web derived page immutable parent differs')
    images={a['document_id']:a for a in page['assets'] if a['kind']=='image'}
    dependencies=[]
    for retained in proof['record']['metadata']['web_caption_derivation']['captions']:
        image=images.get(retained['document_id'])
        if not image:raise Conflict('Web caption image unlinked from selected page')
        actual=await caption_dependency(c,retained['stage_key'],image,contract,stage_root)
        if canonical(actual)!=canonical(retained):raise Conflict('Web caption dependency changed')
        dependencies.append(actual)
    derived=derived_page(parent,dependencies,proof['record']['vector'],contract,proof['record']['metadata']['web_caption_derivation']['embedding_config_digest'],candidate['stage_key'])
    if canonical(derived)!=canonical(proof['record']):raise Conflict('Web parent rematerialization changed')
    return derived

async def verify_page_record(c,op,page,candidate,parent,selected,contract,raw_root,stage_root):
    """Gate selected snapshot content, including inherited derived page records."""
    if selected['record_id']!=parent['record_id']:raise Conflict('Web page record identity changed')
    if record_digest(selected)==record_digest(parent):return
    if 'web_enrichment_derivation' in selected['metadata']:
        from .web_children import verify_enriched
        return await verify_enriched(c,op,page,candidate,parent,selected,contract,raw_root,stage_root)
    if not await c.fetchval("SELECT to_regclass('collection_control.web_caption_derivations')"):raise Conflict('Web page differs without a caption derivation')
    row=await c.fetchrow('SELECT * FROM collection_control.web_caption_derivations WHERE record_sha256=$1',record_digest(selected))
    if not row or row['collection_id']!=op['collection_id']:raise Conflict('Selected web page lacks immutable caption derivation')
    actual=await verify_derived(c,row,parent,candidate,page,contract,raw_root,stage_root)
    if record_digest(actual)!=record_digest(selected):raise Conflict('Selected page differs from caption derivation')


class WebCaptionRepository:
    def __init__(self,control,collection_id,contract,stage_root,raw_root,snapshot_root):
        self.control=control;self.pool=control.pool;self.collection_id=collection_id;self.contract=contract
        self.stage_root=Path(stage_root);self.raw_root=Path(raw_root);self.snapshot_root=Path(snapshot_root)

    async def admit(self,stage_key,*,embed,embedding_config_digest,expected_model_digest):
        """Capture a current snapshot, compute outside locks, then fence and prepare.

        A premature callback stays in Redis pending without another caption call.
        An obsolete callback returns None and can be acknowledged terminally.
        """
        from .web_groups import resolve_graph,_verify_candidate,_json
        from .admission_repository import ProvenanceRepository
        # v2 images live in independent child jobs rather than sealed members.
        async with self.pool.acquire() as c,c.transaction():
            col=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1',self.collection_id)
            asset=await c.fetchrow('SELECT * FROM caption_control.assets WHERE stage_key=$1',stage_key)
            if asset:
                active=await c.fetchrow('SELECT d.status AS document_status,j.status AS job_status FROM public.documents d JOIN public.jobs j ON j.job_id=d.job_id WHERE d.document_id=$1 AND d.job_id=$2 FOR SHARE OF d,j',asset['document_id'],asset['parent_job_id'])
                if not active or any(active[k] in ('cancelled','canceled','deleted','failed','superseded') for k in ('document_status','job_status')):return None
                if asset['collection_id']!=self.collection_id:raise Conflict('Caption belongs to another collection')
            if col and not col['deleted'] and asset and col['current_operation']:
                graph=await resolve_graph(c,col)
                if graph.get('schema_version')=='web-published-graph.v2':
                    images=[a for p in graph['pages'] for a in p['assets'] if a['kind']=='image' and a['document_id']==asset['document_id']]
                    if images:
                        reference=images[0].get('publication_ref')
                        # A retained legacy image continues through its v1 proof path.
                        # Check selected caption model before using the generalized derivation.
                        dependency=await caption_dependency(c,stage_key,images[0],self.contract,self.stage_root)
                        if dependency['model_digest']!=full_hash(expected_model_digest):raise Conflict('Web caption result uses another selected model')
                    else:
                        # Native image publication can legitimately still be pending.
                        from .web_children import unselected_caption_pending
                        if await unselected_caption_pending(c,asset,graph,self.collection_id,self.raw_root):raise Conflict('Web caption awaits selected native image publication')
                        return None
                else:reference=None
            else:reference=None
        if reference:
            from .web_children import admit_child
            return await admit_child(self,reference,embed=embed,embedding_config_digest=embedding_config_digest,caption_stage_key=stage_key)
        async with self.pool.acquire() as c,c.transaction():
            col=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',self.collection_id)
            if not col or col['deleted']:return None
            if col['contract_sha256']!=sha(self.contract):raise Conflict('Web caption collection contract changed')
            asset=await c.fetchrow('SELECT * FROM caption_control.assets WHERE stage_key=$1',stage_key)
            group=await c.fetchrow("SELECT g.* FROM collection_control.web_groups g JOIN collection_control.web_group_members m USING(group_id) WHERE m.document_id=$1 AND m.role='image'",asset['document_id'] if asset else '')
            if not asset or asset['collection_id']!=self.collection_id or not group:raise Conflict('Caption is outside grouped image scope')
            active=await c.fetchrow('SELECT d.status AS document_status,j.status AS job_status FROM public.documents d JOIN public.jobs j ON j.job_id=d.job_id WHERE d.document_id=$1 AND d.job_id=$2 FOR SHARE OF d,j',asset['document_id'],asset['parent_job_id'])
            if not active or any(active[k] in ('cancelled','canceled','deleted','failed','superseded') for k in ('document_status','job_status')):return None
            if not group['operation_id']:raise Conflict('Web caption awaits initial parent publication')
            group_operation=await c.fetchval('SELECT status FROM collection_control.operations WHERE operation_id=$1',group['operation_id'])
            if group_operation!='published':raise Conflict('Web caption awaits parent receipts')
            graph=await resolve_graph(c,col)
            pages=[p for p in graph['pages'] if any(a['kind']=='image' and a['document_id']==asset['document_id'] for a in p['assets'])]
            if not pages:return None
            pending=await c.fetchrow("SELECT * FROM collection_control.operations WHERE collection_id=$1 AND idempotency_key=$2 AND status='pending'",self.collection_id,'web-caption:'+stage_key+':'+str(col['current_operation']))
            if pending:
                await self.control._proof(c,pending)
                return dict(pending)
            prior=await c.fetchrow('SELECT * FROM collection_control.operations WHERE operation_id=$1',col['current_operation'])
            await self.control._proof(c,prior)
            base=await asyncio.to_thread(load_artifact,self.snapshot_root,prior['artifact_sha256'])
            updates=[]
            for page in pages:
                candidate=await c.fetchrow('SELECT a.* FROM collection_control.web_source_candidates a WHERE a.document_id=$1 AND a.source_revision_id=$2',page['document_id'],page['source_revision_id'])
                if not candidate:raise Conflict('Web caption current page original candidate missing')
                original,_=await _verify_candidate(c,candidate,self.contract,self.raw_root,self.stage_root)
                from application.html_disclosures import main_records
                original=main_records(original)
                if len(original)!=1:raise Conflict('Web page requires one original main-page vector')
                parent=original[0];selected=[r for r in base['records'] if r['record_id']==parent['record_id']]
                if len(selected)!=1:return None
                existing=selected[0]['metadata'].get('web_caption_derivation',{}).get('captions',[])
                keys=sorted({x['stage_key'] for x in existing}|{stage_key})
                images={a['document_id']:a for a in page['assets'] if a['kind']=='image'}
                captions=[]
                for key in keys:
                    item=await c.fetchrow('SELECT document_id FROM caption_control.assets WHERE stage_key=$1',key)
                    image=images.get(item['document_id']) if item else None
                    if not image:return None
                    captions.append(await caption_dependency(c,key,image,self.contract,self.stage_root))
                current=next(x for x in captions if x['stage_key']==stage_key)
                if current['model_digest']!=full_hash(expected_model_digest):raise Conflict('Web caption result uses another selected model')
                if keys==[x['stage_key'] for x in existing]:continue
                updates.append((parent,candidate,captions))
            if not updates:
                return dict(prior)
            base_operation=col['current_operation'];base_revision=col['revision'];contract_sha=col['contract_sha256']
        replacements=[]
        for parent,candidate,captions in updates:
            text=captioned_text(parent['text'],captions)
            vector=await asyncio.to_thread(embed,text)
            record=derived_page(parent,captions,vector,self.contract,embedding_config_digest,candidate['stage_key'])
            proof={'schema_version':'web-caption-page-derivation.v1','parent_stage_key':candidate['stage_key'],'parent_record_sha256':record_digest(parent),'record':record}
            from nv_ingest_durable.deferred_caption import retain
            uri,digest=await asyncio.to_thread(retain,self.raw_root,canonical(proof),'.web-caption-page.json')
            replacements.append((record,candidate['stage_key'],uri,digest))
        by_id={r['record_id']:r for r,_,_,_ in replacements}
        snapshot={'contract':self.contract,'records':[by_id.get(r['record_id'],r) for r in base['records']]}
        snapshot_sha=await asyncio.to_thread(store_snapshot,self.snapshot_root,snapshot)
        async with self.pool.acquire() as c,c.transaction():
            locked=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',self.collection_id)
            if not locked or locked['deleted']:return None
            if locked['current_operation']!=base_operation or locked['revision']!=base_revision:raise Conflict('Web caption parent publication changed during embedding')
            if await c.fetchval("SELECT EXISTS(SELECT 1 FROM collection_control.operations WHERE collection_id=$1 AND status='pending')",self.collection_id):raise Conflict('Collection publication pending')
            op=await ProvenanceRepository(PinnedPool(c)).prepare(self.collection_id,'web-caption:'+stage_key+':'+str(base_operation),'snapshot',base_revision,snapshot_sha,contract_sha)
            for record,parent_key,uri,digest in replacements:
                await c.execute('INSERT INTO collection_control.web_caption_derivations(record_sha256,collection_id,parent_stage_key,proof_uri,proof_sha256) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING',record_digest(record),self.collection_id,parent_key,uri,digest)
            await c.execute("INSERT INTO collection_control.generation_admissions(operation_id,artifact_sha256,content_sha256,row_count,policy_version) VALUES($1,$2,$3,$4,'retained-source-stage.v1')",op['operation_id'],snapshot_sha,content_digest(snapshot['records']),len(snapshot['records']))
            ids=[r['record_id'] for r in snapshot['records']]
            for table in ('record_lineage','caption_record_lineage'):
                if await c.fetchval('SELECT to_regclass($1)','collection_control.'+table):
                    await c.execute('INSERT INTO collection_control.'+table+'(operation_id,record_id,record_sha256,stage_key) SELECT $1,record_id,record_sha256,stage_key FROM collection_control.'+table+' WHERE operation_id=$2 AND record_id=ANY($3::text[])',op['operation_id'],base_operation,ids)
            for record,parent_key,_,_ in replacements:
                await c.execute('UPDATE collection_control.record_lineage SET record_sha256=$3 WHERE operation_id=$1 AND record_id=$2 AND stage_key=$4',op['operation_id'],record['record_id'],record_digest(record),parent_key)
            await self.control._proof(c,op)
            return dict(op)
