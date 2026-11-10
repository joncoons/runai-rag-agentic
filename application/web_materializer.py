"""Read committed native image dependencies before the single parent embedding."""
import hashlib
from collection_control.repository import Conflict
from collection_control.web_materialization import image_evidence,materialized_text,verify_materialization

async def page_materialization(pool,revision,raw_root,units):
    group=await pool.fetchrow('SELECT * FROM collection_control.web_groups WHERE group_id=$1',revision['web_group_id'])
    if not group:raise Conflict('Web group is unavailable')
    import json
    from collection_control.provenance import verified_file
    manifest=json.loads(verified_file(raw_root,group['manifest_uri'],group['manifest_sha256']))
    page=next(p for p in manifest['pages'] if p['source_revision_id']==revision['source_revision_id'])
    images=[]
    for asset in sorted({a['document_id']:a for a in page['assets'] if a['kind']=='image'}.values(),key=lambda a:a['document_id']):
        stage=await pool.fetchrow("SELECT * FROM public.stage_units WHERE document_id=$1 AND stage='extraction' AND status='committed'",asset['document_id'])
        if not stage:raise Conflict('Native image evidence is not committed')
        envelope=json.loads(stage['input_envelope']) if isinstance(stage['input_envelope'],str) else stage['input_envelope']
        proof={k:asset[k] for k in ('document_id','source_revision_id','canonical_uri','raw_uri','raw_sha256')}
        proof.update(stage_key=stage['idempotency_key'],artifact_uri=stage['artifact_uri'],sha256=stage['artifact_digest'].removeprefix('sha256:'),model_digest=envelope['model_digest'],config_digest=envelope['config_digest'])
        images.append((proof,image_evidence(raw_root,proof)))
    base=units['units'][0]['text'];text=materialized_text(base,images)
    proof={'schema_version':'web-page-materialization.v1','group_id':str(revision['web_group_id']),'collection_id':revision['collection_id'],'document_id':revision['document_id'],'canonical_uri':revision['canonical_uri'],'source_revision_id':revision['source_revision_id'],'group_manifest_uri':group['manifest_uri'],'group_manifest_sha256':group['manifest_sha256'],'html_text_sha256':hashlib.sha256(base.encode()).hexdigest(),'images':[i[0] for i in images],'text_sha256':hashlib.sha256(text.encode()).hexdigest()}
    if verify_materialization(raw_root,proof,revision['source_revision_id'],base)!=text:raise Conflict('Web image evidence materialization differs')
    return proof,text
