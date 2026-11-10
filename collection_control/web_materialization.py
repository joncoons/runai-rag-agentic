"""Reproducible whole-page text from captured HTML and committed image evidence."""
import hashlib
import json
from pathlib import Path
from .artifacts import canonical,_unique_object
from .provenance import verified_file,full_hash
from .repository import Conflict


def image_evidence(raw_root,proof):
    body=json.loads(verified_file(Path(raw_root).parent/'stages',proof['artifact_uri'],full_hash(proof['sha256'])),object_pairs_hook=_unique_object)
    for key in ('document_id','source_revision_id','raw_uri','raw_sha256'):
        if body[key]!=proof[key]:raise Conflict('Image evidence differs from retained dependency identity')
    if body['schema_version']!='native-web-image-evidence.v1' or body['status']!='complete':raise Conflict('Incomplete native image dependency')
    verified_file(raw_root,body['raw_uri'],full_hash(body['raw_sha256']))
    verified_file(raw_root,body['normalized']['uri'],full_hash(body['normalized']['sha256']))
    if body['native']['coordinate_frame']!='normalized_image' or body['native']['internal_frame_number']!=1:raise Conflict('Unqualified image coordinate frame')
    for key in ('model_digest','config_digest'):
        if full_hash(body['native'][key])!=full_hash(proof[key]):raise Conflict('Image computation differs from dependency contract')
    if not isinstance(body['text'],str):raise Conflict('Native image evidence requires complete text')
    return body


def materialized_text(base,images):
    text=base
    for image,body in images:
        if body['text'].strip():text+='\n\n[Grounded image evidence: '+image['canonical_uri']+']\n'+body['text']
    return text


def verify_materialization(raw_root,proof,revision,base):
    if proof['schema_version']!='web-page-materialization.v1' or proof['source_revision_id']!=revision or proof['html_text_sha256']!=hashlib.sha256(base.encode()).hexdigest():raise Conflict('Web materialization differs from captured HTML revision')
    group=json.loads(verified_file(raw_root,proof['group_manifest_uri'],full_hash(proof['group_manifest_sha256'])),object_pairs_hook=_unique_object)
    page=next((p for p in group['pages'] if p['source_revision_id']==revision),None)
    if any(proof.get(k)!=group.get(k) for k in ('group_id','collection_id')) or not page or any(proof.get(k)!=page.get(k) for k in ('document_id','canonical_uri')):raise Conflict('Materialization belongs to another group or page')
    if not page or group['schema_version']!='web-asset-group.v1' or group['coverage']!='complete':raise Conflict('Materialization requires a complete retained page-asset manifest')
    expected={a['document_id']:a for a in page['assets'] if a['kind']=='image'}
    if len(proof['images'])!=len(expected) or len({i['document_id'] for i in proof['images']})!=len(expected):raise Conflict('Image materialization dependency coverage differs')
    images=[]
    for image in proof['images']:
        asset=expected.get(image['document_id'])
        if not asset or any(image[k]!=asset[k] for k in ('source_revision_id','canonical_uri','raw_uri','raw_sha256','model_digest','config_digest')):raise Conflict('Image materialization references another page/asset observation')
        images.append((image,image_evidence(raw_root,image)))
    if [i[0]['document_id'] for i in images]!=sorted(expected):raise Conflict('Image materialization order must be deterministic')
    text=materialized_text(base,images)
    if proof['text_sha256']!=hashlib.sha256(text.encode()).hexdigest():raise Conflict('Materialized page text changed')
    return text
