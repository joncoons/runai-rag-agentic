"""Content-addressed handoff admission; no source acquisition or embedding calls."""
import hashlib
import json
import math
import os
import stat
import struct
from pathlib import Path

from .repository import Conflict, digest


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf-8')


def record_digest(record):
    metadata={k:v for k,v in record.items() if k!='vector'}
    return hashlib.sha256(canonical(metadata)+struct.pack('<2048f',*record['vector'])).hexdigest()


def content_digest(records):
    return hashlib.sha256(canonical(sorted((r['record_id'],record_digest(r)) for r in records))).hexdigest()


def admit(artifact):
    try:
        c=artifact['contract'];rs=artifact['records']
        if c['dimensions']!=2048 or c['dtype']!='float32' or c['normalization']!='unit' or c['metric']!='IP' or c['schema_version']!=1:
            raise Conflict('Unsupported complete vector contract')
        digest(c['embedding_checkpoint_sha256'])
        if not isinstance(rs,list):raise Conflict('A full snapshot record list is required')
        ids=set()
        for r in rs:
            if not isinstance(r['record_id'],str) or not 1<=len(r['record_id'].encode())<=512 or r['record_id'] in ids:
                raise Conflict('Invalid or duplicate record identity')
            ids.add(r['record_id']);v=r['vector'];m=r['metadata']
            if len(v)!=2048 or any(isinstance(x,bool) or not isinstance(x,(float,int)) or not math.isfinite(x) for x in v):
                raise Conflict('Expected2048 finite float32 coordinates')
            packed=struct.pack('<2048f',*v);norm=math.sqrt(sum(x*x for x in struct.unpack('<2048f',packed)))
            if abs(norm-1)>1e-4:raise Conflict('Vector is not unit norm; implicit normalization forbidden')
            if m['origin'] not in ('grounded','synthetic','derived') or not m['source_id'] or not m['source_revision_id']:
                raise Conflict('Missing source/revision/origin lineage')
            if m['origin']=='derived' and 'web_enrichment_derivation' in m:
                from .web_captions import captioned_text
                proof=m['web_enrichment_derivation']
                if proof.get('policy') not in ('retained-web-enrichment.v2','retained-web-enrichment.v3') or proof.get('parent_record_id')!=r['record_id'] or proof.get('source_revision_id')!=m['source_revision_id'] or proof.get('text_sha256')!=m['content_sha256'] or proof.get('embedding_checkpoint_sha256')!=c['embedding_checkpoint_sha256'] or not proof.get('parent_stage_key') or m.get('generated_annotation') is not bool(proof.get('captions')):
                    raise Conflict('Derived web page requires explicit native/caption dependency proof')
                digest(proof['parent_record_sha256']);digest(proof['embedding_config_digest'])
                images=proof['images']
                if not isinstance(images,list) or [i['document_id'] for i in images]!=sorted({i['document_id'] for i in images}):raise Conflict('Native image dependencies must be unique and deterministic')
                for image in images:
                    if not all(isinstance(image[k],str) and image[k] for k in ('document_id','source_revision_id','canonical_uri','stage_key')) or not isinstance(image['text'],str):raise Conflict('Invalid native image dependency')
                    digest(image['stage_artifact_sha256'])
                if proof['captions']:captioned_text('',proof['captions'])
                if not images and not proof['captions']:raise Conflict('Enrichment requires retained image or caption dependencies')
            elif m['origin']=='derived' and 'web_caption_derivation' in m:
                from .web_captions import POLICY, LEGACY_POLICY, captioned_text
                proof=m['web_caption_derivation']
                if proof.get('policy') not in (POLICY,LEGACY_POLICY) or m.get('generated_annotation') is not True or proof.get('parent_record_id')!=r['record_id'] or proof.get('source_revision_id')!=m['source_revision_id'] or proof.get('text_sha256')!=m['content_sha256'] or proof.get('embedding_checkpoint_sha256')!=c['embedding_checkpoint_sha256'] or not proof.get('parent_stage_key'):
                    raise Conflict('Derived web page requires original parent and explicit caption proof')
                digest(proof['parent_record_sha256']);digest(proof['embedding_config_digest'])
                captioned_text('',proof['captions'])
            elif m['origin']=='derived':
                proof=m.get('caption_derivation',{})
                if proof.get('policy')!='retained-caption-stage.v1' or m.get('generated_annotation') is not True or proof.get('text_sha256')!=m.get('content_sha256') or proof.get('source_revision_id')!=m.get('source_revision_id') or proof.get('embedding_checkpoint_sha256')!=c['embedding_checkpoint_sha256']:
                    raise Conflict('Derived records require explicit caption derivation proof')
                for key in ('asset_sha256','stage_artifact_sha256','model_digest','config_digest','embedding_config_digest'):
                    digest(proof[key])
                if not proof.get('caption_stage_key') or not proof.get('parent_stage_key'):
                    raise Conflict('Derived caption parent/stage identity missing')
            digest(m['content_sha256'])
            if not isinstance(r['text'],str) or hashlib.sha256(r['text'].encode()).hexdigest()!=m['content_sha256']:
                raise Conflict('Text integrity mismatch')
            if not isinstance(m['acl'],list) or not m['acl'] or any(not isinstance(a,str) or not a for a in m['acl']):
                raise Conflict('Explicit nonempty ACL required')
    except (KeyError,TypeError,ValueError,OverflowError,struct.error) as e:
        if isinstance(e,Conflict):raise
        raise Conflict('Artifact fails vector/provenance admission') from e
    return artifact


def _unique_object(pairs):
    out={}
    for k,v in pairs:
        if k in out:raise Conflict('Duplicate JSON key')
        out[k]=v
    return out


def load_artifact(root,sha256):
    digest(sha256)
    path=Path(root).resolve()/(sha256+'.json')
    try:
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
        with os.fdopen(fd,'rb') as f:
            st=os.fstat(f.fileno())
            if not stat.S_ISREG(st.st_mode) or st.st_size>512*1024*1024:raise Conflict('Invalid or oversized artifact file')
            raw=f.read(512*1024*1024+1)
    except OSError as e:raise Conflict('Artifact unavailable or symlink refused') from e
    if hashlib.sha256(raw).hexdigest()!=sha256:raise Conflict('Artifact SHA-256 mismatch')
    try:artifact=json.loads(raw,object_pairs_hook=_unique_object)
    except (ValueError,UnicodeError) as e:raise Conflict('Invalid artifact JSON') from e
    return admit(artifact)
