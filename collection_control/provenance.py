"""Strict mapping of the local unstructured-source provenance contract.

No fallback source/revision/chunk identities or historical observations are made.
Hash verification establishes correspondence, not authorship or factual truth.
"""
import copy
import hashlib
import json
import math
import os
import stat
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote,urlparse
from uuid import uuid4

from .artifacts import canonical,admit,_unique_object
from .repository import Conflict,digest


def full_hash(value):
    if not isinstance(value,str):raise Conflict('Missing full source hash')
    return digest(value.removeprefix('sha256:').lower())


def verified_file(root,uri,expected,size=None):
    """Read a bounded regular file beneath a trusted root without symlink traversal."""
    digest(expected)
    parsed=urlparse(uri)
    if parsed.scheme!='file' or parsed.netloc or parsed.query or parsed.fragment:
        raise Conflict('Only retained local file artifacts are supported')
    root=Path(root).resolve();path=Path(unquote(parsed.path))
    try:relative=path.relative_to(root)
    except ValueError as e:raise Conflict('Artifact escapes trusted root') from e
    if not relative.parts or any(p in ('.','..') for p in relative.parts):raise Conflict('Invalid artifact path')
    try:
        with ExitStack() as stack:
            def opened(path,flags,**kwargs):
                fd=os.open(path,flags,**kwargs);stack.callback(os.close,fd);return fd
            parent=opened(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            for component in relative.parts[:-1]:parent=opened(component,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent)
            fd=opened(relative.parts[-1],os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parent)
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size>512*1024*1024 or (size is not None and info.st_size!=size):
                raise Conflict('Artifact type or size is not admitted')
            parts=[];hasher=hashlib.sha256();total=0
            while True:
                block=os.read(fd,1024*1024)
                if not block:break
                total+=len(block)
                if total>512*1024*1024:raise Conflict('Artifact exceeds supported size')
                hasher.update(block);parts.append(block)
            if hasher.hexdigest()!=expected or total!=info.st_size:raise Conflict('Retained artifact hash/size mismatch')
            return b''.join(parts)
    except OSError as e:raise Conflict('Retained artifact unavailable or symlink refused') from e


def store_snapshot(root,artifact):
    admit(artifact);raw=canonical(artifact);sha=hashlib.sha256(raw).hexdigest()
    root=Path(root).resolve();root.mkdir(parents=True,exist_ok=True)
    final=root/(sha+'.json');temporary=root/('.pending-'+uuid4().hex)
    try:
        with temporary.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
        try:os.link(temporary,final)
        except FileExistsError:verified_file(root,final.as_uri(),sha,len(raw))
        fd=os.open(root,os.O_DIRECTORY|os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)
    finally:temporary.unlink(missing_ok=True)
    return sha


def envelope_digest(envelope):
    # Transport message/attempt timestamps are excluded; immutable processing identity is not.
    keys=('job_id','document_id','pipeline_version','config_digest','model_digest','idempotency_key','artifact_uri')
    try:
        values={k:envelope[k] if isinstance(envelope,dict) else getattr(envelope,k) for k in keys}
        values['stage']=envelope['stage'] if isinstance(envelope,dict) else envelope.stage.value
        return hashlib.sha256(canonical(values)).hexdigest()
    except (KeyError,TypeError,AttributeError) as error:
        raise Conflict('Missing immutable processing input envelope') from error


def checkpoint_records(checkpoint,envelope,document,job,contract,raw_root):
    try:
        if checkpoint['payload_format']!='es_docs' or checkpoint['schema_version']!='1.0':raise Conflict('Expected finalized es_docs checkpoint v1.0')
        records=[];identity=None;proofs=[];shared_revision=None
        expected_acl=json.loads(job['metadata']) if isinstance(job['metadata'],str) else dict(job['metadata'])
        job_metadata=expected_acl
        capture_run=expected_acl.get('ingestion_run_id')
        expected_acl=expected_acl.get('acl')
        if not isinstance(expected_acl,list) or not expected_acl or any(not isinstance(x,str) or not x for x in expected_acl):raise Conflict('Job requires explicit ACL policy')
        for doc in checkpoint['documents']:
            m=copy.deepcopy(doc['metadata']);p=m['provenance'];cm=m['content_metadata'];source=m['source']
            if p['schema_version']!='unstructured-source-provenance.v1' or cm['provenance_schema_version']!=p['schema_version']:raise Conflict('Unsupported upstream provenance schema')
            for key in ('source_revision_id','source_chunk_id'):
                if not isinstance(p[key],str) or not p[key] or source.get(key)!=p[key] or cm.get(key)!=p[key]:raise Conflict('Missing or conflicting upstream join IDs')
            if not capture_run or p['ingestion_run_id']!=capture_run or cm['ingestion_run_id']!=p['ingestion_run_id']:raise Conflict('Source run does not match durable job')
            if p['canonical_uri']!=document['source_uri'] or cm['canonical_uri']!=p['canonical_uri'] or not p['final_uri']:raise Conflict('Source locator does not match captured document')
            for key,expected in [('source_system',p['source_system']),('source_kind',p['source_kind']),('modality',p['modality']),('final_uri',p['final_uri']),('retrieved_at',p['observed_at'])]:
                if cm[key]!=expected or (key in source and source[key]!=expected):raise Conflict('Required source metadata mirror mismatch')
            if p['source_system'] not in ('web_crawl','document_capture') or p['source_kind'] in ('unknown','') or p['modality'] not in ('text','document'):
                raise Conflict('Derived image/audio/video lineage requires separate qualification')
            observed=datetime.fromisoformat(p['observed_at'].replace('Z','+00:00'))
            if observed.utcoffset() is None:raise Conflict('Source observation must be timezone-aware')
            anchors=p['anchors']
            if not isinstance(anchors,dict):raise Conflict('Exact source anchor required')
            section=(isinstance(anchors.get('heading_path'),list) and bool(anchors['heading_path']) and all(isinstance(x,str) and x.strip() for x in anchors['heading_path'])) or any(isinstance(anchors.get(k),str) and anchors[k].strip() for k in ('heading','section_h1'))
            page=isinstance(anchors.get('page_number'),int) and not isinstance(anchors['page_number'],bool) and anchors['page_number']>0
            if p['source_kind'] in TYPED_SOURCE_KINDS:
                typed_source_anchors(p['source_kind'],anchors)
            elif not (section if p['source_system']=='web_crawl' else (section or page)):raise Conflict('Source-specific section/page anchor required')
            if 'bbox' in anchors:
                box=anchors['bbox']
                if not page or not isinstance(box,list) or len(box)!=4 or any(not isinstance(x,(int,float)) or isinstance(x,bool) or not math.isfinite(x) for x in box) or box[0]>box[2] or box[1]>box[3]:raise Conflict('Invalid page bounding box anchor')
            text_hash=hashlib.sha256(doc['text'].encode()).hexdigest();raw_hash=full_hash(p['hashes']['raw_sha256'])
            if full_hash(p['hashes']['text_sha256'])!=text_hash or full_hash(cm['text_sha256'])!=text_hash or full_hash(cm['raw_sha256'])!=raw_hash or full_hash(document['source_digest'])!=raw_hash:
                raise Conflict('Source/text hash or mirror binding mismatch')
            verified_file(raw_root,p['raw_artifact_uri'],raw_hash)
            if p['source_kind']=='html' and 'acquisition' in p:
                verify_web_acquisition(raw_root,p['acquisition'],p['final_uri'],raw_hash)
            if job_metadata.get('ingestion_options',{}).get('connector_kind')=='location' and 'location_acquisition' not in p:raise Conflict('Registered location source requires acquisition evidence')
            if 'location_acquisition' in p:
                from .location_provenance import verify_location_capture
                verify_location_capture(raw_root,p['location_acquisition'],p['canonical_uri'],raw_hash,job['corpus_namespace'],job_metadata.get('source_run_id'))
            if p['hashes'].get('normalized_sha256'):
                verified_file(raw_root,p['normalized_artifact_uri'],full_hash(p['hashes']['normalized_sha256']))
            transforms=p['transforms']
            if not isinstance(transforms,list) or not transforms:raise Conflict('Versioned transformation lineage required')
            for t in transforms:
                if not t['component'] or not t['version']:raise Conflict('Transformation identity/version missing')
                full_hash(t['config_digest'])
                if t.get('model_digest'):full_hash(t['model_digest'])
            components={t['component']:t for t in transforms}
            parsers={'parser','html_parser','markdown_parser','text_parser','document_extraction','nemotron_parse','ocr'}
            parser=next((t for t in transforms if t['component'] in parsers),None)
            if parser is None or cm['parser_version']!=parser['version']:raise Conflict('Source parser/extraction version lineage required')
            if p['source_system']=='web_crawl':
                chunker=next((t for t in transforms if t['component'] in ('chunker','text_chunker','page_unit')),None)
                if chunker is None or cm['chunker_version']!=chunker['version']:raise Conflict('Web page-unit/chunker lineage required')
            else:
                method=cm.get('extraction_method')
                if method not in ('retained_text','text_layer','ocr','nemotron_ocr','nemotron_parse'):raise Conflict('Unqualified document extraction method')
                if method in ('ocr','nemotron_ocr') and ('ocr' not in components or not components['ocr'].get('model_digest')):raise Conflict('OCR method requires an OCR model transform')
                if method=='nemotron_parse' and not any(t['component'] in ('nemotron_parse','document_extraction') and t.get('model_digest') for t in transforms):raise Conflict('Parse method requires model-specific extraction lineage')
                if 'ocr' in components and not components['ocr'].get('model_digest'):raise Conflict('OCR model identity required')
            if not any(t['component']=='embedding' and full_hash(t['model_digest'])==full_hash(envelope.model_digest)==contract['embedding_checkpoint_sha256'] and full_hash(t['config_digest'])==full_hash(envelope.config_digest) for t in transforms):
                raise Conflict('Embedding transformation does not match stage/collection')
            if m.get('origin','grounded')!='grounded' or m.get('generated_annotation') is True or cm.get('generated_annotation') is True or m['acl']!=expected_acl:raise Conflict('Grounded source kind or ACL policy mismatch')
            aliases={'source_id':source['source_id'],'source_revision_id':p['source_revision_id'],'content_sha256':text_hash,'origin':'grounded','durable_document_id':envelope.document_id}
            for k,v in aliases.items():
                if k in m and m[k]!=v:raise Conflict('Conflicting flattened lineage field')
                m[k]=v
            revision_fields=(p['canonical_uri'],p['source_revision_id'],raw_hash,observed,p['ingestion_run_id'],p['final_uri'],p['source_system'],p['source_kind'],p['modality'],p['raw_artifact_uri'])
            if shared_revision is not None and shared_revision!=revision_fields:raise Conflict('Chunks disagree on immutable source revision fields')
            shared_revision=revision_fields
            current=(p['canonical_uri'],p['source_revision_id'],raw_hash,p.get('previous_source_revision_id'))
            if identity is not None and current!=identity:raise Conflict('Checkpoint must contain one complete source revision')
            identity=current
            records.append({'record_id':p['source_chunk_id'],'text':doc['text'],'vector':doc['vector'],'metadata':m})
            proofs.append(p)
        if not records:raise Conflict('Empty source revisions need an explicit delete operation')
        captured=job_metadata['source_manifests'][envelope.document_id]
        manifest_sha=full_hash(captured['sha256'])
        manifest=json.loads(verified_file(raw_root,captured['uri'],manifest_sha),object_pairs_hook=_unique_object)
        coverage=checkpoint['coverage']
        if manifest['schema_version']!='source-unit-manifest.v1' or manifest['document_id']!=envelope.document_id or manifest['source_revision_id']!=identity[1] or manifest['canonical_uri']!=identity[0] or full_hash(manifest['raw_sha256'])!=identity[2] or manifest['coverage']!='complete' or full_hash(coverage['manifest_sha256'])!=manifest_sha or coverage['failed_units']:
            raise Conflict('Source-unit coverage is incomplete or does not match retained intake')
        units=manifest['expected_units'];completed=coverage['completed_units']
        if not isinstance(units,list) or not units or any(not isinstance(u,str) or not u for u in units) or len(set(units))!=len(units) or not isinstance(completed,dict) or set(completed)!=set(units):raise Conflict('Expected source-unit coverage differs from completed extraction')
        covered=[]
        for ids in completed.values():
            if not isinstance(ids,list) or any(not isinstance(i,str) or not i for i in ids):raise Conflict('Invalid completed-unit record coverage')
            covered.extend(ids)
        if len(set(covered))!=len(covered) or set(covered)!={r['record_id'] for r in records}:raise Conflict('Full-source revision record coverage is incomplete')
        if 'location_acquisition' in manifest:
            ref=manifest['location_acquisition']
            if checkpoint['retained_source_capture'].get('location_acquisition')!=ref or any(r['metadata']['provenance'].get('location_acquisition')!=ref for r in records):raise Conflict('Location proof differs from source manifest')
        if job_metadata.get('ingestion_options',{}).get('connector_kind')=='location' and 'location_acquisition' not in manifest:raise Conflict('Source manifest omits registered acquisition')
        if 'native_source_units' in manifest:
            native=manifest['native_source_units']
            if canonical(checkpoint['native_source_units'])!=canonical(native) or native['schema_version']!='native-source-units.v1':raise Conflict('Retained typed source manifest differs from checkpoint')
            source_units=native['units'];by_unit={u['unit_id']:u for u in source_units}
            if native['source_kind']=='html':
                from application.html_disclosures import validate_html_units
                validate_html_units(native)
            if len(by_unit)!=len(source_units):raise Conflict('Duplicate original typed source units')
            materialized=None
            if 'web_materialization' in manifest:
                if native['source_kind']!='html':raise Conflict('Materialization is only qualified for HTML')
                from .web_materialization import verify_materialization
                materialized=verify_materialization(raw_root,manifest['web_materialization'],identity[1],by_unit['page:1']['text'])
            office=manifest.get('office_visuals')
            if job_metadata.get('ingestion_options',{}).get('office_visuals_enabled') and native['source_kind'] in ('docx','pptx') and office is None:raise Conflict('Missing required Office visual source coverage')
            if office is not None:
                from .office_visuals import verify_office_visuals
                if canonical(checkpoint.get('office_visuals'))!=canonical(office):raise Conflict('Office checkpoint/manifest proof differs')
                for unit in verify_office_visuals(raw_root,office,identity[2],native):
                    if unit['unit_id'] in by_unit:raise Conflict('Duplicate Office visual source unit')
                    by_unit[unit['unit_id']]=unit
            if list(by_unit)!=units:raise Conflict('Typed source-unit inventory differs from retained coverage')
            per_unit={u:[] for u in by_unit}
            for record in records:
                metadata=record['metadata'];p=metadata['provenance'];unit_id=p['source_unit_id'];unit=by_unit[unit_id]
                if native['source_kind']!=p['source_kind'] or unit['status']!='complete' or record['text']!=(materialized if materialized is not None and unit_id=='page:1' else unit['text']) or not (materialized if materialized is not None and unit_id=='page:1' else unit['text']).strip() or canonical(p['anchors'])!=canonical(unit['anchors']) or metadata['content_metadata']['source_unit_id']!=unit_id or metadata['source']['source_unit_id']!=unit_id:raise Conflict('Typed record differs from retained source unit')
                if materialized is not None and unit_id=='page:1' and canonical(p.get('web_materialization'))!=canonical(manifest['web_materialization']):raise Conflict('Record image materialization differs from retained proof')
                if unit['anchors'].get('section')=='office_visual' and canonical(p.get('office_visuals'))!=canonical(office):raise Conflict('Record Office proof differs')
                if native['source_kind']=='html' and native['transform']['version'] in ('2','3','4'):
                    from application.html_disclosures import record_metadata
                    expected_metadata=record_metadata(unit['anchors'],metadata['source_id'],p['source_revision_id'],p['canonical_uri'])
                    if any(metadata.get(k)!=v for k,v in expected_metadata.items()):raise Conflict('Disclosure audience/parent metadata differs from retained unit')
                transform=native['transform']
                if not any(t['component'] in ('document_extraction','html_parser') and t.get('name')==transform['name'] and t['version']==transform['version'] for t in p['transforms']):raise Conflict('Typed extraction transform differs from retained intake')
                per_unit[unit_id].append(record['record_id'])
            for unit_id,unit in by_unit.items():
                if unit['status']=='complete':
                    if not isinstance(unit['text'],str) or len(per_unit[unit_id])!=(1 if (materialized if materialized is not None and unit_id=='page:1' else unit['text']).strip() else 0):raise Conflict('Typed nonempty unit coverage is incomplete')
                elif unit['status']!='excluded' or not isinstance(unit.get('reason'),str) or not unit['reason'].strip():raise Conflict('Typed exclusion must be explicit')
                if per_unit[unit_id]!=completed[unit_id]:raise Conflict('Typed record coverage belongs to another source unit')
        admit({'contract':contract,'records':records})
        return records,identity,hashlib.sha256(canonical(proofs)).hexdigest()
    except (KeyError,TypeError,ValueError,AttributeError) as e:
        raise Conflict('Incomplete or invalid source-stage provenance') from e


def caption_record(checkpoint,stage,asset,manifest,parent,contract,stage_root):
    """Bind one generated annotation to retained pixels and a source admission."""
    try:
        envelope=json.loads(stage['input_envelope']) if isinstance(stage['input_envelope'],str) else stage['input_envelope']
        if checkpoint['payload_format']!='es_docs' or checkpoint['schema_version']!='1.0' or checkpoint['embedding_contract']!=contract or len(checkpoint['documents'])!=1:
            raise Conflict('Caption requires one finalized vector and explicit embedding contract')
        doc=checkpoint['documents'][0];m=copy.deepcopy(doc['metadata']);p=m['caption_provenance'];context=p['source_context']
        text_sha=hashlib.sha256(doc['text'].encode()).hexdigest()
        embedding_config_digest=full_hash(checkpoint['embedding_config_digest'])
        if checkpoint['caption_text_sha256']!=text_sha or full_hash(checkpoint['caption_asset_sha256'])!=asset['asset_sha256']:
            raise Conflict('Caption text/asset checkpoint binding differs')
        scope=asset['source_metadata'];scope=json.loads(scope) if isinstance(scope,str) else scope
        expected={'source_uri':asset['source_uri'],'source_digest':asset['source_digest'],'collection_id':asset['collection_id'],**scope}
        if context!=expected or m['acl']!=scope['acl'] or p['parent_job_id']!=str(asset['parent_job_id']) or p['document_id']!=asset['document_id']:
            raise Conflict('Caption source scope differs from retained asset')
        if envelope['idempotency_key']!=asset['stage_key'] or envelope['document_id']!=asset['document_id'] or envelope['job_id']!=str(asset['parent_job_id']) or envelope['artifact_uri']!=asset['manifest_uri'] or envelope['stage']!='caption':
            raise Conflict('Caption persisted envelope differs from asset association')
        if manifest['parent_job_id']!=str(asset['parent_job_id']) or manifest['document_id']!=asset['document_id'] or manifest['asset_sha256']!=asset['asset_sha256'] or p['parent_stage_key']!=manifest['parent_stage_key'] or p['asset_uri']!=manifest['asset_uri'] or p['source_metadata']!=manifest['source_metadata']:
            raise Conflict('Caption derivation differs from retained image manifest')
        if full_hash(p['asset_sha256'])!=asset['asset_sha256'] or full_hash(p['model_digest'])!=full_hash(envelope['model_digest']) or full_hash(manifest['model_digest'])!=full_hash(envelope['model_digest']) or full_hash(p['config_digest'])!=full_hash(envelope['config_digest']) or hashlib.sha256(canonical(manifest['caption_config'])).hexdigest()!=full_hash(envelope['config_digest']):
            raise Conflict('Caption model/config identity differs from retained computation')
        cm=m['content_metadata']
        if cm['generated_annotation'] is not True or cm['type']!='image_caption' or cm['page_number']!=manifest['page_number']:
            raise Conflict('Caption page/crop anchor differs from retained image')
        if not isinstance(cm['page_number'],int) or isinstance(cm['page_number'],bool) or cm['page_number']<1:
            raise Conflict('Caption requires a positive source page anchor')
        retained_bbox=manifest['bbox'];result_bbox=cm['bbox'];geometry_encoding='exact_json'
        for box in (retained_bbox,result_bbox):
            if box is not None and (not isinstance(box,list) or len(box)!=4 or any(isinstance(x,bool) or not isinstance(x,(int,float)) or not math.isfinite(x) for x in box) or box[0]>box[2] or box[1]>box[3]):
                raise Conflict('Caption crop requires four finite ordered coordinates')
        if canonical(result_bbox)!=canonical(retained_bbox):
            # Older CaptionProcessor used pandas JSON's ten-digit precision.
            # Admit only the exact same retained native float32 crop; no
            # numerical tolerance or quantization of arbitrary JSON geometry.
            import struct
            if retained_bbox is None or result_bbox is None:raise Conflict('Caption crop anchor differs from retained image')
            try:
                packed=struct.pack('<4f',*retained_bbox)
                if any(float(value)!=stored for value,stored in zip(retained_bbox,struct.unpack('<4f',packed))) or struct.pack('<4f',*result_bbox)!=packed:
                    raise Conflict('Caption crop is not the identical retained float32 geometry')
            except (OverflowError,struct.error) as error:
                raise Conflict('Caption crop cannot represent retained float32 geometry') from error
            geometry_encoding='retained_float32_equivalent'
        cm['bbox']=copy.deepcopy(retained_bbox)
        verified_file(stage_root,manifest['asset_uri'],asset['asset_sha256'])
        proof={'policy':'retained-caption-stage.v1','caption_stage_key':asset['stage_key'],'parent_stage_key':manifest['parent_stage_key'],'parent_source_stage_key':parent['stage_key'],'source_revision_id':parent['source_revision_id'],'asset_sha256':asset['asset_sha256'],'stage_artifact_sha256':full_hash(stage['artifact_digest']),'model_digest':full_hash(envelope['model_digest']),'config_digest':full_hash(envelope['config_digest']),'text_sha256':text_sha,'embedding_checkpoint_sha256':contract['embedding_checkpoint_sha256'],'embedding_config_digest':embedding_config_digest,'geometry_encoding':geometry_encoding}
        m.update(origin='derived',generated_annotation=True,source_id=asset['stage_key'],source_revision_id=parent['source_revision_id'],content_sha256=text_sha,durable_document_id=asset['document_id'],caption_derivation=proof)
        # Preserve the upstream source provenance separately; generated text is
        # never presented as a captured grounded chunk.
        m['provenance']={'canonical_uri':asset['source_uri']}
        record={'record_id':'caption:'+hashlib.sha256(asset['stage_key'].encode()).hexdigest(),'text':doc['text'],'vector':doc['vector'],'metadata':m}
        admit({'contract':contract,'records':[record]})
        return record
    except (KeyError,TypeError,ValueError,AttributeError) as e:
        raise Conflict('Incomplete caption derivation proof') from e


def empty_source_capture(checkpoint,stage,document,job,raw_root):
    """Qualify a complete captured source with zero source text/vector rows."""
    try:
        capture=checkpoint['retained_source_capture'];metadata=json.loads(job['metadata']) if isinstance(job['metadata'],str) else job['metadata']
        if checkpoint['schema_version']!='1.0' or checkpoint['payload_format']!='es_docs' or checkpoint['documents']!=[]:
            raise Conflict('Capture-only parent must have zero source records')
        if capture['canonical_uri']!=document['source_uri'] or full_hash(capture['raw_sha256'])!=full_hash(document['source_digest']) or capture['ingestion_run_id']!=metadata['ingestion_run_id'] or capture['source_manifest']!=metadata['source_manifests'][document['document_id']]:
            raise Conflict('Empty capture source/run/manifest binding differs')
        observed=datetime.fromisoformat(capture['observed_at'].replace('Z','+00:00'))
        if observed.utcoffset() is None or not isinstance(capture['source_revision_id'],str) or not capture['source_revision_id']:
            raise Conflict('Empty capture requires actual observation and revision identity')
        verified_file(raw_root,capture['raw_artifact_uri'],full_hash(capture['raw_sha256']))
        bound=capture['source_manifest'];manifest=json.loads(verified_file(raw_root,bound['uri'],full_hash(bound['sha256'])),object_pairs_hook=_unique_object)
        coverage=checkpoint['coverage'];units=manifest['expected_units']
        if manifest['schema_version']!='source-unit-manifest.v1' or manifest['coverage']!='complete' or manifest['document_id']!=document['document_id'] or manifest['source_revision_id']!=capture['source_revision_id'] or manifest['canonical_uri']!=capture['canonical_uri'] or full_hash(manifest['raw_sha256'])!=full_hash(capture['raw_sha256']) or full_hash(coverage['manifest_sha256'])!=full_hash(bound['sha256']) or coverage['failed_units'] or not isinstance(units,list) or not units or len(set(units))!=len(units) or any(not isinstance(u,str) or not u for u in units) or coverage['completed_units']!={u:[] for u in units}:
            raise Conflict('Empty captured source does not prove complete extraction coverage')
        if metadata.get('ingestion_options',{}).get('connector_kind')=='location' and 'location_acquisition' not in manifest:raise Conflict('Empty location source omitted acquisition')
        if 'location_acquisition' in manifest:
            from .location_provenance import verify_location_capture
            if capture.get('location_acquisition')!=manifest['location_acquisition']:raise Conflict('Empty location acquisition differs')
            verify_location_capture(raw_root,manifest['location_acquisition'],capture['canonical_uri'],capture['raw_sha256'],job['corpus_namespace'],metadata.get('source_run_id'))
        if 'office_visuals' in manifest:
            from .office_visuals import verify_office_visuals
            if verify_office_visuals(raw_root,manifest['office_visuals'],capture['raw_sha256'],manifest['native_source_units']):raise Conflict('Nonempty Office visual evidence cannot be omitted')
        if 'web_materialization' in manifest:
            from .web_materialization import verify_materialization
            materialization=manifest['web_materialization']
            if canonical(materialization)!=canonical(checkpoint.get('web_materialization')) or verify_materialization(raw_root,materialization,capture['source_revision_id'],manifest['native_source_units']['units'][0]['text']).strip():raise Conflict('Nonempty image materialization cannot be dropped as empty capture')
        return {'stage_key':stage['idempotency_key'],'document_id':document['document_id'],'source_revision_id':capture['source_revision_id'],'raw_sha256':full_hash(capture['raw_sha256']),'collection_id':job['corpus_namespace'],'capture':capture}
    except (KeyError,TypeError,ValueError,AttributeError) as e:
        raise Conflict('Incomplete empty-source capture proof') from e


TYPED_SOURCE_KINDS = frozenset(('docx','pptx','txt','markdown','csv','xls','xlsx','html'))


def typed_source_anchors(source_kind, anchors):
    """Validate native format anchors without manufacturing PDF page identities.

    Line/paragraph/slide/record/row/column/sheet indices are one-based;
    character offsets are zero-based half-open offsets. Package part names and
    spreadsheet cell references identify source structure, never filesystem paths.
    """
    def positive(value):
        return isinstance(value,int) and not isinstance(value,bool) and value>0
    def text(value):
        return isinstance(value,str) and bool(value.strip())
    if source_kind not in TYPED_SOURCE_KINDS or not isinstance(anchors,dict) or 'page_number' in anchors or 'bbox' in anchors:
        raise Conflict('Typed source requires actual format-specific anchors')
    if anchors.get('section')=='office_visual':
        scope=anchors.get('source_scope',{})
        if source_kind not in ('docx','pptx') or not text(anchors.get('part')) or scope.get('precision') not in ('document','slide','paragraph') or not isinstance(scope.get('unit_ids'),list) or len(set(scope['unit_ids']))!=len(scope['unit_ids']) or not all(text(u) for u in scope['unit_ids']):raise Conflict('Invalid Office visual anchor scope')
        return
    for key in ('paragraph_index','table_index','cell_row','cell_column','slide_number','record_number','line_start','line_end','row','column','sheet_index'):
        if key in anchors and not positive(anchors[key]):raise Conflict('Typed source anchor index must be positive')
    for key in ('char_start','char_end'):
        if key in anchors and (not isinstance(anchors[key],int) or isinstance(anchors[key],bool) or anchors[key]<0):raise Conflict('Invalid source character offset')
    if ('line_start' in anchors)!=('line_end' in anchors) or ('line_start' in anchors and anchors['line_end']<anchors['line_start']):raise Conflict('Invalid source line range')
    if ('char_start' in anchors)!=('char_end' in anchors) or ('char_start' in anchors and anchors['char_end']<anchors['char_start']):raise Conflict('Invalid source character range')
    if source_kind=='html':
        valid=(anchors.get('section')=='page' and set(anchors)<= {'section','heading_path','legal_disclosures'} and ('heading_path' not in anchors or anchors['heading_path']==['page']))
        if anchors.get('section')=='legal_disclosure':
            valid=({'section','heading_path','source_anchor','variant_id','parent_unit_id'} <= set(anchors) <= {'section','heading_path','source_anchor','variant_id','parent_unit_id','clause_id','legal_content_sha256','input_contract_sha256','occurrences'} and ('clause_id' not in anchors or text(anchors['clause_id'])) and anchors.get('parent_unit_id')=='page:1' and text(anchors.get('source_anchor')) and text(anchors.get('variant_id')) and anchors.get('heading_path')==['Legal disclosures',anchors['variant_id']])
    elif source_kind=='docx':valid=text(anchors.get('part')) and positive(anchors.get('paragraph_index'))
    elif source_kind=='pptx':valid=text(anchors.get('part')) and positive(anchors.get('slide_number')) and anchors.get('part_kind') in ('slide','notes')
    elif source_kind in ('txt','markdown'):valid=positive(anchors.get('line_start')) and positive(anchors.get('line_end'))
    elif source_kind=='csv':valid=positive(anchors.get('record_number'))
    else:
        import re
        cell=anchors.get('cell')
        valid=(positive(anchors.get('sheet_index')) and text(anchors.get('sheet_name')) and positive(anchors.get('row')) and positive(anchors.get('column')))
        if source_kind=='xlsx':valid=valid and isinstance(anchors.get('sheet_id'),str) and re.fullmatch('[1-9][0-9]*',anchors['sheet_id']) is not None and isinstance(cell,str) and re.fullmatch('[A-Z]+[1-9][0-9]*',cell) is not None
        if valid and cell is not None:
            letters='';number=anchors['column']
            while number:number,remainder=divmod(number-1,26);letters=chr(65+remainder)+letters
            valid=cell==letters+str(anchors['row'])
    if not valid:raise Conflict('Missing or conflicting typed source anchor')
    return anchors


def verify_web_acquisition(raw_root, proof, final_uri, raw_sha):
    """Verify retained HTTP acquisition identity; this never fetches a URL."""
    try:
        payload=verified_file(raw_root,proof['artifact_uri'],full_hash(proof['sha256']))
        acquisition=json.loads(payload,object_pairs_hook=_unique_object)
        rendering=acquisition.get('http',{}).get('rendering')
        if rendering:
            verified_file(raw_root,rendering['http_html_artifact_uri'],full_hash(rendering['http_html_sha256']))
            if rendering.get('representation')!='rendered-dom':raise Conflict('Unknown rendered acquisition representation')

        if (acquisition['schema_version'] not in ('web-acquisition.v1','web-acquisition.v2')
                or acquisition['final_uri']!=final_uri
                or full_hash(acquisition['raw_sha256'])!=raw_sha
                or acquisition['capture_complete'] is not True):
            raise Conflict('Retained web acquisition identity or completeness differs')
        if acquisition['schema_version']=='web-acquisition.v2' and (acquisition.get('asset_processing')!='deferred' or acquisition.get('asset_acquisition_complete') is not False):raise Conflict('Text-first acquisition must distinguish pending asset work')
        return acquisition
    except (KeyError,TypeError,ValueError,AttributeError) as error:
        raise Conflict('Incomplete retained web acquisition proof') from error
