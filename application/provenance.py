"""Bind native PDF extraction to a verified, newly persisted intake capture.

This adapter does not recover historical identities, infer page anchors, parse
PDFs, generate captions, alter vectors, or publish records. Publication retains
its separate strict provenance and full-vector admission checks.
"""
import copy
import hashlib
import math
import json
from datetime import datetime
from pathlib import Path

from collection_control.artifacts import canonical, _unique_object
from collection_control.provenance import full_hash, verified_file, typed_source_anchors, TYPED_SOURCE_KINDS, verify_web_acquisition
from collection_control.repository import Conflict


SCHEMA = 'unstructured-source-provenance.v1'


def _hash(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def _first(mappings, keys):
    for mapping in mappings:
        for key in keys:
            if mapping.get(key) is not None:
                return mapping[key]
    return None


def _bbox(value):
    if value is None:
        return None
    if (not isinstance(value, (list, tuple)) or len(value) != 4
            or any(isinstance(v, bool) or not isinstance(v, (int, float))
                   or not math.isfinite(v) for v in value)
            or value[0] > value[2] or value[1] > value[3]):
        raise Conflict('Invalid native page bounding box')
    return list(value)


def enrich_checkpoint(checkpoint: dict, revision: dict, *, raw_root: Path,
                      model_digest: str, config_digest: str,
                      ocr_model_digest: str, library_version='26.8.2') -> tuple[dict, dict]:
    """Return copied es_docs and a deterministic complete page-unit manifest.

    Revision identities must already have been persisted by intake. Empty pages
    retain explicit empty coverage; an empty source remains empty and must be
    handled by the caller rather than being admitted as a nonempty snapshot.
    """
    if revision.get('source_kind','pdf') != 'pdf':
        return _enrich_typed_checkpoint(checkpoint, revision, raw_root=raw_root,
            model_digest=model_digest, config_digest=config_digest, library_version=library_version)
    try:
        if checkpoint['schema_version'] != '1.0' or checkpoint['payload_format'] != 'es_docs':
            raise Conflict('Expected native es_docs checkpoint v1.0')
        if not isinstance(checkpoint['documents'], list):
            raise Conflict('Native documents must be a list')
        required = ('source_id', 'source_revision_id', 'collection_id', 'document_id',
                    'job_id', 'canonical_uri', 'raw_uri', 'ingestion_run_id', 'filename')
        for key in required:
            if not isinstance(revision[key], str) or not revision[key].strip():
                raise Conflict('Persisted intake capture requires ' + key)
        previous = revision.get('previous_source_revision_id')
        if previous is not None and (not isinstance(previous, str) or not previous):
            raise Conflict('Previous capture revision must be explicit')
        pages = revision['page_count']
        if isinstance(pages, bool) or not isinstance(pages, int) or pages < 1:
            raise Conflict('PDF capture requires a positive observed page count')
        observed = revision['observed_at']
        if isinstance(observed, datetime):
            observed = observed.isoformat()
        if not isinstance(observed, str) or datetime.fromisoformat(observed.replace('Z', '+00:00')).utcoffset() is None:
            raise Conflict('Capture observation must be timezone-aware')
        if not isinstance(library_version, str) or not library_version:
            raise Conflict('Native extraction library version is required')
        raw_sha = full_hash(revision['raw_sha256'])
        model_sha, config_sha = full_hash(model_digest), full_hash(config_digest)
        ocr_sha = full_hash(ocr_model_digest)
        verified_file(raw_root, revision['raw_uri'], raw_sha)
        completion=checkpoint['native_page_completions']
        completed_pages=completion['pages']
        if completion['schema_version']!='native-page-completions.v1' or not isinstance(completed_pages,list) or len(completed_pages)!=pages:
            raise Conflict('Native extraction page completion coverage is incomplete')
        page_ids=[]
        for item in completed_pages:
            number=item['page_number']
            if isinstance(number,bool) or not isinstance(number,int) or item.get('status')!='complete' or item.get('error'):
                raise Conflict('Native extraction page completion failed or has invalid identity')
            page_ids.append(number)
        if len(set(page_ids))!=len(page_ids) or set(page_ids)!=set(range(1,pages+1)):
            raise Conflict('Native observed pages differ from complete intake page manifest')
        manifest = {
            'schema_version': 'source-unit-manifest.v1',
            'document_id': revision['document_id'],
            'source_revision_id': revision['source_revision_id'],
            'canonical_uri': revision['canonical_uri'], 'raw_sha256': raw_sha,
            'native_page_completions_sha256':_hash(completion),
            'coverage': 'complete', 'expected_units': [f'page:{page}' for page in range(1, pages + 1)],
        }
        location=_location_reference(revision,raw_root)
        if location is not None:manifest['location_acquisition']=location
        manifest_sha = _hash(manifest)
        out = copy.deepcopy(checkpoint)
        out['document_id'] = revision['document_id']
        completed = {unit: [] for unit in manifest['expected_units']}
        for ordinal, doc in enumerate(out['documents']):
            if not isinstance(doc['text'], str):
                raise Conflict('Native source text must be a string')
            # Keep the native vector exactly as received, including its dtype and norm.
            if 'vector' not in doc:
                raise Conflict('Native source record requires its original vector')
            m = doc['metadata']
            if not isinstance(m, dict):
                raise Conflict('Native record metadata must be an object')
            cm = m.setdefault('content_metadata', {})
            if not isinstance(cm, dict):
                raise Conflict('Native content metadata must be an object')
            kind = cm.get('type', doc.get('_content_type', doc.get('content_type', 'text')))
            if not isinstance(kind, str) or not kind:
                raise Conflict('Native content type must be explicit')
            if 'caption' in kind.lower() or m.get('generated_annotation') or cm.get('generated_annotation'):
                raise Conflict('Generated captions require separate derived-source admission')
            page = _first((cm, m, doc), ('page_number', '_page_number'))
            if isinstance(page, bool) or not isinstance(page, int) or not 1 <= page <= pages:
                raise Conflict('Native record requires an observed in-range page number')
            box = _bbox(_first((cm, m, doc), ('bbox', 'bbox_xyxy_norm', '_bbox_xyxy_norm')))
            anchors = {'page_number': page}
            if box is not None:
                anchors['bbox'] = box
            text_sha = hashlib.sha256(doc['text'].encode('utf-8')).hexdigest()
            chunk = 'capture-chunk:' + _hash({
                'source_revision_id': revision['source_revision_id'], 'page': page,
                'content_type': kind, 'bbox': box, 'ordinal': ordinal, 'text_sha256': text_sha,
            })
            needs_ocr = _first((cm, m), ('needs_ocr_for_text',))
            has_text = _first((cm, m), ('has_text',))
            ocr = kind.lower() in ('table', 'chart', 'infographic', 'ocr') or bool(needs_ocr and not has_text)
            transforms = [{'component': 'document_extraction', 'version': library_version,
                           'config_digest': config_sha}]
            if ocr:
                transforms.append({'component': 'ocr', 'version': library_version,
                                   'config_digest': config_sha, 'model_digest': ocr_sha})
            transforms.append({'component': 'embedding', 'version': library_version,
                               'config_digest': config_sha, 'model_digest': model_sha})
            mirrors = {
                'source_id': revision['source_id'], 'source_revision_id': revision['source_revision_id'],
                'source_chunk_id': chunk, 'identity_origin': 'new_capture',
                'ingestion_run_id': revision['ingestion_run_id'],
                'collection_id': revision['collection_id'], 'document_id': revision['document_id'],
                'job_id': revision['job_id'],
                'source_system': 'document_capture', 'source_kind': 'pdf', 'modality': 'document',
                'canonical_uri': revision['canonical_uri'], 'final_uri': revision['canonical_uri'],
                'retrieved_at': observed,
            }
            p = {**mirrors, 'schema_version': SCHEMA, 'observed_at': observed,
                 'hashes': {'raw_sha256': raw_sha, 'text_sha256': text_sha},
                 'raw_artifact_uri': revision['raw_uri'], 'anchors': anchors, 'transforms': transforms}
            if previous is not None:
                p['previous_source_revision_id'] = previous
            original_source = copy.deepcopy(m.get('source', {}))
            m['native_source'] = original_source
            m['source'] = {**mirrors,
                           'source_uri': revision['canonical_uri'], 'source_name': revision['filename']}
            cm.update(mirrors, type=kind, page_number=page, provenance_schema_version=SCHEMA,
                      parser_version=library_version, raw_sha256=raw_sha, text_sha256=text_sha,
                      extraction_method='nemotron_ocr' if ocr else 'text_layer')
            m.update(mirrors)
            m.update(provenance=p, source_id=revision['source_id'],
                     source_revision_id=revision['source_revision_id'], source_chunk_id=chunk,
                     source_revision={**{k: v for k, v in mirrors.items() if k != 'source_chunk_id'},
                                      'raw_sha256': raw_sha},
                     source_chunk={**mirrors, 'text_sha256': text_sha, 'anchors': copy.deepcopy(anchors)},
                     identity_origin='new_capture', origin='grounded', acl=['local-operator'],
                     content_sha256=text_sha, durable_document_id=revision['document_id'])
            completed[f'page:{page}'].append(chunk)
        out['coverage'] = {'manifest_sha256': manifest_sha, 'completed_units': completed, 'failed_units': []}
        out['retained_source_capture'] = {
            'source_revision_id': revision['source_revision_id'], 'canonical_uri': revision['canonical_uri'],
            'raw_sha256': raw_sha, 'raw_artifact_uri': revision['raw_uri'],
            'observed_at': observed, 'ingestion_run_id': revision['ingestion_run_id'],
            'source_manifest': {'uri': revision.get('source_manifest_uri') or
                                (Path(raw_root).resolve() / (manifest_sha + '.json')).as_uri(),
                                'sha256': manifest_sha},
        }
        if previous is not None:
            out['retained_source_capture']['previous_source_revision_id'] = previous
        if location is not None:
            out['retained_source_capture']['location_acquisition']=copy.deepcopy(location)
            for doc in out['documents']:doc['metadata']['provenance']['location_acquisition']=copy.deepcopy(location)
        return out, manifest
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        raise Conflict('Incomplete or invalid native intake capture provenance') from error


def _enrich_typed_checkpoint(checkpoint, revision, *, raw_root, model_digest,
                             config_digest, library_version):
    """Bind one vector per retained nonempty structural unit to persisted intake."""
    try:
        if checkpoint['schema_version']!='1.0' or checkpoint['payload_format']!='es_docs' or not isinstance(checkpoint['documents'],list):
            raise Conflict('Expected native es_docs checkpoint v1.0')
        for key in ('source_id','source_revision_id','collection_id','document_id','job_id','canonical_uri','raw_uri','ingestion_run_id','filename'):
            if not isinstance(revision[key],str) or not revision[key].strip():raise Conflict('Persisted intake capture requires '+key)
        kind=revision['source_kind']
        if kind not in TYPED_SOURCE_KINDS:raise Conflict('Unqualified typed source kind')
        previous=revision.get('previous_source_revision_id')
        if previous is not None and (not isinstance(previous,str) or not previous):raise Conflict('Previous capture revision must be explicit')
        observed=revision['observed_at']
        if isinstance(observed,datetime):observed=observed.isoformat()
        if not isinstance(observed,str) or datetime.fromisoformat(observed.replace('Z','+00:00')).utcoffset() is None:raise Conflict('Capture observation must be timezone-aware')
        if not isinstance(library_version,str) or not library_version:raise Conflict('Embedding library version required')
        raw_sha=full_hash(revision['raw_sha256']);model_sha=full_hash(model_digest);config_sha=full_hash(config_digest)
        verified_file(raw_root,revision['raw_uri'],raw_sha)
        acquisition=None
        if kind=='html' and revision.get('acquisition') is not None:
            captured=revision['acquisition']
            if isinstance(captured,str):captured=json.loads(captured,object_pairs_hook=_unique_object)
            if captured is not None:
                acquisition={'artifact_uri':captured['uri'],'sha256':full_hash(captured['sha256'])}
                verify_web_acquisition(raw_root,acquisition,revision['canonical_uri'],raw_sha)
        native=revision['source_units']
        if isinstance(native,str):native=json.loads(native,object_pairs_hook=_unique_object)
        if canonical(native)!=canonical(checkpoint['native_source_units']):raise Conflict('Checkpoint source units differ from persisted intake')
        if native['schema_version']!='native-source-units.v1' or native['source_kind']!=kind:raise Conflict('Typed source unit schema/kind mismatch')
        transform=native['transform']
        if any(not isinstance(transform[key],str) or not transform[key].strip() for key in ('name','version')):raise Conflict('Typed extraction transform identity required')
        units=native['units']
        if not isinstance(units,list) or not units:raise Conflict('Typed capture requires explicit source units')
        indexed={};expected={}
        for unit in units:
            unit_id=unit['unit_id'];anchors=unit['anchors'];status=unit['status']
            if not isinstance(unit_id,str) or not unit_id.strip() or unit_id in indexed:raise Conflict('Source unit identities must be unique')
            if not isinstance(anchors,dict) or not anchors:raise Conflict('Source unit anchors required')
            if status=='excluded':
                if not isinstance(unit.get('reason'),str) or not unit['reason'].strip():raise Conflict('Excluded source unit requires an explicit reason')
            elif status=='complete':
                text=unit['text']
                if not isinstance(text,str):raise Conflict('Complete unit text must be a string')
                if text.strip():
                    typed_source_anchors(kind,anchors);expected[unit_id]=text
            else:raise Conflict('Failed or incomplete source unit')
            indexed[unit_id]=unit
        if kind=='html':
            from .html_disclosures import validate_html_units
            validate_html_units(native)
        office=checkpoint.get('office_visuals')
        if revision.get('office_visuals_required') and office is None:raise Conflict('Required Office visual evidence is missing')
        if office is not None:
            if kind not in ('docx','pptx'):raise Conflict('Office visuals on another format')
            from collection_control.office_visuals import verify_office_visuals
            for unit in verify_office_visuals(raw_root,office,raw_sha,native):
                if unit['unit_id'] in indexed:raise Conflict('Duplicate visual source unit')
                indexed[unit['unit_id']]=unit
                if unit['status']=='complete' and unit['text'].strip():expected[unit['unit_id']]=unit['text']
        materialization=checkpoint.get('web_materialization')
        if materialization is not None:
            if kind!='html':raise Conflict('Only whole webpages can consume image evidence')
            from collection_control.web_materialization import verify_materialization
            materialized_text=verify_materialization(raw_root,materialization,revision['source_revision_id'],units[0]['text'])
            if materialized_text.strip():expected['page:1']=materialized_text
            else:expected.pop('page:1',None)
        manifest={'schema_version':'source-unit-manifest.v1','document_id':revision['document_id'],
            'source_revision_id':revision['source_revision_id'],'canonical_uri':revision['canonical_uri'],
            'raw_sha256':raw_sha,'coverage':'complete','expected_units':list(indexed),
            'native_source_units':copy.deepcopy(native)}
        if materialization is not None:manifest['web_materialization']=copy.deepcopy(materialization)
        if office is not None:manifest['office_visuals']=copy.deepcopy(office)
        location=_location_reference(revision,raw_root)
        if location is not None:manifest['location_acquisition']=location
        manifest_sha=_hash(manifest);completed={unit:[] for unit in indexed};seen=set()
        out=copy.deepcopy(checkpoint);out['document_id']=revision['document_id']
        for doc in out['documents']:
            m=doc['metadata'];cm=m.setdefault('content_metadata',{});unit_id=cm['source_unit_id']
            if not isinstance(unit_id,str) or unit_id not in expected or unit_id in seen or doc['text']!=expected[unit_id] or 'vector' not in doc:raise Conflict('Typed record must exactly cover one retained nonempty source unit')
            if m.get('generated_annotation') or cm.get('generated_annotation') or 'caption' in str(cm.get('type','')).lower():raise Conflict('Generated text requires separate derived-source admission')
            if 'page_number' in cm or 'bbox' in cm or 'page_number' in m:raise Conflict('Typed source records cannot carry manufactured PDF anchors')
            seen.add(unit_id);unit=indexed[unit_id];anchors=copy.deepcopy(unit['anchors'])
            text_sha=hashlib.sha256(doc['text'].encode()).hexdigest()
            chunk='capture-chunk:'+_hash({'source_revision_id':revision['source_revision_id'],'source_unit_id':unit_id,'anchors':anchors,'text_sha256':text_sha})
            source_system='web_crawl' if kind=='html' else 'document_capture'
            modality='text' if kind in ('html','txt','markdown','csv') else 'document'
            mirrors={'source_id':revision['source_id'],'source_revision_id':revision['source_revision_id'],
                'source_chunk_id':chunk,'identity_origin':'new_capture','ingestion_run_id':revision['ingestion_run_id'],
                'collection_id':revision['collection_id'],'document_id':revision['document_id'],'job_id':revision['job_id'],
                'source_system':source_system,'source_kind':kind,'modality':modality,'canonical_uri':revision['canonical_uri'],
                'final_uri':revision['canonical_uri'],'retrieved_at':observed}
            transforms=[{'component':'html_parser' if kind=='html' else 'document_extraction',
                'name':transform['name'],'version':transform['version'],'config_digest':config_sha}]
            if kind=='html':transforms.append({'component':'page_unit','version':transform['version'],'config_digest':config_sha})
            transforms.append({'component':'embedding','version':library_version,'config_digest':config_sha,'model_digest':model_sha})
            provenance={**mirrors,'schema_version':SCHEMA,'observed_at':observed,
                'hashes':{'raw_sha256':raw_sha,'text_sha256':text_sha},'raw_artifact_uri':revision['raw_uri'],
                'anchors':anchors,'transforms':transforms,'source_unit_id':unit_id}
            if previous is not None:provenance['previous_source_revision_id']=previous
            if location is not None:provenance['location_acquisition']=copy.deepcopy(location)
            if acquisition is not None:provenance['acquisition']=copy.deepcopy(acquisition)
            if materialization is not None and unit_id=='page:1':provenance['web_materialization']=copy.deepcopy(materialization)
            if anchors.get('section')=='office_visual':
                provenance['office_visuals']=copy.deepcopy(office)
                transforms.extend([{'component':'office_render','name':office['renderer']['name'],'version':office['renderer']['version'],'config_digest':_hash(office['renderer'])},{'component':'native_visual_extraction','version':office['native']['library_version'],'model_digest':full_hash(office['native']['model_digest']),'config_digest':full_hash(office['native']['config_digest'])}])
            m['native_source']=copy.deepcopy(m.get('source',{}))
            m['source']={**mirrors,'source_uri':revision['canonical_uri'],'source_name':revision['filename'],'source_unit_id':unit_id}
            cm.update(mirrors,provenance_schema_version=SCHEMA,parser_version=transform['version'],raw_sha256=raw_sha,text_sha256=text_sha,extraction_method='retained_text')
            if kind=='html':cm['chunker_version']=transform['version']
            m.update(mirrors)
            if kind=='html' and transform['version'] in ('2','3','4'):
                from .html_disclosures import record_metadata
                m.update(record_metadata(anchors,revision['source_id'],revision['source_revision_id'],revision['canonical_uri']))
            m.update(provenance=provenance,source_revision={**{k:v for k,v in mirrors.items() if k!='source_chunk_id'},'raw_sha256':raw_sha},
                source_chunk={**mirrors,'source_unit_id':unit_id,'text_sha256':text_sha,'anchors':copy.deepcopy(anchors)},
                origin='grounded',acl=['local-operator'],content_sha256=text_sha,durable_document_id=revision['document_id'])
            completed[unit_id].append(chunk)
        if seen!=set(expected):raise Conflict('Typed source revision is missing retained nonempty units')
        out['coverage']={'manifest_sha256':manifest_sha,'completed_units':completed,'failed_units':[]}
        out['retained_source_capture']={'source_revision_id':revision['source_revision_id'],'canonical_uri':revision['canonical_uri'],
            'raw_sha256':raw_sha,'raw_artifact_uri':revision['raw_uri'],'observed_at':observed,'ingestion_run_id':revision['ingestion_run_id'],
            'source_manifest':{'uri':revision.get('source_manifest_uri') or (Path(raw_root).resolve()/(manifest_sha+'.json')).as_uri(),'sha256':manifest_sha}}
        if acquisition is not None:out['retained_source_capture']['acquisition']=copy.deepcopy(acquisition)
        if previous is not None:out['retained_source_capture']['previous_source_revision_id']=previous
        if location is not None:
            out['retained_source_capture']['location_acquisition']=copy.deepcopy(location)
            for doc in out['documents']:doc['metadata']['provenance']['location_acquisition']=copy.deepcopy(location)
        return out,manifest
    except (KeyError,TypeError,ValueError,AttributeError) as error:
        raise Conflict('Incomplete or invalid typed intake capture provenance') from error


def _location_reference(revision,raw_root):
    captured=revision.get('acquisition')
    if isinstance(captured,str):captured=json.loads(captured)
    if not isinstance(captured,dict) or captured.get('kind')!='location':return None
    ref={'artifact_uri':captured['uri'],'sha256':full_hash(captured['sha256'])}
    from collection_control.location_provenance import verify_location_capture
    verify_location_capture(raw_root,ref,revision['canonical_uri'],revision['raw_sha256'],revision['collection_id'])
    return ref
