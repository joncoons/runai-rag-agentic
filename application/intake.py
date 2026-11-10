"""New-capture intake: original bytes and identities precede any model work."""
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4,uuid5,NAMESPACE_URL
from collection_control.artifacts import canonical
from collection_control.repository import Conflict
from nv_ingest_durable.contracts import Stage,StageEnvelope
from nv_ingest_durable.deferred_caption import retain

APP='runai-rag-agentic'
PIPELINE='agentic-rag-native-v1'


def hash_value(value):return hashlib.sha256(canonical(value)).hexdigest()


def embedding_config(settings):
    return hash_value({'model':settings.models['models']['embed']['model'],'checkpoint':settings.contract['embedding_checkpoint_sha256'],'dimensions':2048,'modality':'text','input_type':'passage','truncate':'NONE','library':'26.8.2'})


def pdf_page_count(raw):
    import pypdfium2 as pdfium
    try:
        document=pdfium.PdfDocument(raw)
        try:
            pages=len(document)
            if pages<1:raise ValueError('PDF contains no pages')
            return pages
        finally:document.close()
    except Exception as error:raise ValueError('Unsupported or invalid PDF document') from error


def inspect_upload(filename, raw, *, captions_enabled=False):
    from ingestion.document_formats import inspect_document
    if not filename or len(filename)>255 or '/' in filename or '\\' in filename or any(ord(c)<32 for c in filename):
        raise ValueError('Choose filenames without directory paths')
    if not raw or len(raw)>128*1024*1024:raise ValueError('Empty or oversized document')
    if filename.lower().endswith('.pdf'):
        return {'source_kind':'pdf','media_type':'application/pdf','page_count':pdf_page_count(raw),'source_units':None}
    if captions_enabled:
        raise ValueError('Image captions are currently qualified for PDF uploads only; disable captions for structural formats')
    result=inspect_document(filename,raw)
    return {'source_kind':result['source_kind'],'media_type':result['media_type'],'page_count':None,
            'source_units':{'schema_version':'native-source-units.v1','source_kind':result['source_kind'],
                'transform':result['transform'],'units':result['units']}}


def inspect_linked_image(raw,media_type):
    from PIL import Image
    from io import BytesIO
    try:
        if media_type=='image/svg+xml':
            from .svg_images import validate_svg
            validate_svg(raw)
        else:
            with Image.open(BytesIO(raw)) as image:
                if image.width*image.height>32000000 or getattr(image,'n_frames',1)!=1:raise ValueError('Unqualified image frame/pixel count')
                image.verify()
    except (OSError,SyntaxError,ValueError,Image.DecompressionBombError) as error:
        raise ValueError('Invalid or unsupported linked image') from error
    return {'source_kind':'web_image','media_type':media_type,'page_count':None,'source_units':None}


async def submit(context,collection_id,uploads,*,captions_enabled=False,_web_captures=None,_web_group=None,_refresh=None,_streamed=False,_inspections=None):
    if not isinstance(captions_enabled,bool):raise ValueError('captions_enabled must be boolean')
    settings=context.settings;job=uuid4();capture=str(uuid4());observed=datetime.now(timezone.utc)
    config=embedding_config(settings);prepared=[];seen=set()
    for filename,raw in uploads:
        captured=(_web_captures or {}).get(filename)
        cached=(_inspections or {}).get(filename)
        if cached is not None:
            if cached['raw_sha256']!=hashlib.sha256(raw).hexdigest():raise ValueError('Preflight inspection input changed')
            inspection=__import__('copy').deepcopy(cached['inspection'])
        elif captured and captured.get('role')=='image':
            inspection=await __import__('asyncio').to_thread(inspect_linked_image,raw,captured['media_type'])
        else:inspection=await __import__('asyncio').to_thread(inspect_upload,filename,raw,captions_enabled=captions_enabled and (captured is None or filename.lower().endswith('.pdf')))
        if inspection['source_kind']=='html' and inspection['source_units']['transform']['version']=='4' and any(u['anchors'].get('section')=='legal_disclosure' for u in inspection['source_units']['units']):
            from .legal_text_budget import load_budget
            if load_budget().checkpoint!=settings.contract['embedding_checkpoint_sha256']:raise Conflict('HTML tokenizer budget differs from the configured embedding checkpoint')
        source_sha=hashlib.sha256(raw).hexdigest()
        source_uri=captured['canonical_uri'] if captured else 'upload://'+collection_id+'/'+quote(filename,safe='')
        dedupe=source_uri if captured else source_sha
        if dedupe in seen:continue
        seen.add(dedupe)
        source_id='source:'+hashlib.sha256((collection_id+'\0'+source_uri).encode()).hexdigest()
        revision='capture:'+uuid4().hex;document='sha256:'+hashlib.sha256((str(job)+'\0'+source_uri+'\0'+source_sha).encode()).hexdigest()
        raw_uri,_=retain(settings.raw,raw,Path(filename).suffix.lower())
        key='sha256:'+hash_value({'job':str(job),'document':document,'source':source_sha,'captions_enabled':captions_enabled,'pipeline':PIPELINE})
        envelope=StageEnvelope(job_id=str(job),document_id=document,stage=Stage.EXTRACTION,artifact_uri=raw_uri,idempotency_key=key,pipeline_version=PIPELINE,config_digest='sha256:'+config,model_digest='sha256:'+settings.contract['embedding_checkpoint_sha256'])
        if captured and captured.get('role')=='image':
            from dataclasses import replace
            from .image_extraction import image_contract
            md,cd=image_contract();envelope=replace(envelope,model_digest=md,config_digest=cd)
        prepared.append({'source_id':source_id,'source_revision_id':revision,'document_id':document,'canonical_uri':source_uri,'filename':filename,'raw_sha256':source_sha,'raw_uri':raw_uri,**inspection,'acquisition':captured.get('acquisition') if captured else None,'web_role':captured.get('role','page') if captured else None,'envelope':envelope})
    if not prepared:raise ValueError('No unique document uploads')
    group=None
    if _web_group is not None:
        by_name={item['filename']:item for item in prepared}
        members=[{k:item[k] for k in ('document_id','source_revision_id','canonical_uri')}|{'role':item['web_role']} for item in prepared]
        pages=[]
        for entry in _web_group['pages']:
            item=by_name[entry['filename']];assets=[]
            for relation in entry['assets']:
                child=by_name[relation['filename']]
                assets.append({k:child[k] for k in ('document_id','source_revision_id','canonical_uri','raw_uri','raw_sha256','media_type')}|{'model_digest':child['envelope'].model_digest,'config_digest':child['envelope'].config_digest}|{k:v for k,v in relation.items() if k!='filename'})
            pages.append({k:item[k] for k in ('document_id','source_revision_id','canonical_uri')}|({'relations':entry['relations']} if _web_group.get('schema_version')=='web-page-group.v2' else {'assets':assets}))
        manifest={'schema_version':_web_group.get('schema_version','web-asset-group.v1'),'group_id':str(uuid4()),'collection_id':collection_id,'job_id':str(job),'pages':pages,'members':members,'coverage':'complete','policy':_web_group['policy']}
        uri,sha=retain(settings.raw,canonical(manifest),'.web-group.json');group={'manifest':manifest,'uri':uri,'sha256':sha}
    async with context.pool.acquire() as c,c.transaction():
        col=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',collection_id)
        if not col or col['deleted']:raise Conflict('Collection is unavailable')
        # v2 intake grants queue/discovery authority only. Full graph/source
        # validation belongs to fenced publication admission, not acquisition.
        if group and group['manifest']['schema_version']!='web-page-group.v2':
            from collection_control.web_groups import resolve_graph,merge_graph
            merge_graph(await resolve_graph(c,col),group['manifest'])
        metadata={'application':APP,'acl':['local-operator'],'ingestion_run_id':capture,'ingestion_options':{'captions_enabled':captions_enabled,'office_visuals_enabled':any(i['source_kind'] in ('docx','pptx') for i in prepared)},'source_manifests':{}}
        if _refresh is not None:metadata['source_run_id']=str(_refresh['run_id'])
        if any(isinstance(i.get('acquisition'),dict) and i['acquisition'].get('kind')=='location' for i in prepared):metadata['ingestion_options']['connector_kind']='location'
        await c.execute("INSERT INTO public.jobs(job_id,corpus_namespace,status,pipeline_version,metadata) VALUES($1,$2,'accepted',$3,$4::jsonb)",job,collection_id,PIPELINE,json.dumps(metadata))
        await c.execute('INSERT INTO application.jobs(job_id,collection_id) VALUES($1,$2)',job,collection_id)
        for item in prepared:
            await c.execute('INSERT INTO application.sources(source_id,collection_id,canonical_uri) VALUES($1,$2,$3) ON CONFLICT DO NOTHING',item['source_id'],collection_id,item['canonical_uri'])
            previous=await c.fetchval('SELECT current_revision_id FROM application.sources WHERE source_id=$1',item['source_id'])
            await c.execute("INSERT INTO public.documents(document_id,job_id,source_uri,source_digest,status) VALUES($1,$2,$3,$4,'accepted')",item['document_id'],job,item['canonical_uri'],'sha256:'+item['raw_sha256'])
            await c.execute('''INSERT INTO application.source_revisions(source_revision_id,source_id,collection_id,document_id,job_id,canonical_uri,filename,raw_sha256,raw_uri,observed_at,ingestion_run_id,page_count,previous_source_revision_id,source_kind,media_type,source_units,acquisition,web_group_id,web_role)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb,$17::jsonb,$18,$19)''',item['source_revision_id'],item['source_id'],collection_id,item['document_id'],job,item['canonical_uri'],item['filename'],item['raw_sha256'],item['raw_uri'],observed,capture,item['page_count'],previous,item['source_kind'],item['media_type'],json.dumps(item['source_units']),json.dumps(item['acquisition']),__import__('uuid').UUID(group['manifest']['group_id']) if group else None,item['web_role'])
            if item['source_kind'] in ('docx','pptx'):
                await c.execute('UPDATE application.source_revisions SET office_visuals_required=true WHERE source_revision_id=$1',item['source_revision_id'])
            envelope=item['envelope']
            await c.execute("INSERT INTO public.stage_units(idempotency_key,job_id,document_id,stage,status,input_envelope) VALUES($1,$2,$3,'extraction','pending',$4::jsonb)",envelope.idempotency_key,job,item['document_id'],json.dumps(envelope.to_dict()))
            await c.execute('INSERT INTO public.outbox_events(event_id,idempotency_key,target_stream,envelope) VALUES($1,$2,$3,$4::jsonb)',uuid4(),envelope.idempotency_key,('rag.agentic.pages.extraction.pending' if item['web_role']=='page' else 'rag.agentic.images.extraction.pending' if item['web_role']=='image' else 'rag.agentic.binaries.extraction.pending') if _streamed else 'rag.agentic.extraction.pending',json.dumps(envelope.to_dict()))
        if group:
            from collection_control.web_groups import register_group
            await register_group(c,group_id=__import__('uuid').UUID(group['manifest']['group_id']),collection_id=collection_id,job_id=job,manifest_uri=group['uri'],manifest_sha256=group['sha256'],members=group['manifest']['members'])
        if _refresh is not None:
            from .source_registry import SourceRegistry
            await SourceRegistry(context.pool).accept(c,**_refresh,job_id=job)
        await c.execute("INSERT INTO application.events(job_id,event_type,payload) VALUES($1,'intake_committed',$2::jsonb)",job,json.dumps({'documents':len(prepared),'captions_enabled':captions_enabled}))
    return str(job)


def acquisition_evidence(page,raw_root):
    import copy
    evidence=copy.deepcopy(page.evidence)
    if page.http_raw_bytes is not None:
        uri,sha=retain(raw_root,page.http_raw_bytes,'.http.html')
        expected=evidence['rendering']['http_html_sha256'].removeprefix('sha256:')
        if sha.removeprefix('sha256:')!=expected:raise Conflict('Original HTTP response checksum differs')
        evidence['rendering']['http_html_artifact_uri']=uri
    return evidence


async def submit_web(context,collection_id,capture,policy,*,_acquired_assets=None,_refresh=None,captions_enabled=False):
    """Acknowledge only after all complete fetched pages enter durable intake.

    Acquisition is bounded request work; the acknowledged extraction/publication
    thereafter uses the existing PostgreSQL/Redis outbox and retry mechanism.
    Discovered asset relations are retained evidence, not ingestion receipts.
    """
    from dataclasses import asdict,replace
    if not capture.complete or capture.errors or not capture.pages:
        raise ValueError('Only complete nonempty bounded captures can be accepted')
    from .web_asset_acquisition import acquire_assets
    asset_captures=await acquire_assets(capture,replace(policy,max_response_bytes=policy.max_asset_response_bytes)) if _acquired_assets is None else _acquired_assets
    expected={(a.parent_url,a.url,a.kind,a.locator) for page in capture.pages for a in page.assets}
    acquired={(a.parent_url,a.requested_uri,a.kind,a.locator) for a in asset_captures}
    if expected!=acquired:raise ValueError('Asset acquisition coverage differs from selected page relationships')
    uploads=[];captures={};final_pages={};group_pages=[];asset_names={}
    for asset in asset_captures:
        name=hashlib.sha256(asset.requested_uri.encode()).hexdigest()+Path(asset.filename).suffix.lower()
        if name in asset_names:
            if asset_names[name].raw_sha256!=asset.raw_sha256:raise ValueError('Conflicting bytes for one linked asset')
            continue
        asset_names[name]=asset
        captures[name]={'canonical_uri':asset.requested_uri,'role':asset.kind,'media_type':asset.media_type}
        uploads.append((name,asset.raw_bytes))
    for page in capture.pages:
        raw_sha=hashlib.sha256(page.raw_bytes).hexdigest()
        if page.raw_sha256.removeprefix('sha256:')!=raw_sha:raise ValueError('Acquired page raw checksum mismatch')
        if page.final_uri in final_pages:
            if final_pages[page.final_uri]!=raw_sha:raise ValueError('Conflicting captures resolve to the same canonical webpage URL')
            continue
        final_pages[page.final_uri]=raw_sha
        filename=hashlib.sha256(page.final_uri.encode()).hexdigest()+'.html'
        inspected=inspect_upload(filename,page.raw_bytes,captions_enabled=False)
        units=inspected['source_units']['units']
        if units[0].get('text')!=page.text:
            raise ValueError('Captured whole-page text differs from qualified normalization')
        relations=[];seen_relations=set()
        for asset in asset_captures:
            relation_key=(asset.parent_url,asset.requested_uri,asset.kind,asset.locator)
            if asset.parent_url==page.final_uri and relation_key not in seen_relations:
                seen_relations.add(relation_key)
                name=hashlib.sha256(asset.requested_uri.encode()).hexdigest()+Path(asset.filename).suffix.lower()
                relations.append({'filename':name,'kind':asset.kind,'requested_uri':asset.requested_uri,'final_uri':asset.final_uri,'locator':asset.locator,'alt':asset.alt,'http':asset.evidence})
        group_pages.append({'filename':filename,'assets':relations})
        manifest={'schema_version':'web-acquisition.v1','requested_uri':page.requested_uri,'final_uri':page.final_uri,
            'raw_sha256':raw_sha,'http':acquisition_evidence(page,context.settings.raw),'policy':asdict(policy),'assets':[asdict(a) for a in page.assets],
            'asset_processing':'native_ingestion','asset_acquisition_complete':True,'capture_complete':True,'deletion_authorized':False}
        encoded=canonical(manifest);uri,sha=retain(context.settings.raw,encoded,'.web-capture.json')
        captures[filename]={'canonical_uri':page.final_uri,'acquisition':{'uri':uri,'sha256':sha},'role':'page'}
        uploads.append((filename,page.raw_bytes))
    return await submit(context,collection_id,uploads,captions_enabled=captions_enabled,_web_captures=captures,_web_group={'pages':group_pages,'policy':asdict(policy)},_refresh=_refresh)


async def submit_web_text(context,collection_id,capture,policy,*,observation_id,captions_enabled=False,_source_run=None):
    """Commit each HTML page and its deferred acquisition intents atomically.

    Captures are individual complete page observations, not claims that a whole
    crawl or its assets completed. No network, image, binary or model work occurs.
    """
    from dataclasses import asdict
    from types import SimpleNamespace
    from collection_control.pinned_connection import PinnedPool
    from .deferred_web_assets import DeferredWebAssets,download_identity
    if not isinstance(captions_enabled,bool):raise ValueError('captions_enabled must be boolean')
    if not capture.pages:raise ValueError('No complete HTML pages supplied')
    policy_dict=asdict(policy);policy_dict.pop('private_hosts',None);policy_dict.pop('private_ports',None)
    from ingestion import document_formats
    from . import html_disclosures
    identity=hash_value({'html_disclosures_sha256':hashlib.sha256(Path(html_disclosures.__file__).read_bytes()).hexdigest(),'embedding':embedding_config(context.settings),'policy':policy_dict,'captions_enabled':captions_enabled,'schema':'web-page-group.v2','pipeline':PIPELINE,'intake_adapter_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'parser_adapter_sha256':hashlib.sha256(Path(document_formats.__file__).read_bytes()).hexdigest()})
    observation_id=collection_id+':'+str(observation_id)
    jobs=[]
    for page in capture.pages:
        raw_sha=hashlib.sha256(page.raw_bytes).hexdigest()
        if page.raw_sha256.removeprefix('sha256:')!=raw_sha:raise ValueError('Acquired page checksum differs')
        filename=hashlib.sha256(page.final_uri.encode()).hexdigest()+'.html'
        inspected=inspect_upload(filename,page.raw_bytes)
        if inspected['source_units']['units'][0]['text']!=page.text:raise ValueError('Whole page normalization differs')
        relations=[{'request_id':str(download_identity(observation_id,a.url,a.kind,policy_dict)),'canonical_uri':a.url,'kind':a.kind,'locator':a.locator,'alt':a.alt} for a in page.assets]
        manifest={'schema_version':'web-acquisition.v2','requested_uri':page.requested_uri,'final_uri':page.final_uri,'raw_sha256':raw_sha,
            'http':acquisition_evidence(page,context.settings.raw),'policy':policy_dict,'assets':[asdict(a) for a in page.assets],
            'asset_processing':'deferred','asset_acquisition_complete':False,'capture_complete':True,'deletion_authorized':False}
        uri,sha=retain(context.settings.raw,canonical(manifest),'.web-capture.json')
        async with context.pool.acquire() as c,c.transaction():
            col=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',collection_id)
            if not col or col['deleted']:raise Conflict('Collection is unavailable')
            if _source_run is not None:
                from .source_registry import SourceRegistry
                await SourceRegistry(context.pool)._leased(c,_source_run['run_id'],_source_run['fence'],require_deadline=True)
            previous=await c.fetchrow('SELECT * FROM application.web_page_admissions WHERE observation_id=$1 AND collection_id=$2 AND canonical_uri=$3',observation_id,collection_id,page.final_uri)
            if previous:
                if previous['raw_sha256']!=raw_sha or previous['processing_identity']!=identity:raise Conflict('Immutable page observation changed')
                jobs.append(str(previous['job_id']));continue
            pinned=SimpleNamespace(settings=context.settings,pool=PinnedPool(c))
            job=await submit(pinned,collection_id,[(filename,page.raw_bytes)],captions_enabled=captions_enabled,
                _web_captures={filename:{'canonical_uri':page.final_uri,'role':'page','acquisition':{'uri':uri,'sha256':sha}}},
                _web_group={'schema_version':'web-page-group.v2','pages':[{'filename':filename,'assets':[],'relations':relations}],'policy':policy_dict},_streamed=True)
            revision=await c.fetchrow('SELECT * FROM application.source_revisions WHERE job_id=$1',__import__('uuid').UUID(job))
            await DeferredWebAssets(context.pool,context.settings.raw).discover(c,observation_id=observation_id,collection_id=collection_id,
                parent_source_revision_id=revision['source_revision_id'],parent_document_id=revision['document_id'],parent_job_id=job,
                policy=policy_dict,assets=page.assets,captions_enabled=captions_enabled,
                source_run_id=_source_run['run_id'] if _source_run else None,source_version=_source_run['source_version'] if _source_run else None)
            await c.execute('INSERT INTO application.web_page_admissions(observation_id,collection_id,canonical_uri,raw_sha256,processing_identity,job_id,parent_source_revision_id) VALUES($1,$2,$3,$4,$5,$6,$7)',observation_id,collection_id,page.final_uri,raw_sha,identity,__import__('uuid').UUID(job),revision['source_revision_id'])
            if _source_run is not None:
                await c.execute('INSERT INTO application.website_run_jobs(run_id,job_id) VALUES($1,$2) ON CONFLICT DO NOTHING',_source_run['run_id'],__import__('uuid').UUID(job))
            jobs.append(job)
    return jobs
