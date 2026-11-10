"""Verify supplemental Office visuals without changing original structural coverage."""
from __future__ import annotations
import copy
import hashlib
from io import BytesIO
import json
import math
import posixpath
import zipfile
from PIL import Image, ImageOps
from .artifacts import canonical, _unique_object
from .provenance import verified_file, full_hash
from .repository import Conflict

SCHEMA='native-office-visuals.v1'
MAX_FRAMES=256
MAX_PIXELS=32_000_000
DPI=200
RENDERER={'name':'LibreOffice','version':'7.3.7.2 30(Build:2)',
    'binary_sha256':'53197e5ee66dc6d30384baac42eba080431d1d60ece478fb379a617fd007b1a2',
    'binary_path':'/usr/lib/libreoffice/program/soffice.bin',
    'options':['--headless','--convert-to','pdf'], 'private_profile':True,
    'timeout_seconds':60,'total_budget_seconds':240,
    'pptx_policy':'derived-copy-all-slides-visible', 'dpi':DPI,'max_pixels':MAX_PIXELS}


def sha(raw):return hashlib.sha256(raw).hexdigest()


def _preflight_word_fields(root):
    """Assemble field code runs within their actual complex-field boundaries.

    Nested fields in an unfinished instruction can synthesize arbitrary command
    text from their results, so that ambiguous case is rejected before rendering.
    Field nesting inside an already completed result remains supported.
    """
    import re
    from ingestion.document_formats import W
    active=[]
    def check(instruction):
        if not instruction.strip():raise ValueError('Empty Office field instruction is unsupported')
        if re.search(r'\b(?:DDEAUTO|DDE|INCLUDEPICTURE|INCLUDETEXT|LINK|DATABASE)\b',instruction,re.I):
            raise ValueError('External Office field instructions are unsupported')
    walk=[(root,None)]
    while walk:
        node,boundary=walk.pop()
        if boundary is not None:
            if tuple(id(field) for field in active)!=boundary:
                raise ValueError('Office complex field crosses a simple field boundary')
            continue
        local=node.tag.rsplit('}',1)[-1]
        if local in ('fldChar','instrText','fldSimple','delInstrText') and not node.tag.startswith('{'+W+'}'):
            raise ValueError('Unsupported Office field namespace')
        if node.tag==f'{{{W}}}fldSimple':
            if active and active[-1]['phase']=='instruction':
                raise ValueError('Nested Office field instruction is ambiguous')
            instruction=node.get(f'{{{W}}}instr')
            if instruction is None:raise ValueError('Office simple field instruction is missing')
            check(instruction)
            walk.append((node,tuple(id(field) for field in active)))
        elif node.tag==f'{{{W}}}fldChar':
            kind=node.get(f'{{{W}}}fldCharType')
            if kind=='begin':
                if active and active[-1]['phase']=='instruction':
                    raise ValueError('Nested Office field instruction is ambiguous')
                active.append({'phase':'instruction','pieces':[]})
            elif kind=='separate':
                if not active or active[-1]['phase']!='instruction':
                    raise ValueError('Office field separator is unbound or repeated')
                check(''.join(active[-1]['pieces']));active[-1]['phase']='result'
            elif kind=='end':
                if not active:raise ValueError('Office field end is unbound')
                field=active.pop()
                if field['phase']=='instruction':check(''.join(field['pieces']))
            else:raise ValueError('Unsupported Office field boundary')
        elif node.tag==f'{{{W}}}instrText':
            if not active or active[-1]['phase']!='instruction':
                raise ValueError('Office field instruction is unbound or follows its separator')
            if list(node):raise ValueError('Office field instruction contains unsupported child elements')
            active[-1]['pieces'].append(node.text or '')
        elif node.tag==f'{{{W}}}delInstrText':
            raise ValueError('Revision-dependent Office field instructions are unsupported')
        elif node.tag==f'{{{W}}}t' and active and active[-1]['phase']=='instruction':
            raise ValueError('Office field result precedes its instruction separator')
        walk.extend((child,None) for child in reversed(list(node)))
    if active:raise ValueError('Office complex field instruction is unclosed')


def office_inventory(raw,filename,source_units):
    """Re-inventory source bytes and map media occurrences to actual source units."""
    from ingestion.document_formats import inspect_document,_Package,W,R
    actual=inspect_document(filename,raw)
    native={'schema_version':'native-source-units.v1','source_kind':actual['source_kind'],'transform':actual['transform'],'units':actual['units']}
    if actual['source_kind'] not in ('docx','pptx') or canonical(native)!=canonical(source_units):
        raise ValueError('Office structural manifest differs from original bytes')
    package=_Package(raw)
    try:
        if any('vbaproject' in n.lower() or n.lower().endswith(('.vba','.bin')) for n in package.names):
            raise ValueError('Office macros or active binary parts are unsupported')
        if '[Content_Types].xml' in package.names:
            types=package.read('[Content_Types].xml').lower()
            if b'macroenabled' in types or b'vba' in types:raise ValueError('Office macros are unsupported')
        # Relationships may otherwise cause LibreOffice to fetch remote resources.
        for name in sorted(package.names):
            if not name.endswith('.rels'):continue
            base,leaf=posixpath.split(name)
            part=posixpath.join(posixpath.dirname(base),leaf[:-5]) if base.endswith('_rels') else ''
            if not part:
                root=package.xml(name)
                for relation in root:
                    if relation.get('TargetMode')=='External' and not relation.get('Type','').endswith('/hyperlink'):
                        raise ValueError('External Office package relationships are unsupported')
                continue
            for rel in package.relations(part).values():
                if rel['external'] and not rel['type'].endswith('/hyperlink'):
                    raise ValueError('External Office media/template relationships are unsupported')
        # Field instructions may span runs; never inspect fragments independently.
        for name in sorted(package.names):
            if name.endswith('.xml'):_preflight_word_fields(package.xml(name))
        units=[u for u in native['units'] if u['status']=='complete']
        slides=[u for u in units if u['anchors'].get('part_kind')=='slide']
        scope=[u['unit_id'] for u in units]
        media=[]
        for name in sorted(n for n in package.names if '/media/' in n and not n.endswith('/')):
            occurrences=[]
            for unit in units:
                part=unit['anchors']['part']
                relationships=package.relations(part)
                keys={k for k,r in relationships.items() if not r['external'] and r['target']==name}
                if not keys:continue
                root=package.xml(part)
                if native['source_kind']=='docx':
                    paragraphs=list(root.iter(f'{{{W}}}p'))
                    index=unit['anchors'].get('paragraph_index',0)
                    nodes=paragraphs[index-1:index] if index else []
                else:nodes=[root]
                if any(value in keys for node in nodes for element in node.iter() for key,value in element.attrib.items() if key.startswith('{'+R+'}')):
                    occurrences.append({'part':part,'precision':'paragraph' if native['source_kind']=='docx' else 'slide','unit_ids':[unit['unit_id']]})
            # Unreferenced retained raster parts still receive explicit document scope.
            if not occurrences:occurrences=[{'part':'word/document.xml' if native['source_kind']=='docx' else 'ppt/presentation.xml','precision':'document','unit_ids':scope}]
            data=package.read(name)
            media.append({'part':name,'sha256':sha(data),'size_bytes':len(data),'occurrences':occurrences})
        if len(media)+len(slides)>MAX_FRAMES:raise ValueError('Office visual frame count exceeds bound')
        return {'source_kind':native['source_kind'],'slides':[{'unit_id':u['unit_id'],'anchors':u['anchors']} for u in slides],
            'source_unit_ids':scope,'media':media,
            'visual_parts':[n for n in sorted(package.names) if '/charts/' in n or '/diagrams/' in n]}
    finally:package.zip.close()


def prepare_office(raw,inventory):
    """A retained derivative makes hidden PPTX slides visible, preserving their order."""
    if inventory['source_kind']=='docx':return raw
    import xml.etree.ElementTree as ET
    output=BytesIO()
    slide_parts={u['anchors']['part'] for u in inventory['slides']}
    with zipfile.ZipFile(BytesIO(raw)) as original,zipfile.ZipFile(output,'w',zipfile.ZIP_DEFLATED) as derivative:
        for info in original.infolist():
            data=original.read(info.filename)
            if info.filename in slide_parts:
                node=ET.fromstring(data);node.attrib.pop('show',None);data=ET.tostring(node,encoding='utf-8',xml_declaration=True)
            entry=zipfile.ZipInfo(info.filename,date_time=(1980,1,1,0,0,0));entry.compress_type=zipfile.ZIP_DEFLATED
            derivative.writestr(entry,data)
    return output.getvalue()


def normalized_png(raw):
    with Image.open(BytesIO(raw)) as image:
        if getattr(image,'n_frames',1)!=1:raise ValueError('Multi-frame Office media unsupported')
        if image.width<1 or image.height<1 or image.width*image.height>MAX_PIXELS:raise ValueError('Office image pixel bound exceeded')
        image=ImageOps.exif_transpose(image)
        if image.mode in ('RGBA','LA') or (image.mode=='P' and 'transparency' in image.info):
            rgba=image.convert('RGBA');result=Image.new('RGB',rgba.size,'white');result.paste(rgba,mask=rgba.getchannel('A'))
        else:result=image.convert('RGB')
        result.info.clear();output=BytesIO();result.save(output,format='PNG')
        return output.getvalue(),result.width,result.height


def raster_pages(raw):
    import pypdfium2 as pdfium
    document=pdfium.PdfDocument(raw)
    try:
        if not 0<len(document)<=MAX_FRAMES:raise ValueError('Office rendered page count exceeds bounds')
        for index in range(len(document)):
            page=document[index]
            try:
                width,height=page.get_size()
                if width*height*(DPI/72)**2>MAX_PIXELS:raise ValueError('Office rendered page pixel bound exceeded')
                bitmap=page.render(scale=DPI/72)
                try:
                    image=bitmap.to_pil().convert('RGB');image.info.clear();output=BytesIO();image.save(output,format='PNG')
                    yield output.getvalue(),image.width,image.height
                finally:bitmap.close()
            finally:page.close()
    finally:document.close()


def _error(value):
    if isinstance(value,dict):return bool(value.get('error')) or any(_error(v) for k,v in value.items() if k!='error')
    if isinstance(value,list):return any(_error(v) for v in value)
    return False


def visual_text(rows,kind):
    """Page OCR remains evidence; only visual-region text becomes supplemental text."""
    if not isinstance(rows,list) or len(rows)!=1 or not isinstance(rows[0],dict) or _error(rows):
        raise ValueError('Native Office image extraction failed or returned invalid frames')
    row=rows[0]
    if row.get('page_number',1)!=1:raise ValueError('Native Office frame identity differs')
    text=[]
    if kind=='embedded_media' and isinstance(row.get('text'),str) and row['text'].strip():text.append(row['text'])
    for category in ('table','chart','infographic','image'):
        entries=row.get(category) or []
        if not isinstance(entries,list):raise ValueError('Native Office visual output must be a collection')
        for entry in entries:
            if isinstance(entry,dict) and isinstance(entry.get('text'),str) and entry['text'].strip():
                if entry['text'] not in text:text.append(entry['text'])
    return '\n\n'.join(text)


def frame_unit(frame,rows):
    text=visual_text(rows,frame['kind'])
    if not text.strip():return None
    return {'unit_id':'office-visual:'+frame['frame_id'],'status':'complete','text':text,
        'anchors':{'section':'office_visual','part':frame['source']['part'],
            'source_scope':{k:copy.deepcopy(frame['source'][k]) for k in ('precision','unit_ids')}}}


def _binding(raw_root,binding):return verified_file(raw_root,binding['uri'],full_hash(binding['sha256']),binding.get('size_bytes'))


def verify_office_visuals(raw_root,proof,raw_sha,source_units):
    """Independently bind original structure, retained pixels and native evidence."""
    try:
        if proof['schema_version']!=SCHEMA or proof['status']!='complete' or full_hash(proof['original']['sha256'])!=full_hash(raw_sha):raise Conflict('Office source identity differs')
        if proof['source_units_sha256']!=sha(canonical(source_units)) or proof['renderer']!=RENDERER:raise Conflict('Office structural/renderer contract differs')
        original=_binding(raw_root,proof['original'])
        inventory=office_inventory(original,proof['filename'],source_units)
        if canonical(inventory)!=canonical(proof['inventory']):raise Conflict('Office visual inventory differs')
        prepared=_binding(raw_root,proof['prepared_source'])
        if prepared!=prepare_office(original,inventory):raise Conflict('Office hidden-slide derivative differs')
        empty=inventory['source_kind']=='pptx' and not inventory['slides'] and not inventory['media'] and not inventory['visual_parts']
        if empty and proof['rendered_pdf'] is not None:raise Conflict('Empty original presentation cannot invent rendered pages')
        pdf=None if empty else _binding(raw_root,proof['rendered_pdf'])
        def expected_frames():
            page_count=0
            for index,(data,width,height) in enumerate(() if empty else raster_pages(pdf),1):
                scope={'part':'word/document.xml','precision':'document','unit_ids':inventory['source_unit_ids']}
                if inventory['source_kind']=='pptx':
                    if index>len(inventory['slides']):raise Conflict('Office render has extra slides')
                    slide=inventory['slides'][index-1];scope={'part':slide['anchors']['part'],'precision':'slide','unit_ids':[slide['unit_id']]}
                yield ('page:'+str(index),'rendered_page',scope,data,width,height,{'page_number':index,'coordinate_frame':'rendered_page_pixels'})
                page_count=index
            if inventory['source_kind']=='pptx' and page_count!=len(inventory['slides']):raise Conflict('Office hidden/full slide render coverage differs')
            with zipfile.ZipFile(BytesIO(original)) as package:
                for media in inventory['media']:
                    data,width,height=normalized_png(package.read(media['part']))
                    for number,scope in enumerate(media['occurrences'],1):
                        yield ('media:'+media['part']+':'+str(number),'embedded_media',scope,data,width,height,{'package_part':media['part'],'coordinate_frame':'normalized_media_pixels'})
        if len(proof['frames'])>MAX_FRAMES:raise Conflict('Office visual frame coverage exceeds bounds')
        # Captured contracts remain verifiable after legitimate model rotation.
        # Future renderer upgrades must retain this accepted contract in an allowlist.
        native=proof['native'];models=native['models'];configuration=native['config']
        model='sha256:'+sha(canonical(models));config='sha256:'+sha(canonical(configuration))
        if native['model_digest']!=model or native['config_digest']!=config:
            raise Conflict('Office captured model/config hashes differ')
        if native['library_version']!='26.8.2' or configuration['library_version']!=native['library_version']:
            raise Conflict('Office native library contract unsupported')
        if set(models)!={'page-elements','ocr','table-structure'}:raise Conflict('Office extraction models incomplete')
        for captured in models.values():
            full_hash(captured['checkpoint_manifest_sha256']);full_hash(captured['platform_image_digest'])
        required={'extraction_mode':'image','method':'pdfium_hybrid','ocr_version':'v2','dpi':DPI,
            'extract_text':True,'extract_images':True,'extract_tables':True,'extract_charts':True,
            'extract_infographics':True,'use_page_elements':True,'use_table_structure':True,
            'caption':False,'embedding':False,'vdb_upload':False,'stage_order':[],'split_config':{}}
        if any(configuration.get(k)!=v for k,v in required.items()):raise Conflict('Office extraction configuration unsupported')
        normalization=configuration['normalization']
        if any(normalization.get(k)!=v for k,v in {'format':'PNG','mode':'RGB','exif_transpose':True,
                'alpha_background':'white','multi_frame':'reject','max_pixels':MAX_PIXELS}.items()):
            raise Conflict('Office captured image normalization unsupported')
        output=[]
        from itertools import zip_longest
        for frame,expected in zip_longest(proof['frames'],expected_frames()):
            if frame is None or expected is None:raise Conflict('Office visual frame coverage differs')
            identity,kind,scope,data,width,height,derivative=expected
            if frame['frame_id']!=identity or frame['kind']!=kind or frame['source']!=scope or frame['derivative']!=derivative or frame['width']!=width or frame['height']!=height:raise Conflict('Office source/derivative anchor mapping differs')
            if _binding(raw_root,frame['raster'])!=data:raise Conflict('Office retained pixels differ from source derivative')
            evidence=json.loads(_binding(raw_root,frame['evidence']),object_pairs_hook=_unique_object)
            if evidence['schema_version']!='native-office-frame.v1' or evidence['frame_id']!=identity or evidence['raster_sha256']!=sha(data) or evidence['model_digest']!=model or evidence['config_digest']!=config:raise Conflict('Office native evidence input/contract differs')
            unit=frame_unit(frame,evidence['rows'])
            if unit is not None:output.append(unit)
        if sha(canonical(output))!=proof['visual_units_sha256']:raise Conflict('Office visual text differs from retained native outputs')
        return output
    except (KeyError,TypeError,ValueError,AttributeError,OSError) as error:
        if isinstance(error,Conflict):raise
        raise Conflict('Incomplete or invalid Office visual derivation proof') from error
