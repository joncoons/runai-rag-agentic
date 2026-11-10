"""Deterministic explicit legal sections; preserve every audience's source text."""
import hashlib
from bs4 import BeautifulSoup

LEGAL_SELECTORS='section#disclaimer, section.disclaimer, section.legal-disclaimer, #legal-disclaimers, [role="doc-endnotes"]'

def _legal_text(soup):
    # Preserve caption and out-of-cell qualifications as well as row cells.
    # The ordinary crawler table helper deliberately serializes only rows and
    # cannot serve as a complete legal-table normalizer.
    from bs4 import NavigableString
    for node in soup.find_all(['script','style','noscript','template']):node.decompose()
    def table_lines(node):
        if isinstance(node,NavigableString):return [str(node)] if str(node).strip() else []
        if node.name=='tr':
            cells=node.find_all(['td','th'],recursive=False)
            extras=[c for c in node.contents if (isinstance(c,NavigableString) and str(c).strip()) or (not isinstance(c,NavigableString) and c.name not in ('td','th'))]
            if extras:return [node.get_text('\n',strip=True)]
            values=[' '.join(c.get_text(' ',strip=True).split()).replace('|','\\|') for c in cells]
            return ['| '+' | '.join(values)+' |'] if values else []
        if node.name=='caption':return [node.get_text('\n',strip=True)]
        return [line for child in node.contents for line in table_lines(child)]
    for table in list(soup.find_all('table')):
        if table.parent is not None:table.replace_with(soup.new_string('\n'.join(table_lines(table))))
    return '\n'.join(line for line in (' '.join(v.split()) for v in soup.get_text('\n',strip=True).splitlines()) if line)


def normalize_legal_text(text):
    import unicodedata
    return '\n'.join(line for line in (' '.join(v.split()) for v in unicodedata.normalize('NFC',text).splitlines()) if line)


def _canonical_legal(node):
    return normalize_legal_text(_legal_text(BeautifulSoup(str(node),'html.parser')))


def _source_parts(group):
    """Lossless structural cuts, only used when the complete section cannot fit."""
    import re
    clone=BeautifulSoup(str(group),'html.parser');root=clone.find(True)
    candidates=[]
    for node in root.find_all(id=re.compile('^legal-',re.I)):
        if node.select_one('[id^=legal-]'):continue
        table=node.find_parent('table')
        cut=table if table is not None and table in root.descendants else node
        if cut is not root and not any(cut is n for n in candidates):candidates.append(cut)
    if not candidates:
        candidates=[n for n in root.find_all(['p','li','table','h1','h2','h3','h4'],recursive=True)
                    if not any(p.name in ('p','li','table') for p in n.parents if p is not root)]
    # Ancestor tables/paragraphs are indivisible. Avoid nested cut markers.
    candidates=[n for n in candidates if not any(p is other for p in n.parents for other in candidates)]
    material=str(root);salt=hashlib.sha256(material.encode()).hexdigest()
    prefix='LEGAL_BOUNDARY_'+salt+'_'
    while prefix in material:prefix+='X'
    for index,node in enumerate(candidates):node.insert_before(clone.new_string('\n'+prefix+str(index)+'\n'))
    marked=_canonical_legal(clone)
    parts=[p.strip() for p in re.split(re.escape(prefix)+r'\d+',marked) if p.strip()]
    original=_canonical_legal(group)
    if '\n'.join(parts)!=original:raise ValueError('Legal structural split failed complete source reconstruction')
    return parts or [original]


def _pack_group(group,budget):
    text=_canonical_legal(group)
    if not text:return []
    if budget.fits(text):return [text]
    packed=[];current=''
    for part in _source_parts(group):
        if not budget.fits(part):raise ValueError('Indivisible legal source block exceeds model/service input limits; no text truncated')
        combined=current+'\n'+part if current else part
        if current and not budget.fits(combined):packed.append(current);current=part
        else:current=combined
    if current:packed.append(current)
    if '\n'.join(packed)!=text:raise ValueError('Legal packing failed complete source reconstruction')
    return packed


def html_units(raw,*,budget=None):
    """One vector per complete legal section; exact duplicates retain occurrences."""
    import re
    from .web_acquisition import normalize_html_text_legacy
    soup=BeautifulSoup(raw,'html.parser');units={};references=[];controls=[]
    # Promote empty author anchors to their explicit containing/immediate block.
    container_ids={id(n) for n in soup.select(LEGAL_SELECTORS)}
    for node in list(soup.find_all(id=re.compile(r'^legal-',re.I))):
        if id(node) in container_ids or node.get_text(strip=True):continue
        target=node.parent if node.parent and node.parent.name in ('p','li','td','th') else node.find_next_sibling()
        if target is None or (target is not node.parent and target.name!='p'):continue
        wrapper=soup.new_tag('div',id=node['id']);target.wrap(wrapper);del node['id']
        if node not in target.descendants:wrapper.insert(0,node.extract())
    candidates=soup.select(LEGAL_SELECTORS)
    candidates += [n for n in soup.find_all(id=re.compile(r'^(?:legal|disclaimer)-',re.I)) if not any(n is c for c in candidates)]
    sections=[n for n in candidates if not any(p is c for p in n.parents for c in candidates)]
    for index,section in enumerate(sections,1):
        section_anchor='#'+section['id'] if section.get('id') else 'legal-section:'+str(index)
        clone=BeautifulSoup(str(section),'html.parser');root=clone.find(True)
        # Exclude semantic controls, not text phrases that might be real terms.
        if root.name in ('button','input','select','textarea') or (root.name=='a' and (root.get('role')=='button' or 'collapse-toggle' in root.get('class',[]))):
            controls.append({'kind':'legal_ui_control','tag':root.name,'source_anchor':section_anchor,'text_sha256':hashlib.sha256(root.get_text().encode()).hexdigest(),'reason':'structural_control_not_legal_content'});section.decompose();continue
        for node in list(root.find_all(['button','input','select','textarea'])):
            controls.append({'kind':'legal_ui_control','tag':node.name,'source_anchor':section_anchor,'text_sha256':hashlib.sha256(node.get_text().encode()).hexdigest(),'reason':'structural_control_not_legal_content'});node.decompose()
        for node in list(root.select('a[role="button"],a.collapse-toggle')):
            controls.append({'kind':'legal_ui_control','tag':'a','source_anchor':section_anchor,'text_sha256':hashlib.sha256(node.get_text().encode()).hexdigest(),'reason':'structural_control_not_legal_content'});node.decompose()
        variants=[]
        excluded={'disclaimer-text','disclaimer-content','disclaimer-list','disclaimer-section','disclaimer-container','disclaimer-wrapper'}
        for node in [root,*root.find_all(True)]:
            names=[c for c in node.get('class',[]) if c.startswith('disclaimer-') and c not in excluded]
            if names:variants.append((node,names[0]))
        outer_variants=[v for v in variants if not any(p is other[0] for p in v[0].parents for other in variants)]
        groups=list(outer_variants) if outer_variants else [(root,'all')]
        if outer_variants and not any(root is v[0] for v in outer_variants):
            shared=BeautifulSoup(str(root),'html.parser');shared_root=shared.find(True)
            for node in list(shared_root.find_all(True)):
                if node.parent is not None and any(c.startswith('disclaimer-') and c not in excluded for c in node.get('class',[])):node.decompose()
            if _canonical_legal(shared_root):groups.append((shared_root,'all'))
        for group_index,(group,variant_id) in enumerate(groups,1):
            text=_canonical_legal(group)
            if not text:continue
            if budget is None:
                from .legal_text_budget import load_budget
                budget=load_budget()
            group_id='group:'+str(index)+':'+str(group_index)
            locator=section_anchor+'/'+variant_id
            clause_anchors=list(dict.fromkeys('#'+n['id'] for n in [group,*group.find_all(id=True)] if n.get('id','').startswith('legal-')))
            pieces=_pack_group(group,budget)
            ids=[]
            for segment_index,piece in enumerate(pieces,1):
                content_sha=hashlib.sha256(piece.encode()).hexdigest();unit_id='legal:'+content_sha
                related=[(node,name) for node,name in variants if node is group or any(p is group for p in node.parents)]
                if not related:related=[(group,variant_id)]
                occurrences=[]
                for node,name in related:
                    path=[vname for ancestor in [*reversed(list(node.parents)),node] for vnode,vname in variants if vnode is ancestor] or [name]
                    occurrence={'group_id':group_id,'variant_id':name,'audience_path':path,'source_anchor':section_anchor+'/'+'/'.join(path),'clause_anchors':clause_anchors,
                                'group_content_sha256':hashlib.sha256(text.encode()).hexdigest(),'segment_index':segment_index,'segment_count':len(pieces)}
                    subsection=_canonical_legal(node);offset=text.find(subsection) if subsection else -1
                    occurrence['source_scope']='section'
                    if offset>=0 and text.count(subsection)==1:
                        segment_start=sum(len(x)+1 for x in pieces[:segment_index-1]);segment_end=segment_start+len(piece)
                        if segment_end<=offset or segment_start>=offset+len(subsection):continue
                        occurrence.update(source_char_start=offset,source_char_end=offset+len(subsection),segment_char_start=max(offset,segment_start)-segment_start,segment_char_end=min(offset+len(subsection),segment_end)-segment_start)
                    occurrences.append(occurrence)
                if content_sha not in units:
                    anchors={'section':'legal_disclosure','heading_path':['Legal disclosures',variant_id],'source_anchor':locator,
                             'variant_id':variant_id,'parent_unit_id':'page:1','legal_content_sha256':content_sha,
                             'input_contract_sha256':budget.identity,'occurrences':[]}
                    units[content_sha]={'unit_id':unit_id,'anchors':anchors,'status':'complete','text':piece}
                elif units[content_sha]['text']!=piece:raise ValueError('Legal content digest collision')
                units[content_sha]['anchors']['occurrences'].extend(occurrences);ids.append(unit_id)
            references.append({'variant_id':variant_id,'source_anchor':locator,'unit_count':len(ids)})
        section.decompose()
    if len(references)>12:references=[{'variant_id':'all','source_anchor':'explicit-legal-source-units','unit_count':len(units)}]
    main=normalize_html_text_legacy(str(soup).encode())
    return [{'unit_id':'page:1','anchors':{'section':'page','heading_path':['page'],**({'legal_disclosures':references} if references else {})},'status':'complete','text':main,**({'exclusions':controls} if controls else {})},*units.values()]


def validate_html_units(native):
    from collection_control.repository import Conflict
    if native.get('source_kind')!='html':raise Conflict('Page requires native HTML source units')
    units=native['units'];version=native['transform']['version']
    if native['source_kind']!='html' or not units or units[0]['unit_id']!='page:1' or units[0]['anchors'].get('section')!='page' or any(u['status']!='complete' for u in units):
        raise Conflict('HTML requires one complete main page and explicit complete disclosure units')
    if version not in ('2','3','4') and len(units)!=1:raise Conflict('Historical HTML requires one whole-page unit')
    seen={'page:1'}
    for unit in units[1:]:
        a=unit['anchors']
        if not unit['unit_id'].startswith('legal:') or unit['unit_id'] in seen or a.get('section')!='legal_disclosure' or a.get('parent_unit_id')!='page:1' or not all(isinstance(a.get(k),str) and a[k] for k in ('variant_id','source_anchor')):
            raise Conflict('Invalid or duplicated legal disclosure source unit')
        if version=='4':
            if hashlib.sha256(unit['text'].encode()).hexdigest()!=a.get('legal_content_sha256') or not isinstance(a.get('occurrences'),list) or not a['occurrences']:raise Conflict('Legal content/occurrence identity is incomplete')
            for o in a['occurrences']:
                if not isinstance(o,dict) or not all(isinstance(o.get(k),str) and o[k] for k in ('group_id','variant_id','source_anchor','group_content_sha256')) or not isinstance(o.get('clause_anchors'),list) or not 1<=o.get('segment_index',0)<=o.get('segment_count',0):raise Conflict('Legal occurrence/source association is incomplete')
        seen.add(unit['unit_id'])
    return units


def record_metadata(anchors,source_id,revision_id,uri):
    if anchors.get('section')!='legal_disclosure':return {'content_role':'page'}
    result={'content_role':'legal_disclosure','parent_page_id':source_id,'parent_page_revision_id':revision_id,
            'canonical_url':uri,'source_anchor':anchors['source_anchor'],'variant_id':anchors['variant_id'],
            'audience':anchors['variant_id'].removeprefix('disclaimer-'),
            **({'clause_id':anchors['clause_id']} if 'clause_id' in anchors else {})}
    if 'occurrences' in anchors:result.update(legal_content_sha256=anchors['legal_content_sha256'],legal_occurrences=anchors['occurrences'],audiences=sorted({name for a in anchors['occurrences'] for name in a.get('audience_path',[a['variant_id']])}))
    return result


def main_records(records):
    return [r for r in records if r.get('metadata',{}).get('content_role')!='legal_disclosure' and
            (r.get('metadata',{}).get('provenance',{}).get('source_unit_id','page:1')=='page:1')]
