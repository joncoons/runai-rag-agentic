"""Deterministic explicit legal sections; preserve every audience's source text."""
import hashlib
from bs4 import BeautifulSoup

LEGAL_SELECTORS='section#disclaimer, section.disclaimer, section.legal-disclaimer, #legal-disclaimers, [role="doc-endnotes"]'

def _legal_text(soup):
    from .web_acquisition import html_table_to_markdown
    # A selected disclosure is already scoped. Do not choose only its first
    # article or discard footer/header clauses inside that legal subtree.
    for node in soup.find_all(['script','style','noscript','template']):node.decompose()
    for table in list(soup.find_all('table')):
        if table.parent is not None:table.replace_with(soup.new_string(html_table_to_markdown(table)))
    return '\n'.join(line for line in (' '.join(v.split()) for v in soup.get_text('\n',strip=True).splitlines()) if line)


def html_units(raw):
    """Separate author-marked legal clauses, never arbitrary text windows.

    Shared group prose is retained once. Clause identity includes DOM order so
    duplicate author IDs cannot merge distinct disclosures.
    """
    import re
    from .web_acquisition import normalize_html_text_legacy
    soup=BeautifulSoup(raw,'html.parser');units=[];references=[]
    # Empty named anchors label their paragraph, rather than an empty unit.
    # Associate only the containing paragraph or immediate following paragraph;
    # never consume arbitrary following product blocks.
    container_ids={id(n) for n in soup.select(LEGAL_SELECTORS)}
    for node in list(soup.find_all(id=re.compile(r'^legal-',re.I))):
        if id(node) in container_ids or node.get_text(strip=True):continue
        target=node.parent if node.parent and node.parent.name in ('p','li','td','th') else node.find_next_sibling()
        if target is None or (target is not node.parent and target.name!='p'):continue
        wrapper=soup.new_tag('div',id=node['id']);target.wrap(wrapper)
        del node['id']
        if node not in target.descendants:wrapper.insert(0,node.extract())
    candidates=soup.select(LEGAL_SELECTORS)
    # Several RBC pages place their explicit legal clauses outside a section.
    candidates += [n for n in soup.find_all(id=re.compile(r'^(?:legal|disclaimer)-',re.I)) if n not in candidates]
    sections=[n for n in candidates if not any(p in candidates for p in n.parents)]
    for index,section in enumerate(sections,1):
        section_anchor='#'+section['id'] if section.get('id') else 'legal-section:'+str(index)
        clone=BeautifulSoup(str(section),'html.parser');root=clone.find(True)
        variants=[]
        for node in root.find_all(True):
            names=[c for c in node.get('class',[]) if c.startswith('disclaimer-') and c not in ('disclaimer-text','disclaimer-content')]
            if names and not any(p in [v[0] for v in variants] for p in node.parents):variants.append((node,names[0]))
        groups=variants if variants else [(root,'all')]
        def emit(node,variant_id,clause_id,ordinal,locator):
            text=_legal_text(BeautifulSoup(str(node),'html.parser'))
            if not text.strip():return
            key=hashlib.sha256((section_anchor+'\0'+str(index)+'\0'+variant_id+'\0'+str(len(units))+'\0'+str(ordinal)+'\0'+locator).encode()).hexdigest()[:24]
            anchors={'section':'legal_disclosure','heading_path':['Legal disclosures',variant_id],
                     'source_anchor':locator,'variant_id':variant_id,'parent_unit_id':'page:1'}
            if clause_id is not None:anchors['clause_id']=clause_id
            units.append({'unit_id':'legal:'+key,'anchors':anchors,'status':'complete','text':text})
        for group,variant_id in groups:
            before=len(units)
            possible=[n for n in [group,*group.find_all(True)] if re.match(r'^legal-',n.get('id',''),re.I) and not n.select_one('[id^=legal-]') and n.get('id')!='legal-disclaimers']
            clauses=possible
            # Remove clauses from the common group, but retain their whole
            # nested tables, links and qualifications within their own unit.
            for ordinal,clause in enumerate(clauses,1):
                emit(clause,variant_id,clause['id'],ordinal,'#'+clause['id'])
                if clause is group:group=None
                else:clause.extract()
            if group is not None:emit(group,variant_id,None,0,section_anchor+'/'+variant_id)
            references.append({'variant_id':variant_id,'source_anchor':section_anchor,'unit_count':len(units)-before})
        if variants:
            for variant,_ in variants:variant.extract()
            before=len(units);emit(root,'all',None,0,section_anchor+'/shared')
            if len(units)>before:references.append({'variant_id':'all','source_anchor':section_anchor,'unit_count':len(units)-before})
        section.decompose()
    if len(references)>12:
        references=[{'variant_id':'all','source_anchor':'explicit-legal-source-units','unit_count':len(units)}]
    main=normalize_html_text_legacy(str(soup).encode())
    return [{'unit_id':'page:1','anchors':{'section':'page','heading_path':['page'],**({'legal_disclosures':references} if references else {})},'status':'complete','text':main},*units]


def validate_html_units(native):
    from collection_control.repository import Conflict
    if native.get('source_kind')!='html':raise Conflict('Page requires native HTML source units')
    units=native['units'];version=native['transform']['version']
    if native['source_kind']!='html' or not units or units[0]['unit_id']!='page:1' or units[0]['anchors'].get('section')!='page' or any(u['status']!='complete' for u in units):
        raise Conflict('HTML requires one complete main page and explicit complete disclosure units')
    if version not in ('2','3') and len(units)!=1:raise Conflict('Historical HTML requires one whole-page unit')
    seen={'page:1'}
    for unit in units[1:]:
        a=unit['anchors']
        if not unit['unit_id'].startswith('legal:') or unit['unit_id'] in seen or a.get('section')!='legal_disclosure' or a.get('parent_unit_id')!='page:1' or not all(isinstance(a.get(k),str) and a[k] for k in ('variant_id','source_anchor')):
            raise Conflict('Invalid or duplicated legal disclosure source unit')
        seen.add(unit['unit_id'])
    return units


def record_metadata(anchors,source_id,revision_id,uri):
    if anchors.get('section')!='legal_disclosure':return {'content_role':'page'}
    return {'content_role':'legal_disclosure','parent_page_id':source_id,'parent_page_revision_id':revision_id,
            'canonical_url':uri,'source_anchor':anchors['source_anchor'],'variant_id':anchors['variant_id'],
            'audience':anchors['variant_id'].removeprefix('disclaimer-'),
            **({'clause_id':anchors['clause_id']} if 'clause_id' in anchors else {})}


def main_records(records):
    return [r for r in records if r.get('metadata',{}).get('content_role')!='legal_disclosure' and
            (r.get('metadata',{}).get('provenance',{}).get('source_unit_id','page:1')=='page:1')]
