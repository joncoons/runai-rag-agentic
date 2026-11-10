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
    from .web_acquisition import normalize_html_text_legacy
    soup=BeautifulSoup(raw,'html.parser');units=[];references=[]
    candidates=soup.select(LEGAL_SELECTORS)
    sections=[node for node in candidates if not any(parent in candidates for parent in node.parents)]
    for index,section in enumerate(sections,1):
        anchor=section.get('id') or ('legal-section-'+str(index))
        variants=[]
        for node in section.find_all(True):
            names=[c for c in node.get('class',[]) if c.startswith('disclaimer-') and c not in ('disclaimer-text','disclaimer-content')]
            if names and not any(parent in [v[0] for v in variants] for parent in node.parents):
                variants.append((node,names[0]))
        if not variants:variants=[(section,'all')]
        for variant_index,(variant,variant_id) in enumerate(variants,1):
            # Keep shared section prose with each variant, rather than discard
            # bare div/span content or detach a qualification from its audience.
            clone=BeautifulSoup(str(section),'html.parser')
            if variant is not section:
                selector_nodes=[]
                for node in clone.find(True).find_all(True):
                    names=[c for c in node.get('class',[]) if c.startswith('disclaimer-') and c not in ('disclaimer-text','disclaimer-content')]
                    if names and not any(parent in selector_nodes for parent in node.parents):selector_nodes.append(node)
                for i,node in enumerate(selector_nodes,1):
                    if i!=variant_index:node.decompose()
            text=_legal_text(clone)
            locator=('#'+anchor if section.get('id') else 'legal-section:'+str(index))+('/'+variant_id if variant is not section else '')
            key=hashlib.sha256((locator+'\0'+str(index)+'\0'+str(variant_index)).encode()).hexdigest()[:24]
            unit_id='legal:'+key
            anchors={'section':'legal_disclosure','heading_path':['Legal disclosures',variant_id],
                     'source_anchor':locator,'variant_id':variant_id,'parent_unit_id':'page:1'}
            units.append({'unit_id':unit_id,'anchors':anchors,'status':'complete','text':text})
            references.append({'unit_id':unit_id,'variant_id':variant_id,'source_anchor':locator})
        section.decompose()
    main=normalize_html_text_legacy(str(soup).encode())
    return [{'unit_id':'page:1','anchors':{'section':'page','heading_path':['page'],**({'legal_disclosures':references} if references else {})},'status':'complete','text':main},*units]


def validate_html_units(native):
    from collection_control.repository import Conflict
    if native.get('source_kind')!='html':raise Conflict('Page requires native HTML source units')
    units=native['units'];version=native['transform']['version']
    if native['source_kind']!='html' or not units or units[0]['unit_id']!='page:1' or units[0]['anchors'].get('section')!='page' or any(u['status']!='complete' for u in units):
        raise Conflict('HTML requires one complete main page and explicit complete disclosure units')
    if version!='2' and len(units)!=1:raise Conflict('Historical HTML requires one whole-page unit')
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
            'audience':anchors['variant_id'].removeprefix('disclaimer-')}


def main_records(records):
    return [r for r in records if r.get('metadata',{}).get('content_role')!='legal_disclosure' and
            (r.get('metadata',{}).get('provenance',{}).get('source_unit_id','page:1')=='page:1')]
