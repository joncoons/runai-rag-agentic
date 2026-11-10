"""Versioned text-first HTML discovery; pending relationships are not assets."""
import copy
from .artifacts import canonical
from .repository import Conflict


def relations(page):
    """Historical v1 assets also establish retained relationship evidence."""
    if 'relations' in page:return page['relations']
    return [{'canonical_uri':a['canonical_uri'],'kind':a['kind'],'locator':a.get('locator',''),'alt':a.get('alt','')} for a in page['assets']]


def validate_relations(page):
    seen=set();identities={}
    for relation in page['relations']:
        if relation.get('kind') not in ('image','binary') or not all(isinstance(relation.get(k),str) for k in ('request_id','canonical_uri','locator','alt')) or not relation['request_id'] or not relation['canonical_uri']:
            raise Conflict('Invalid discovered web relationship')
        key=(relation['request_id'],relation['locator'],relation['canonical_uri'],relation['kind'])
        if key in seen:raise Conflict('Duplicate discovered web relationship')
        seen.add(key)
        identity=(relation['canonical_uri'],relation['kind'])
        if relation['request_id'] in identities and identities[relation['request_id']]!=identity:raise Conflict('Web request identity reused for another asset')
        identities[relation['request_id']]=identity


def merge_discovery(previous,manifest):
    if manifest.get('schema_version')!='web-page-group.v2' or manifest.get('coverage')!='complete':raise Conflict('Complete HTML discovery required')
    old={p['canonical_uri']:p for p in previous['pages']};incoming={}
    known_binary={}
    for prior_page in old.values():
        for asset in prior_page.get('assets',[]):
            if asset['kind']=='binary':
                prior=known_binary.get(asset['canonical_uri'])
                if prior and prior['source_revision_id']!=asset['source_revision_id']:raise Conflict('Published shared binary revisions disagree')
                known_binary[asset['canonical_uri']]=asset
    for page in manifest['pages']:
        if page['canonical_uri'] in incoming:raise Conflict('Duplicate page in discovery group')
        if page.get('assets'):raise Conflict('Discovery manifest cannot claim acquired assets')
        validate_relations(page)
        prior=old.get(page['canonical_uri'])
        new_keys={(r['canonical_uri'],r['kind']) for r in page['relations']}
        if prior:
            old_keys={(r['canonical_uri'],r['kind']) for r in relations(prior)}
            old_policy=prior.get('policy',previous.get('policy'))
            if old_policy is not None and canonical(old_policy)!=canonical(manifest['policy']) and old_keys-new_keys:raise Conflict('Changed asset policy cannot authorize unlink')
        selected=[copy.deepcopy(a) for a in (prior or {}).get('assets',[]) if (a['canonical_uri'],a['kind']) in new_keys]
        # A newly discovered shared reference keeps an already selected binary
        # alive while its independent refresh is pending. Last-edge unlink must
        # not retire that source merely because this edge is new.
        selected_keys={(a['canonical_uri'],a['kind']) for a in selected}
        for uri,kind in sorted(new_keys-selected_keys):
            if kind=='binary' and uri in known_binary:selected.append(copy.deepcopy(known_binary[uri]))
        selected.sort(key=lambda a:(a['canonical_uri'],a['kind'],a['source_revision_id']))
        item=copy.deepcopy(page);item.update(assets=selected,policy=copy.deepcopy(manifest['policy']))
        if 'group_id' in manifest:item['group_id']=manifest['group_id']
        incoming[page['canonical_uri']]=item
    merged={**copy.deepcopy(old),**incoming}
    return {'schema_version':'web-published-graph.v2','pages':[merged[k] for k in sorted(merged)],'policy':copy.deepcopy(manifest['policy'])}
