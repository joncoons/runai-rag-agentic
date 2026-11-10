"""Validate future acquisition edits without rewriting admitted source identity."""
from dataclasses import asdict
from urllib.parse import urlsplit
import os
from fastapi import HTTPException


def validate_source_edit(source,body,*,roots=None):
    from .api import WebIngestion
    from .source_api import _POLICY_FIELDS
    from .source_registry import _safe_urls
    from .web_acquisition import AcquisitionPolicy,validate_url
    fields=body.model_fields_set-{'expected_version'}
    if not fields:raise ValueError('No source changes provided')
    result={k:getattr(body,k) for k in fields}
    if 'schedule' in result:result['schedule']=body.schedule.model_dump()
    if source.get('kind','website')=='website':
        if 'location' in fields:raise ValueError('Website identity cannot become a location')
        policy=dict(source.get('policy') or {});policy.update(body.policy or {})
        public={k:v for k,v in policy.items() if k in _POLICY_FIELDS}
        if body.policy is not None and set(body.policy)-set(_POLICY_FIELDS):raise ValueError('Unsupported policy fields')
        # Validate changed extension selections; retain untouched historical
        # options instead of silently normalizing or blocking unrelated edits.
        checks=dict(public)
        unchanged_extensions=set()
        for key in ('included_extensions','ignored_extensions'):
            if key not in (body.policy or {}) or (body.policy or {}).get(key)==(source.get('policy') or {}).get(key):
                checks.pop(key,None);unchanged_extensions.add(key)
        selection=WebIngestion.model_validate({**checks,'seed_urls':body.seed_urls if body.seed_urls is not None else source['seed_urls'],
            'captions_enabled':body.captions_enabled if body.captions_enabled is not None else source.get('captions_enabled',False)})
        private={v.strip().lower() for v in os.environ.get('WEB_ASSET_PRIVATE_HOSTS','').split(',') if v.strip()}
        effective=AcquisitionPolicy(**{k:tuple(getattr(selection,k)) if isinstance(getattr(selection,k),list) else getattr(selection,k) for k in _POLICY_FIELDS},
            private_hosts=tuple(h for h in selection.allowed_hosts if h.lower() in private),
            private_ports=tuple(int(v) for v in os.environ.get('WEB_ASSET_PRIVATE_PORTS','').split(',') if v.strip()))
        for seed in _safe_urls(selection.seed_urls):validate_url(seed,effective)
        if selection.page_prefixes:
            for prefix in _safe_urls(selection.page_prefixes):validate_url(prefix,effective)
        if 'policy' in fields:result['policy']={**(source.get('policy') or {}),**{k:getattr(selection,k) for k in (body.policy or {}) if k not in unchanged_extensions}}
        if 'seed_urls' in fields:result['seed_urls']=selection.seed_urls
    else:
        if fields & {'seed_urls','policy'}:raise ValueError('Location sources use a location spec')
        if 'location' in fields:
            from .location_api import LocationCreate,_roots
            from .location_acquisition import _location_parts,_parse_spec,LocationLimits
            location={**(source.get('location') or {}),**body.location}
            if set(location)-{'uri','recursive','max_depth','max_files','allowed_hosts'}:raise ValueError('Unsupported location fields')
            checked=LocationCreate.model_validate({'name':body.name or source['name'],**location})
            uri=checked.uri
            if uri.startswith('location://'):
                alias,_=_location_parts(uri)
                if alias not in (roots if roots is not None else _roots()):raise ValueError('Unregistered directory')
                result['location']={k:getattr(checked,k) for k in ('uri','recursive','max_depth','max_files')}
            elif uri.startswith(('https://','http://')):
                from .source_refresh import network_policy
                _safe_urls([uri])
                hosts=checked.allowed_hosts or [urlsplit(uri).hostname]
                validate_url(uri,network_policy({'policy':{'allowed_hosts':hosts}}))
                result['location']={'uri':uri,'allowed_hosts':hosts,'max_files':checked.max_files}
            else:raise ValueError('Unsupported protocol')
            _parse_spec(result['location'],LocationLimits())
    return result
