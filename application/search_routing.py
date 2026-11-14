"""Request-scoped Search routing; provider credentials remain in Switchyard."""
import copy,ipaddress,json,os,re,time,uuid
from dataclasses import is_dataclass,replace
from pathlib import Path
from urllib.parse import urlsplit
from collection_control.repository import Conflict


def enabled():
    return os.environ.get('SEARCH_ROUTING_ENABLED','false').lower()=='true'


def validate_config(config):
    try:
        u=urlsplit(config['endpoint']);host=u.hostname or ''
        try:local=ipaddress.ip_address(host).is_private or ipaddress.ip_address(host).is_loopback
        except ValueError:local='.' not in host or host.endswith(('.svc.cluster.local','.svc','.internal','.local','.test'))
        if not host or not local or u.scheme not in ('http','https') or u.username or u.password or u.query or u.fragment:raise ValueError()
        if type(config['schema_version']) is not int or config['schema_version']!=1:raise ValueError()
        if not re.fullmatch('[a-f0-9]{64}',config['policy_sha256']):raise ValueError()
        targets=config['targets'];locals_=config['local_models'];routes=config['routes']
        if not isinstance(targets,dict) or sorted(targets.values()) not in (['medium','small'],['large','medium','small']):raise ValueError()
        if not isinstance(locals_,list) or len(set(locals_))!=2 or {targets[m] for m in locals_}!={'small','medium'}:raise ValueError()
        if set(routes)!=({'local','hosted'} if len(targets)==3 else {'local'}) or len(set(routes.values()))!=len(routes):raise ValueError()
        if any(not isinstance(n,str) or not re.fullmatch(r'[A-Za-z0-9_./:-]{1,256}',n) for n in [*targets,*routes.values()]):raise ValueError()
        if type(config['hosted_context_window']) is not int or config['hosted_context_window']<4096:raise ValueError()
        return config
    except (KeyError,ValueError,TypeError,AttributeError):
        raise Conflict('Search routing configuration is invalid') from None


def load_config():
    try:return validate_config(json.loads(Path(os.environ['SEARCH_ROUTING_CONFIG_FILE']).read_text()))
    except (OSError,KeyError,ValueError):raise Conflict('Search routing configuration is unavailable') from None


class SearchModels:
    def __init__(self,query,config=None):
        self.query=query;self.config=validate_config(config) if config is not None else load_config()

    def client(self,messages,*,allow_remote=False,max_tokens=2048):
        if type(allow_remote) is not bool:raise Conflict('Remote inference permission must be an explicit boolean')
        config=self.config
        # A conservative byte-based eligibility test, not a tokenizer or truncation.
        # Includes chat framing, output reserve and margin; large inputs stay local.
        input_bound=sum(len(m.content.encode('utf-8'))+128 for m in messages)+1024+max_tokens
        excluded=allow_remote and input_bound>config['hosted_context_window']
        remote=allow_remote and 'hosted' in config['routes'] and not excluded
        if allow_remote and 'hosted' not in config['routes']:raise Conflict('Hosted inference is not configured for this installation')
        client=copy.copy(self.query);settings=self.query.settings
        values={'llm_endpoint':config['endpoint'],'llm_model':config['routes']['hosted' if remote else 'local']}
        if is_dataclass(settings):client.settings=replace(settings,**values)
        else:
            client.settings=copy.copy(settings)
            for k,v in values.items():setattr(client.settings,k,v)
        client.search_routing={'policy_sha256':config['policy_sha256'],'route':values['llm_model'],
            'remote_permitted':allow_remote,'remote_context_excluded':bool(excluded),'model':None,'tier':None}
        allowed=set(config['targets'] if remote else config['local_models'])
        client.http=_Transport(self.query.http,config,allowed,client.search_routing)
        return client


def answer_client(query,messages,*,allow_remote=False,max_tokens=2048):
    if type(allow_remote) is not bool:raise Conflict('Remote inference permission must be an explicit boolean')
    if not enabled():
        if allow_remote:raise Conflict('Hosted Search inference is not enabled')
        return query
    return SearchModels(query).client(messages,allow_remote=allow_remote,max_tokens=max_tokens)


class _Transport:
    def __init__(self,http,config,allowed,record):
        self.http=http;self.config=config;self.allowed=allowed;self.record=record
        self.affinity=uuid.uuid4().hex;self.called=False
    def post(self,url,**kwargs):
        from .query import _endpoint
        if self.called or url!=_endpoint(self.config['endpoint'],'/chat/completions'):raise Conflict('Search allows one routed answer call per query')
        self.called=True
        kwargs['headers']={**kwargs.get('headers',{}),'x-switchyard-session-id':self.affinity}
        kwargs['allow_redirects']=False
        return _Request(self,self.http.post(url,**kwargs))


class _Request:
    def __init__(self,transport,request):self.t=transport;self.request=request
    async def __aenter__(self):
        self.started=time.monotonic();response=await self.request.__aenter__()
        try:
            if response.status>=300:return response
            model=response.headers.get('x-model-router-selected-model')
            if model not in self.t.allowed:raise Conflict('Router selected an unapproved Search model')
            self.t.record.update(model=model,tier=self.t.config['targets'][model],response_header_ms=round((time.monotonic()-self.started)*1000,2))
            return _Response(response,model)
        except BaseException:
            await self.request.__aexit__(None,None,None);raise
    async def __aexit__(self,*args):return await self.request.__aexit__(*args)


class _Response:
    def __init__(self,response,model):self.response=response;self.model=model
    def __getattr__(self,name):return getattr(self.response,name)
    def record_model_event(self,event):
        if 'model' in event and event['model']!=self.model:raise Conflict('Search stream changed the selected model')
    async def json(self):
        result=await self.response.json();self.record_model_event(result);return result
