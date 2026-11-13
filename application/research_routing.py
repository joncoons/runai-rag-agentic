"""Research-only task routing with durable phase pinning and no prompt logging."""
import copy,hashlib,json,os,re,time
from contextlib import aclosing
from dataclasses import is_dataclass,replace
from pathlib import Path
from urllib.parse import urlsplit
from collection_control.repository import Conflict

PHASES={'plan','document_selection','synthesis'}

def enabled():return os.environ.get('RESEARCH_ROUTING_ENABLED','false').lower()=='true'

def load_config():
 try:
  config=json.loads(Path(os.environ['RESEARCH_ROUTING_CONFIG_FILE']).read_text())
  u=urlsplit(config['endpoint'])
  if config['schema_version']!=1 or u.scheme not in ('http','https') or not u.hostname or u.username or u.password or u.query or u.fragment:raise ValueError()
  if not re.fullmatch('[a-f0-9]{64}',config['policy_sha256']):raise ValueError()
  if len(config['targets'])!=2 or len(set(config['targets'].values()))!=2 or config['capable_model'] not in config['targets']:raise ValueError()
  if set(config['phases'])!=PHASES:raise ValueError()
  names=list(config['targets'])+list(config['targets'].values())+list(config['phases'].values())
  if any(not isinstance(s,str) or not re.fullmatch(r'[A-Za-z0-9_./:-]{1,256}',s) for s in names):raise ValueError()
  return config
 except (OSError,KeyError,ValueError,TypeError):raise Conflict('Research routing configuration is invalid') from None

class ResearchModels:
 def __init__(self,repository,query,lease):
  self.repo=repository;self.query=query;self.lease=lease;self.config=load_config() if enabled() else None
 async def save(self,state):await self.repo.checkpoint(self.lease,state,'model_routing')
 def phase(self,state,name):
  if name not in PHASES:raise Conflict('Unknown research model phase')
  previous=state.get('model_routing')
  if not self.config:
   if previous:raise Conflict('Restore the pinned research routing policy before resuming this job')
   return None
  sha=self.config['policy_sha256']
  if previous and previous.get('policy_sha256')!=sha:raise Conflict('Research routing policy changed; the pinned policy is required to resume')
  routing=state.setdefault('model_routing',{'policy_sha256':sha,'phases':{}})
  phase=routing['phases'].setdefault(name,{'model':None,'validation_retries':0,'calls':[]})
  if phase['model'] is not None and phase['model'] not in self.config['targets']:raise Conflict('Pinned research model is not approved')
  return phase
 def client(self,state,name):
  phase=self.phase(state,name)
  if phase is None:return self.query
  model=self.config['targets'].get(phase['model'],self.config['phases'][name])
  client=copy.copy(self.query);settings=self.query.settings
  values={'llm_endpoint':self.config['endpoint'],'llm_model':model}
  if is_dataclass(settings):client.settings=replace(settings,**values)
  else:
   client.settings=copy.copy(settings)
   for key,value in values.items():setattr(client.settings,key,value)
  affinity=hashlib.sha256((str(self.lease['run_id'])+':'+name+':'+str(phase['validation_retries'])).encode()).hexdigest()
  client.http=_Transport(self,state,phase,affinity)
  return client
 async def json_strings(self,state,name,messages,*,max_tokens,max_items,choices=None):
  while True:
   phase=self.phase(state,name)
   if phase is not None and phase.get('validation_exhausted'):raise Conflict('Research model validation retry was exhausted; start a new job')
   client=self.client(state,name)
   raw=await client._chat(messages,max_tokens=max_tokens)
   try:
    result=json.loads(raw)
    if not isinstance(result,list) or len(result)>max_items or any(not isinstance(x,str) or not x.strip() for x in result) or len(set(result))!=len(result):raise ValueError()
    if choices is not None and not set(result).issubset(choices):raise ValueError()
    return result
   except (ValueError,TypeError):
    phase=self.phase(state,name)
    if phase is None:raise Conflict('Research model returned invalid '+name+' output') from None
    if phase['validation_retries']>=1:
     phase['validation_exhausted']=True;await self.save(state)
     raise Conflict('Research model returned invalid '+name+' output; bounded validation retry exhausted') from None
    phase['validation_retries']+=1;phase['model']=self.config['capable_model'];phase['retry_reason']='invalid_structured_output';await self.save(state)
 async def stream(self,state,name,messages,*,max_tokens):
  client=self.client(state,name)
  async with aclosing(client._chat_stream(messages,max_tokens=max_tokens)) as chunks:
   async for text in chunks:yield text

class _Transport:
 def __init__(self,models,state,phase,affinity):self.models=models;self.state=state;self.phase=phase;self.affinity=affinity
 def post(self,url,**kwargs):
  from .query import _endpoint
  if url!=_endpoint(self.models.config['endpoint'],'/chat/completions'):raise Conflict('Routed client attempted a non-model request')
  kwargs['headers']={**kwargs.get('headers',{}),'x-switchyard-session-id':self.affinity};kwargs['allow_redirects']=False
  if kwargs.get('json',{}).get('stream'):kwargs['json']={**kwargs['json'],'stream_options':{'include_usage':True}}
  return _Request(self,self.models.query.http.post(url,**kwargs),kwargs['json']['model'])

class _Request:
 def __init__(self,transport,request,route):self.t=transport;self.request=request;self.route=route;self.expected_model=transport.phase['model'];self.response=None;self.call=None
 async def __aenter__(self):
  self.started=time.monotonic();response=await self.request.__aenter__();self.response=response
  try:
   if response.status>=300:return response # original model client handles the typed transport failure
   model=response.headers.get('x-model-router-selected-model')
   if model not in self.t.models.config['targets']:raise Conflict('Router returned an unapproved or missing selected-model identity')
   if self.expected_model is not None and model!=self.expected_model:raise Conflict('Router changed a pinned research model; restore the qualified fixed alias')
   self.t.phase['model']=model
   self.call={'model':model,'route':self.route,'decision_source':'task_classifier' if self.route in self.t.models.config['phases'].values() else 'pinned_target','usage_scope':'serving_completion_only','usage':None,'status':'started','response_header_ms':round((time.monotonic()-self.started)*1000,2)};self.t.phase['calls'].append(self.call)
   await self.t.models.save(self.t.state) # durable pin before any model text can escape
   return _Response(response,self.call,self.started)
  except BaseException:
   await self.request.__aexit__(None,None,None);raise
 async def __aexit__(self,typ,value,tb):
  try:return await self.request.__aexit__(typ,value,tb)
  finally:
   if self.call is not None:
    self.call.update(status='completed' if typ is None else 'failed',elapsed_ms=round((time.monotonic()-self.started)*1000,2))
    if typ is not None:self.call['error_type']=typ.__name__
    await self.t.models.save(self.t.state)

class _Response:
 def __init__(self,response,call,started):self.response=response;self.call=call;self.started=started
 def __getattr__(self,name):return getattr(self.response,name)
 def record_model_event(self,result):
  if not isinstance(result,dict):return
  usage=result.get('usage')
  if isinstance(usage,dict):
   self.call['usage']={k:v for k,v in usage.items() if k in ('prompt_tokens','completion_tokens','total_tokens') and type(v) is int and v>=0}
  if 'first_content_ms' not in self.call and any(isinstance(c,dict) and isinstance(c.get('delta'),dict) and c['delta'].get('content') for c in result.get('choices',[])):
   self.call['first_content_ms']=round((time.monotonic()-self.started)*1000,2)
 async def json(self):
  result=await self.response.json();self.record_model_event(result);return result
