"""Collection-scoped registered-location controls; source runs remain shared."""
from urllib.parse import urlsplit
from fastapi import HTTPException,Request
from pydantic import BaseModel,ConfigDict,Field,StrictBool
from collection_control.pinned_connection import PinnedPool
from collection_control.repository import Conflict
from .source_api import SourceSchedule,public_source,public_run

def _roots():
 from .location_sources import root_config
 return root_config()

class LocationCreate(BaseModel):
 model_config=ConfigDict(extra='forbid')
 name:str=Field(min_length=1,max_length=120,strict=True)
 uri:str=Field(min_length=1,max_length=8192,strict=True)
 recursive:StrictBool=True
 max_depth:int=Field(default=8,ge=0,le=32,strict=True)
 max_files:int=Field(default=128,ge=1,le=512,strict=True)
 allowed_hosts:list[str]=Field(default_factory=list,max_length=10)
 captions_enabled:StrictBool=False
 schedule:SourceSchedule=Field(default_factory=SourceSchedule)

def install_location_routes(app):
 from .api import _context,_collection
 from .source_registry import SourceRegistry
 from .location_acquisition import _parse_spec,_location_parts,LocationLimits
 @app.get('/api/location-roots')
 async def locations(request:Request):
  from .location_registry import active_roots,database_registry_enabled
  roots=await active_roots(_context(request).pool) if database_registry_enabled() else _roots()
  return {'roots':[{'alias':alias,'uri':'location://'+alias+'/'} for alias in sorted(roots)]}
 @app.post('/api/collections/{collection_id}/location-sources',status_code=202)
 async def create_location(collection_id:str,body:LocationCreate,request:Request):
  ctx=_context(request);await _collection(ctx.pool,collection_id)
  try:
   spec={k:getattr(body,k) for k in ('uri','recursive','max_depth','max_files')}
   parsed=urlsplit(body.uri)
   if parsed.scheme=='location':
    alias,_=_location_parts(body.uri)
    from .location_registry import active_roots,database_registry_enabled
    roots=await active_roots(ctx.pool) if database_registry_enabled() else _roots()
    if alias not in roots:raise ValueError('Unregistered location')
   elif parsed.scheme in ('http','https'):
    spec.pop('recursive',None);spec.pop('max_depth',None)
    from .source_refresh import network_policy
    from .web_acquisition import validate_url
    spec['allowed_hosts']=body.allowed_hosts or [parsed.hostname];validate_url(body.uri,network_policy({'policy':{'allowed_hosts':spec['allowed_hosts']}}))
   else:raise ValueError('Unsupported location protocol')
   _parse_spec(spec,LocationLimits())
   async with ctx.pool.acquire() as c,c.transaction():
    registry=SourceRegistry(PinnedPool(c));source=await registry.create_location(collection_id,name=body.name,location=spec,schedule=body.schedule.model_dump(),captions_enabled=body.captions_enabled);run=await registry.request(source['source_id'],collection_id)
   return {'source':public_source(source),'run':public_run(run)}
  except Conflict:raise HTTPException(409,'Source or collection state changed') from None
  except ValueError:raise HTTPException(422,'Invalid registered location, URI scope or acquisition bounds') from None
