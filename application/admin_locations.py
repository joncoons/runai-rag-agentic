"""Authenticated directory registration; never grants storage-mount authority."""
import asyncio
from fastapi import HTTPException,Request
from pydantic import BaseModel,ConfigDict,Field,StrictBool
from collection_control.repository import Conflict
from .local_auth import COOKIE,SessionStore,csrf
from .location_registry import LocationRegistry,validate_path
from .location_sources import root_config

class Probe(BaseModel):
    model_config=ConfigDict(extra='forbid')
    path:str=Field(min_length=1,max_length=4096,strict=True)
class Register(Probe):
    alias:str=Field(min_length=1,max_length=128,strict=True)
class Toggle(BaseModel):
    model_config=ConfigDict(extra='forbid')
    expected_version:int=Field(ge=1,strict=True)
    enabled:StrictBool

async def require_admin(request,*,mutation=False):
    from .api import _context
    store=SessionStore(_context(request).pool);config=await store.synchronize_policy()
    session=await store.lookup(request.cookies.get(COOKIE))
    if not config or not session or session['principal']!=config['username']:
        raise HTTPException(401,'Sign in as the local administrator')
    if mutation:csrf(request,session)
    return session


def install_admin_routes(app):
    from .api import _context
    @app.get('/api/admin/locations')
    async def locations(request:Request):
        await require_admin(request)
        rows=await _context(request).pool.fetch('SELECT * FROM application.location_roots ORDER BY alias')
        return {'locations':[dict(r) for r in rows],'mounted_directories':sorted(set(root_config().values()))}

    @app.post('/api/admin/locations/test')
    async def test_location(body:Probe,request:Request):
        await require_admin(request,mutation=True)
        try:await asyncio.to_thread(validate_path,body.path)
        except ValueError:raise HTTPException(422,'Directory is unavailable, contains a symlink, or is outside an approved read-only mount') from None
        return {'accessible':True,'read_only_mount':True}

    @app.post('/api/admin/locations',status_code=201)
    async def register(body:Register,request:Request):
        session=await require_admin(request,mutation=True)
        try:row=await LocationRegistry(_context(request).pool).create(body.alias,body.path,session['principal'])
        except ValueError:raise HTTPException(422,'Use a valid alias and an accessible directory inside an approved read-only mount; symlinks are not allowed') from None
        except Conflict as error:raise HTTPException(409,str(error)) from None
        return {'location':row}

    @app.patch('/api/admin/locations/{alias}')
    async def toggle(alias:str,body:Toggle,request:Request):
        session=await require_admin(request,mutation=True)
        try:row=await LocationRegistry(_context(request).pool).toggle(alias,body.expected_version,body.enabled,session['principal'])
        except ValueError:raise HTTPException(422,'Directory is unavailable or no longer inside an approved mount') from None
        except Conflict as error:raise HTTPException(409,str(error)) from None
        return {'location':row}
