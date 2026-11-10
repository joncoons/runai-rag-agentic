"""Local pilot identity and private-session ownership; no provider credentials."""
import asyncio,hashlib,hmac,json,os,secrets
from pathlib import Path
from uuid import uuid4
from datetime import timedelta
from fastapi import HTTPException,Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel,ConfigDict,Field

COOKIE='rag_session'


def password_record(password):
    salt=secrets.token_bytes(16)
    return {'algorithm':'scrypt','salt':salt.hex(),'digest':hashlib.scrypt(password.encode(),salt=salt,n=16384,r=8,p=1,dklen=32).hex()}


def _configuration():
    path=os.environ.get('LOCAL_ADMIN_CONFIG_FILE')
    if not path:return None
    try:
        data=json.loads(Path(path).read_text());record=data['password'];username=data['username']
        if record['algorithm']!='scrypt' or len(bytes.fromhex(record['salt']))!=16 or len(bytes.fromhex(record['digest']))!=32:raise ValueError()
        if not isinstance(username,str) or not username or len(username)>120 or any(ord(c)<32 for c in username):raise ValueError()
        grants=data.get('collection_write',[])
        if not isinstance(grants,list) or any(not isinstance(g,str) or not g for g in grants):raise ValueError()
        data['policy_version']=hashlib.sha256(json.dumps(data,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        return data
    except (OSError,ValueError,KeyError,TypeError):return None


def _verify(password,record):
    actual=hashlib.scrypt(password.encode(),salt=bytes.fromhex(record['salt']),n=16384,r=8,p=1,dklen=32)
    return hmac.compare_digest(actual,bytes.fromhex(record['digest']))


class SessionStore:
    def __init__(self,pool):self.pool=pool

    async def synchronize_policy(self):
        config=_configuration()
        if config is None:
            await self.pool.execute('UPDATE application.local_principals SET enabled=false WHERE enabled')
            return None
        await self.pool.execute('''INSERT INTO application.local_principals(username,policy_version,collection_write,enabled)
          VALUES($1,$2,$3::jsonb,true) ON CONFLICT(username) DO UPDATE SET policy_version=EXCLUDED.policy_version,
          collection_write=EXCLUDED.collection_write,enabled=true,updated_at=clock_timestamp()
          WHERE local_principals.policy_version<>EXCLUDED.policy_version''',config['username'],config['policy_version'],json.dumps(config.get('collection_write',[])))
        await self.pool.execute('UPDATE application.local_principals SET enabled=false WHERE username<>$1 AND enabled',config['username'])
        return config

    async def _by_hash(self,digest):
        row=await self.pool.fetchrow('''SELECT s.*,p.policy_version AS current_policy,p.collection_write,p.enabled FROM application.local_sessions s
          LEFT JOIN application.local_principals p ON p.username=s.principal WHERE s.token_sha256=$1
          AND s.revoked_at IS NULL AND s.expires_at>clock_timestamp()''',digest)
        if not row:return None
        result=dict(row)
        if result['principal'] and (not result['enabled'] or result['policy_version']!=result['current_policy']):result['principal']=None
        grants=result.get('collection_write') or []
        result['collection_write']=json.loads(grants) if isinstance(grants,str) else grants
        return result

    async def lookup(self,token):
        if not token or len(token)>256:return None
        return await self._by_hash(hashlib.sha256(token.encode()).hexdigest())

    async def issue(self,*,owner_id=None,principal=None,policy_version=None):
        token=secrets.token_urlsafe(32)
        row=await self.pool.fetchrow('''INSERT INTO application.local_sessions(token_sha256,owner_id,principal,policy_version,csrf_token,expires_at)
          VALUES($1,$2,$3,$4,$5,clock_timestamp()+interval '24 hours') RETURNING *''',hashlib.sha256(token.encode()).hexdigest(),owner_id or uuid4(),principal,policy_version,secrets.token_urlsafe(32))
        return token,await self._by_hash(row['token_sha256'])

    async def revoke(self,session):
        await self.pool.execute('UPDATE application.local_sessions SET revoked_at=clock_timestamp() WHERE token_sha256=$1',session['token_sha256'])

    async def reserve_login(self,request,username):
        remote=request.client.host if request.client else 'unknown'
        key=hashlib.sha256((remote+'\0'+username.casefold()).encode()).hexdigest()
        attempts=await self.pool.fetchval('''INSERT INTO application.local_login_attempts(rate_key,attempts) VALUES($1,1)
          ON CONFLICT(rate_key) DO UPDATE SET attempts=CASE WHEN local_login_attempts.window_started<clock_timestamp()-interval '15 minutes' THEN 1 ELSE local_login_attempts.attempts+1 END,
          window_started=CASE WHEN local_login_attempts.window_started<clock_timestamp()-interval '15 minutes' THEN clock_timestamp() ELSE local_login_attempts.window_started END RETURNING attempts''',key)
        if attempts>5:raise HTTPException(429,'Too many login attempts. Try again later.',headers={'Retry-After':'900'})
        return key

    async def authorize_write(self,session,destination):
        current=await self._by_hash(session['token_sha256']) if session else None
        if not current or not current['principal']:raise HTTPException(401,'Sign in to add files to a collection')
        if destination not in current['collection_write'] and '*' not in current['collection_write']:
            raise HTTPException(403,'You do not have permission to write to this collection')
        exists=await self.pool.fetchval('''SELECT EXISTS(SELECT 1 FROM application.collections a JOIN collection_control.collections c USING(collection_id)
          WHERE a.collection_id=$1 AND NOT c.deleted)''',destination)
        if not exists:raise HTTPException(404,'Destination collection is unavailable')
        return {'session_hash':current['token_sha256'],'owner_id':str(current['owner_id']),'principal':current['principal'],
                'policy_version':current['policy_version'],'destination':destination,'valid_until':current['expires_at']}


def public_session(session):
    return {'authenticated':bool(session['principal']),'principal':session['principal'],'csrf_token':session['csrf_token'],
            'can_promote':bool(session['principal'] and session['collection_write'])}


def csrf(request,session):
    expected=os.environ.get('PUBLIC_ORIGIN',str(request.base_url).rstrip('/')).rstrip('/')
    origin=request.headers.get('origin')
    supplied=request.headers.get('x-csrf-token','')
    if not session or not hmac.compare_digest(supplied,session['csrf_token']) or (origin is not None and origin.rstrip('/')!=expected):
        raise HTTPException(403,'The request needs a valid session and same-origin token')


def set_cookie(response,request,token):
    response.set_cookie(COOKIE,token,max_age=86400,httponly=True,samesite='strict',secure=request.url.scheme=='https' or os.environ.get('SESSION_COOKIE_SECURE')=='true',path='/')


class Login(BaseModel):
    model_config=ConfigDict(extra='forbid')
    username:str=Field(min_length=1,max_length=120,strict=True)
    password:str=Field(min_length=1,max_length=4096,strict=True)


def install_auth_routes(app):
    from .api import _context
    async def current(request):
        store=SessionStore(_context(request).pool);await store.synchronize_policy()
        return store,await store.lookup(request.cookies.get(COOKIE))

    @app.get('/api/session')
    async def session(request:Request):
        store,value=await current(request);token=None
        if value is None:token,value=await store.issue()
        response=JSONResponse(public_session(value),headers={'Cache-Control':'no-store'})
        if token:set_cookie(response,request,token)
        return response

    @app.post('/api/session/login')
    async def login(body:Login,request:Request):
        store,old=await current(request);csrf(request,old)
        config=_configuration()
        if config is None:raise HTTPException(503,'Local sign-in is not configured')
        rate_key=await store.reserve_login(request,body.username)
        valid=await asyncio.to_thread(_verify,body.password,config['password'])
        if not valid or not hmac.compare_digest(body.username,config['username']):
            await store.pool.execute("INSERT INTO application.events(event_type,payload) VALUES('admin_login_failed','{}')")
            raise HTTPException(401,'Invalid username or password')
        if not await store.pool.fetchval('SELECT enabled FROM application.local_principals WHERE username=$1',config['username']):raise HTTPException(403,'Local account is disabled')
        token,value=await store.issue(owner_id=old['owner_id'],principal=config['username'],policy_version=config['policy_version'])
        await store.revoke(old);await store.pool.execute('DELETE FROM application.local_login_attempts WHERE rate_key=$1',rate_key)
        await store.pool.execute("INSERT INTO application.events(event_type,payload) VALUES('admin_login',$1::jsonb)",json.dumps({'principal':config['username']}))
        response=JSONResponse(public_session(value),headers={'Cache-Control':'no-store'});set_cookie(response,request,token);return response

    @app.post('/api/session/logout')
    async def logout(request:Request):
        store,old=await current(request);csrf(request,old);await store.revoke(old)
        await store.pool.execute("INSERT INTO application.events(event_type,payload) VALUES('admin_logout',$1::jsonb)",json.dumps({'principal':old['principal']}))
        token,value=await store.issue();response=JSONResponse(public_session(value),headers={'Cache-Control':'no-store'});set_cookie(response,request,token);return response

    @app.get('/api/session/collection-destinations')
    async def destinations(request:Request):
        store,value=await current(request)
        if not value or not value['principal']:raise HTTPException(401,'Sign in to select a destination collection')
        rows=await store.pool.fetch('''SELECT a.collection_id,a.name FROM application.collections a JOIN collection_control.collections c USING(collection_id)
           WHERE NOT c.deleted ORDER BY a.name,a.collection_id''')
        allowed=value['collection_write'];return {'collections':[{'id':r['collection_id'],'name':r['name']} for r in rows if '*' in allowed or r['collection_id'] in allowed]}
