"""Administrator aliases constrained to existing operator-controlled mounts."""
import asyncio,os
from pathlib import PurePosixPath
from .location_sources import root_config
from .location_acquisition import read_root_registry,_ALIAS


def database_registry_enabled():
    return os.environ.get('LOCATION_REGISTRY_DATABASE')=='true'


def mounted_path(path):
    if (not isinstance(path,str) or not path.startswith('/') or '\\' in path or '\0' in path
        or any(p in ('.','..') for p in path.split('/'))):
        raise ValueError('Choose a directory inside an approved read-only mount')
    target=PurePosixPath(path)
    if not any(target==PurePosixPath(base) or PurePosixPath(base) in target.parents for base in root_config().values()):
        raise ValueError('Directory is outside the approved read-only mounts')
    return str(target)


def validate_path(path):
    path=mounted_path(path)
    read_root_registry({'probe':path})
    return path


async def active_roots(pool):
    if not database_registry_enabled():return root_config()
    rows=await pool.fetch('SELECT alias,path FROM application.location_roots WHERE enabled ORDER BY alias')
    result={}
    for row in rows:
        try:result[row['alias']]=mounted_path(row['path'])
        except ValueError:continue
    return result


class LocationRegistry:
    def __init__(self,pool):self.pool=pool

    async def create(self,alias,path,actor):
        if not _ALIAS.fullmatch(alias):raise ValueError('Use letters, numbers, hyphens or underscores for the alias')
        path=await asyncio.to_thread(validate_path,path)
        async with self.pool.acquire() as c,c.transaction():
            row=await c.fetchrow('''INSERT INTO application.location_roots(alias,path,created_by) VALUES($1,$2,$3)
              ON CONFLICT DO NOTHING RETURNING *''',alias,path,actor)
            if not row:
                from collection_control.repository import Conflict
                raise Conflict('Alias already registered; its directory cannot be reassigned')
            await c.execute("INSERT INTO application.location_root_events(alias,action,version,actor) VALUES($1,'registered',1,$2)",alias,actor)
            return dict(row)

    async def toggle(self,alias,version,enabled,actor):
        from collection_control.repository import Conflict
        async with self.pool.acquire() as c,c.transaction():
            row=await c.fetchrow('SELECT * FROM application.location_roots WHERE alias=$1 FOR UPDATE',alias)
            if not row or row['version']!=version:raise Conflict('Location changed; refresh before saving')
            if enabled:await asyncio.to_thread(validate_path,row['path'])
            row=await c.fetchrow('''UPDATE application.location_roots SET enabled=$2,version=version+1,updated_at=clock_timestamp()
              WHERE alias=$1 RETURNING *''',alias,enabled)
            await c.execute('INSERT INTO application.location_root_events(alias,action,version,actor) VALUES($1,$2,$3,$4)',alias,'enabled' if enabled else 'disabled',row['version'],actor)
            return dict(row)
