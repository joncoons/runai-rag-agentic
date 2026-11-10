"""Public saved website controls; operator network grants never become saved policy."""
import os
import re
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictStr, field_validator, model_validator

from collection_control.pinned_connection import PinnedPool
from collection_control.repository import Conflict


class SourceSchedule(BaseModel):
    model_config = ConfigDict(extra='forbid')
    enabled: StrictBool = False
    timezone: str = Field(default='America/Chicago', strict=True, max_length=100)
    start: str = Field(default='02:00', strict=True)
    end: str = Field(default='06:00', strict=True)

    @field_validator('timezone')
    @classmethod
    def valid_timezone(cls, value):
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError):
            raise ValueError('Use an IANA timezone') from None
        return value

    @field_validator('start', 'end')
    @classmethod
    def valid_time(cls, value):
        if not re.fullmatch(r'(?:[01][0-9]|2[0-3]):[0-5][0-9]', value):
            raise ValueError('Use a local time in HH:MM format')
        return value

    @model_validator(mode='after')
    def same_day_window(self):
        if self.start >= self.end:
            raise ValueError('Daily start must precede end on the same day')
        return self


class SourceUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    expected_version: int | None = Field(default=None,ge=1,strict=True)
    name: str | None = Field(default=None,min_length=1,max_length=120,strict=True)
    seed_urls: list[StrictStr] | None = Field(default=None,min_length=1,max_length=100)
    policy: dict | None = None
    location: dict | None = None
    schedule: SourceSchedule | None = None
    paused: StrictBool | None = None
    captions_enabled: StrictBool | None = None


_SOURCE_FIELDS = ('source_id', 'collection_id', 'name', 'seed_urls', 'policy', 'schedule', 'paused', 'version', 'next_fire', 'last_successful_run_id','kind','location','captions_enabled')
_RUN_FIELDS = ('run_id', 'source_id', 'state', 'job_id', 'counts', 'nominal_fire', 'error_code')
_POLICY_FIELDS = ('allowed_hosts', 'page_prefixes', 'max_depth', 'max_pages', 'max_asset_response_bytes', 'prefer_https', 'include_images', 'included_extensions', 'ignored_extensions')


def public_run(row):
    result = {key: row.get(key) for key in _RUN_FIELDS}
    code = result.get('error_code')
    result['error_code'] = code if isinstance(code, str) and re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,79}', code) else None
    return result


def public_source(row):
    result = {key: row.get(key) for key in _SOURCE_FIELDS}
    result['policy'] = {key: value for key, value in (row.get('policy') or {}).items() if key in _POLICY_FIELDS}
    return result


def install_source_routes(app):
    # Lazy imports preserve api.create_app's existing initialization order.
    from .api import WebIngestion, _collection, _context
    from .source_registry import SourceRegistry
    from .web_acquisition import AcquisitionPolicy, validate_url

    class SourceCreate(WebIngestion):
        name: str | None = Field(default=None, strict=True, min_length=1, max_length=120)
        schedule: SourceSchedule = Field(default_factory=SourceSchedule)

        @field_validator('name')
        @classmethod
        def clean_name(cls, value):
            if value is not None:
                value = value.strip()
                if not value or any(ord(c) < 32 for c in value):
                    raise ValueError('Enter a nonempty website name without control characters')
            return value

    async def registry(request, collection_id):
        ctx = _context(request)
        await _collection(ctx.pool, collection_id)
        return SourceRegistry(ctx.pool)

    def failed(error):
        if isinstance(error, Conflict):
            raise HTTPException(409, 'Source is unavailable or its state changed') from error
        raise HTTPException(422, 'Invalid acquisition scope or schedule') from error

    @app.post('/api/collections/{collection_id}/web-sources', status_code=202)
    async def create_source(collection_id: str, body: SourceCreate, request: Request):
        ctx = _context(request)
        await _collection(ctx.pool, collection_id)
        policy = {key: getattr(body, key) for key in _POLICY_FIELDS}
        private = {v.strip().lower() for v in os.environ.get('WEB_ASSET_PRIVATE_HOSTS', '').split(',') if v.strip()}
        try:
            effective = AcquisitionPolicy(**{key: tuple(value) if isinstance(value, list) else value for key, value in policy.items()},
                private_hosts=tuple(h for h in body.allowed_hosts if h.lower() in private),
                private_ports=tuple(int(v) for v in os.environ.get('WEB_ASSET_PRIVATE_PORTS', '').split(',') if v.strip()))
            for seed in body.seed_urls:
                validate_url(seed, effective)
            async with ctx.pool.acquire() as connection, connection.transaction():
                store = SourceRegistry(PinnedPool(connection))
                source = await store.create(collection_id, name=body.name or body.seed_urls[0][:120],
                    seed_urls=body.seed_urls, policy=policy, schedule=body.schedule.model_dump(),captions_enabled=body.captions_enabled)
                run = await store.request(source['source_id'], collection_id)
        except (ValueError, Conflict) as error:
            failed(error)
        return {'source': public_source(source), 'run': public_run(run)}

    @app.get('/api/collections/{collection_id}/sources')
    @app.get('/api/collections/{collection_id}/web-sources')
    async def list_sources(collection_id: str, request: Request):
        store = await registry(request, collection_id)
        try:
            sources = await store.list(collection_id)
        except (ValueError, Conflict) as error:
            failed(error)
        result={'sources': [public_source(source) for source in sources]}
        if request.url.path.endswith('/sources'):
            from .source_inventory import input_inventory
            result.update(await input_inventory(_context(request).pool,collection_id,sources))
        return result

    @app.patch('/api/collections/{collection_id}/sources/{source_id}')
    @app.patch('/api/collections/{collection_id}/web-sources/{source_id}')
    async def update_source(collection_id: str, source_id: UUID, body: SourceUpdate, request: Request):
        if not body.model_fields_set or any(getattr(body, field) is None for field in body.model_fields_set):
            raise HTTPException(422, 'Provide a schedule or paused state')
        store = await registry(request, collection_id)
        try:
            detailed=bool(body.model_fields_set & {'name','seed_urls','policy','location','expected_version'}) or '/sources/' in request.url.path
            if detailed:
                if body.expected_version is None:raise ValueError('Source version required')
                current=next((s for s in await store.list(collection_id) if str(s['source_id'])==str(source_id)),None)
                if current is None:raise Conflict('Source is unavailable')
                from .source_edit import validate_source_edit
                from .location_registry import active_roots,database_registry_enabled
                roots=await active_roots(_context(request).pool) if body.location is not None and database_registry_enabled() else None
                changes=validate_source_edit(current,body,roots=roots)
                source=await store.update_details(source_id,collection_id,expected_version=body.expected_version,changes=changes)
            else:
                source = await store.update(source_id, collection_id,
                    schedule=body.schedule.model_dump() if body.schedule else None, paused=body.paused,**({'captions_enabled':body.captions_enabled} if body.captions_enabled is not None else {}))
        except (ValueError, Conflict) as error:
            failed(error)
        return {'source': public_source(source)}

    @app.post('/api/collections/{collection_id}/sources/{source_id}/check', status_code=202)
    @app.post('/api/collections/{collection_id}/web-sources/{source_id}/check', status_code=202)
    async def check_source(collection_id: str, source_id: UUID, request: Request):
        store = await registry(request, collection_id)
        try:
            run = await store.request(source_id, collection_id)
        except (ValueError, Conflict) as error:
            failed(error)
        return {'run': public_run(run)}

    @app.get('/api/collections/{collection_id}/sources/{source_id}/runs')
    @app.get('/api/collections/{collection_id}/web-sources/{source_id}/runs')
    async def source_runs(collection_id: str, source_id: UUID, request: Request):
        store = await registry(request, collection_id)
        try:
            runs = await store.history(source_id, collection_id)
        except (ValueError, Conflict) as error:
            failed(error)
        return {'runs': [public_run(run) for run in runs]}
