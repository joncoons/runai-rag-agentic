"""Application API: durable intake and publication intents, never direct VDB writes."""
import asyncio
from contextlib import asynccontextmanager
import hashlib
import os
import json
import logging
from pathlib import Path
import re
from typing import Literal
from uuid import UUID, uuid4
from urllib.parse import quote, unquote, urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictStr, field_validator, model_validator
from starlette.datastructures import UploadFile

from collection_control.admission_repository import ProvenanceRepository
from collection_control.artifacts import canonical
from collection_control.pinned_connection import PinnedPool
from collection_control.repository import Conflict, decoded


MAX_FILE_BYTES = 128 * 1024 * 1024
MAX_TOTAL_BYTES = 256 * 1024 * 1024
MAX_REQUEST_BYTES = MAX_TOTAL_BYTES + 1024 * 1024
MAX_FILES = 16
LOG = logging.getLogger(__name__)


class BodyLimitMiddleware:
    """Bound bytes while receiving, including multipart requests without Content-Length."""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http' or scope.get('method') not in ('POST', 'PUT', 'PATCH'):
            return await self.app(scope, receive, send)
        limit = MAX_REQUEST_BYTES if (scope.get('path', '').endswith('/ingestions') or scope.get('path') == '/api/analysis/workspaces') else 2 * 1024 * 1024
        headers = dict(scope.get('headers', []))
        try:
            length = int(headers.get(b'content-length', b'0'))
            if length < 0:
                raise ValueError
        except ValueError:
            return await JSONResponse({'detail': 'Invalid Content-Length'}, status_code=400)(scope, receive, send)
        if length > limit:
            return await JSONResponse({'detail': 'Request exceeds the upload limit'}, status_code=413)(scope, receive, send)
        received = 0
        async def bounded_receive():
            nonlocal received
            message = await receive()
            if message['type'] == 'http.request':
                received += len(message.get('body', b''))
                if received > limit:
                    raise HTTPException(413, 'Request exceeds the upload limit')
            return message
        await self.app(scope, bounded_receive, send)


class CreateCollection(BaseModel):
    model_config = ConfigDict(extra='forbid')
    name: str = Field(min_length=1, max_length=120, strict=True)
    also_write_milvus: StrictBool = False

    @field_validator('name')
    @classmethod
    def clean_name(cls, value):
        value = value.strip()
        if not value or any(ord(c) < 32 for c in value):
            raise ValueError('Enter a nonempty collection name without control characters')
        return value


class EnableMilvus(BaseModel):
    model_config = ConfigDict(extra='forbid')
    enabled: StrictBool

    @field_validator('enabled')
    @classmethod
    def enabled_only(cls, value):
        if not value:
            raise ValueError('This action only enables Milvus')
        return value


class SearchRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    query: str = Field(min_length=1, max_length=131072, strict=True)
    collection_ids: list[str] = Field(default_factory=list, max_length=12)
    backend: Literal['elasticsearch', 'milvus'] = 'elasticsearch'
    mode: Literal['search', 'deep_research'] = 'search'

    @field_validator('collection_ids')
    @classmethod
    def explicit_scope(cls, value):
        if len(value) != len(set(value)) or any(not c or len(c) > 255 or any(ord(ch) < 32 for ch in c) for c in value):
            raise ValueError('Provide distinct, valid collection IDs')
        return value



WEB_EXTENSIONS = {'.pdf', '.docx', '.pptx', '.csv', '.xls', '.xlsx', '.md', '.markdown', '.txt', '.png', '.jpg', '.jpeg', '.webp', '.tif', '.tiff', '.bmp', '.svg', '.gif', '.avif'}


class WebIngestion(BaseModel):
    model_config = ConfigDict(extra='forbid')
    seed_urls: list[StrictStr] = Field(min_length=1, max_length=5)
    allowed_hosts: list[StrictStr] = Field(min_length=1, max_length=10)
    max_depth: int | None = Field(default=1, ge=0, strict=True)
    max_pages: int | None = Field(default=None, ge=1, strict=True)
    page_prefixes: list[StrictStr] = Field(default_factory=list, max_length=20)
    max_asset_response_bytes: int = Field(default=128*1024*1024,ge=1,strict=True)
    prefer_https: StrictBool = False
    include_images: StrictBool = False
    captions_enabled: StrictBool = False
    included_extensions: list[StrictStr] = Field(default_factory=lambda: ['.pdf', '.docx', '.pptx', '.csv', '.xls', '.xlsx', '.md', '.txt'], max_length=10)
    ignored_extensions: list[StrictStr] = Field(default_factory=list, max_length=10)

    @model_validator(mode='after')
    def caption_scope(self):
        if self.captions_enabled and not self.include_images:raise ValueError('Enable image acquisition before requesting webpage captions')
        return self

    @field_validator('allowed_hosts')
    @classmethod
    def exact_hosts(cls, value):
        if len(value) != len(set(value)) or any(len(h) > 253 or not re.fullmatch(r'[a-zA-Z0-9](?:[a-zA-Z0-9.-]*[a-zA-Z0-9])?', h) or '..' in h for h in value):
            raise ValueError('Use distinct exact hostnames without schemes, paths or wildcards')
        return value

    @field_validator('seed_urls')
    @classmethod
    def bounded_seeds(cls, value):
        if len(value) != len(set(value)) or any(len(url) > 2048 for url in value):
            raise ValueError('Use distinct seed URLs of at most 2048 characters')
        return value

    @field_validator('included_extensions', 'ignored_extensions')
    @classmethod
    def admitted_extensions(cls, value):
        if len(value) != len(set(value)) or any(ext not in WEB_EXTENSIONS for ext in value):
            raise ValueError('Unsupported or duplicate linked file extension')
        return value

def _source_name(row):
    captured=row.get('acquisition')
    if isinstance(captured,str):
        try:captured=json.loads(captured)
        except (TypeError,ValueError):captured=None
    return row['canonical_uri'] if row.get('web_role') or isinstance(captured,dict) and captured.get('kind')=='location' else row['filename']

def _context(request):
    context = getattr(request.app.state, 'context', None)
    if context is None:
        raise HTTPException(503, 'Application context is not ready')
    return context


async def _collection(connection, collection_id, *, lock=False, include_deleted=False):
    row = await connection.fetchrow(
        'SELECT c.*,a.name,a.requested_milvus FROM collection_control.collections c '
        'JOIN application.collections a USING(collection_id) WHERE c.collection_id=$1'
        + (' FOR UPDATE OF c' if lock else ''), collection_id)
    if row is None or (row['deleted'] and not include_deleted):
        raise HTTPException(404, 'Collection is unavailable')
    return row


def _control(context, connection):
    return ProvenanceRepository(PinnedPool(connection),
        handle_prefix=getattr(context.control, 'handle_prefix', 'rag_agentic'))


def _safe_error(value):
    if isinstance(value, dict):
        value = value.get('error_type') or value.get('type') or value.get('last_error_type')
    if isinstance(value, str) and re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,79}', value):
        return 'Processing failed (' + value + ').'
    return 'Processing failed; review ingestion and operation status.'


def _safe_errors(value):
    try:
        errors = decoded(value) if value else []
    except (ValueError, TypeError):
        errors = []
    if not isinstance(errors, list):
        errors = [errors]
    return list(dict.fromkeys(_safe_error(error) for error in errors))


from application.gui_health import GUI_SOURCE_CTES,CAPTION_ROWS,CAPTION_STATES,SOURCE_ROWS,visible_captions,visible_job_summary,image_origin_summary,JOB_CAPTION_SCOPE

COLLECTIONS_SQL = GUI_SOURCE_CTES + ''', latest AS (
    SELECT DISTINCT ON (collection_id,source_id) collection_id,source_id,document_id,status,web_role
    FROM gui_sources ORDER BY collection_id,source_id,observed_at DESC,source_revision_id DESC
), extracted_documents AS MATERIALIZED (
 SELECT DISTINCT document_id FROM public.stage_units WHERE stage='extraction' AND status='committed'
), captions AS (''' + CAPTION_ROWS + ''')
SELECT a.name,a.requested_milvus,c.collection_id,c.revision,c.milvus_enabled,c.current_operation,
    (SELECT count(*) FROM latest r WHERE r.collection_id=c.collection_id AND r.web_role IS DISTINCT FROM 'image') AS documents,
    (SELECT count(DISTINCT source_id) FROM gui_sources r WHERE r.collection_id=c.collection_id AND r.source_revision_id=r.current_revision_id AND r.status='published' AND r.web_role IS DISTINCT FROM 'image') AS published_documents,
    (SELECT count(*) FROM latest r WHERE r.collection_id=c.collection_id AND r.status IN ('queued','admitting','admitted','publishing') AND r.web_role IS DISTINCT FROM 'image') AS source_pending,
    (SELECT count(*) FROM latest r WHERE r.collection_id=c.collection_id AND r.status IN ('queued','admitting','admitted','publishing') AND r.web_role='image') AS image_sources_pending,
    (SELECT count(*) FROM latest r WHERE r.collection_id=c.collection_id AND r.status='failed' AND r.web_role='image') AS image_sources_failed,
    (SELECT count(*) FROM latest r WHERE r.collection_id=c.collection_id AND r.status='queued' AND r.web_role IS DISTINCT FROM 'image' AND r.document_id IN (SELECT document_id FROM extracted_documents)) AS admission_waiting,
    (SELECT count(*) FROM latest r WHERE r.collection_id=c.collection_id AND r.status='queued' AND r.web_role='image' AND r.document_id IN (SELECT document_id FROM extracted_documents)) AS image_admission_waiting,
    (SELECT count(*) FROM latest r WHERE r.collection_id=c.collection_id AND r.status='failed' AND r.web_role IS DISTINCT FROM 'image') AS source_failed,
    EXISTS(SELECT 1 FROM collection_control.operations o WHERE o.collection_id=c.collection_id AND o.status='pending') AS mutation_pending,
    (SELECT count(*) FROM collection_control.operations o WHERE o.collection_id=c.collection_id AND o.status='pending') AS mutation_pending_count,
    (SELECT count(*) FROM captions p WHERE p.collection_id=c.collection_id AND p.status IN ('queued','processing','admitting','publishing')) AS caption_pending,
    (SELECT count(*) FROM captions p WHERE p.collection_id=c.collection_id AND p.status='failed') AS caption_failed,
    (SELECT COALESCE(jsonb_agg(jsonb_build_object('stage_key',p.stage_key,'asset_sha256',p.asset_sha256,'status',p.status,'inference_status',p.inference_status,'origin',CASE WHEN p.source_kind='pdf' THEN 'pdf' WHEN p.web_role='image' THEN 'webpage' ELSE 'other' END) || CASE WHEN p.status='failed' THEN jsonb_build_object('manifest_uri',p.manifest_uri,'manifest_sha256',p.manifest_sha256,'asset_sha256',p.asset_sha256) ELSE '{}'::jsonb END),'[]'::jsonb) FROM captions p WHERE p.collection_id=c.collection_id) AS caption_origin_rows
FROM application.collections a JOIN collection_control.collections c USING(collection_id)
WHERE NOT c.deleted ORDER BY a.created_at,c.collection_id'''


def create_app(*, context=None, query_service=None, submitter=None, web_submitter=None):
    @asynccontextmanager
    async def lifespan(app):
        if context is not None:
            yield
            return
        from .context import Context
        from .settings import Settings
        owned = await Context(Settings.load()).open()
        app.state.context = owned
        owned.query_catalog=None
        try:
            if not os.environ.get('QUERY_MCP_URL','').strip():
                from .query_catalog import QueryCatalog
                owned.query_catalog=QueryCatalog(owned.pool,snapshots=owned.settings.snapshots)
                await owned.query_catalog.start()
            yield
        finally:
            if owned.query_catalog is not None:await owned.query_catalog.close()
            await owned.close()
            app.state.context = None

    app = FastAPI(title='Agentic RAG', version='1.0.0', lifespan=lifespan,
                  docs_url=None, redoc_url=None, openapi_url=None)
    app.state.context = context
    app.add_middleware(BodyLimitMiddleware)

    @app.exception_handler(Conflict)
    async def conflict_handler(request, error):
        return JSONResponse({'detail': str(error)}, status_code=409)

    @app.exception_handler(RequestValidationError)
    async def validation_handler(request, error):
        details = [{'loc': list(e['loc']), 'msg': e['msg'], 'type': e['type']} for e in error.errors()]
        return JSONResponse({'detail': details}, status_code=422)

    @app.exception_handler(Exception)
    async def unexpected_handler(request, error):
        category = type(error).__name__
        LOG.error('Application API request failed: %s', category)
        ctx = getattr(request.app.state, 'context', None)
        if ctx is not None:
            try:
                await ctx.pool.execute("INSERT INTO application.events(event_type,payload) VALUES('api_error',$1::jsonb)",
                    json.dumps({'error_type': category, 'method': request.method}))
            except Exception:
                pass
        return JSONResponse({'detail': 'Application request failed. Retry or review ingestion activity.'}, status_code=503)

    @app.get('/api/status')
    async def status(request: Request):
        ctx = _context(request)
        await ctx.pool.fetchval('SELECT 1')
        return {'status': 'ok', 'components': {'database': 'available', 'models': 'not_probed',
            'elasticsearch': 'not_probed', 'milvus': 'not_probed'}}

    @app.get('/api/collections')
    async def collections(request: Request):
        rows = [dict(row) for row in await _context(request).pool.fetch(COLLECTIONS_SQL)]
        for row in rows:
            origins=row.get('caption_origin_rows') or []
            row['caption_origin_rows']=json.loads(origins) if isinstance(origins,str) else origins
        result = []
        failed_counts=None;visible_failed_keys=None
        if any(row['caption_failed'] for row in rows):
            failed=[{**caption,'collection_id':row['collection_id']} for row in rows for caption in row['caption_origin_rows'] if caption['status']=='failed']
            visible=await visible_captions(_context(request).settings,failed)
            failed_counts={};visible_failed_keys={item['stage_key'] for item in visible}
            for item in visible:failed_counts[item['collection_id']]=failed_counts.get(item['collection_id'],0)+1
        for row in rows:
            state = ('ready' if row['current_operation'] else 'indexing' if row['mutation_pending'] or row['source_pending'] else
                     'attention' if row['source_failed'] else 'empty')
            origin_rows=row.get('caption_origin_rows') or []
            if isinstance(origin_rows,str):origin_rows=json.loads(origin_rows)
            origin_summary=image_origin_summary(origin_rows,visible_failed_keys)
            result.append({'id': row['collection_id'], 'name': row['name'],
                'also_write_milvus': row['requested_milvus'] or row['milvus_enabled'],
                'milvus_status': 'ready' if row['milvus_enabled'] and row['current_operation'] else
                                 'empty' if row['milvus_enabled'] and not row['current_operation'] else
                                 'pending' if row['requested_milvus'] else 'not_enabled',
                'status': state, 'generation': row['revision'], 'documents': row['documents'],
                'published_documents':row.get('published_documents',row['documents']),
                'elasticsearch_status':'ready' if row['current_operation'] else 'indexing' if row['mutation_pending'] else 'empty',
                'processing':{'sources_pending':row['source_pending'],'image_sources_pending':row.get('image_sources_pending',0),'image_sources_failed':row.get('image_sources_failed',0),'sources_failed':row['source_failed'],'admission_waiting':row.get('admission_waiting',0),'image_admission_waiting':row.get('image_admission_waiting',0),'captions_pending':sum(v['queued']+v['generating'] for v in origin_summary.values()),'captions_awaiting_admission':sum(v['admitting'] for v in origin_summary.values()),'captions_awaiting_publication':sum(v['publishing'] for v in origin_summary.values()),'captions_generated':sum(v['generated'] for v in origin_summary.values()),'publication_pending':row['mutation_pending'],'publication_pending_operations':row.get('mutation_pending_count',int(bool(row['mutation_pending'])))},
                'image_origins':origin_summary,
                'caption_pending': row['caption_pending'], 'caption_failed': failed_counts.get(row['collection_id'],0) if failed_counts is not None else row['caption_failed']})
        return {'collections': result}

    @app.post('/api/collections', status_code=201)
    async def create_collection(body: CreateCollection, request: Request):
        ctx = _context(request)
        collection_id = 'collection-' + uuid4().hex
        contract_sha = hashlib.sha256(canonical(ctx.settings.contract)).hexdigest()
        async with ctx.pool.acquire() as connection, connection.transaction():
            await _control(ctx, connection).create_collection(collection_id, contract_sha)
            await connection.execute('UPDATE collection_control.collections SET milvus_enabled=$2 WHERE collection_id=$1',
                                     collection_id, body.also_write_milvus)
            await connection.execute('INSERT INTO application.collections(collection_id,name,requested_milvus) VALUES($1,$2,$3)',
                                     collection_id, body.name, body.also_write_milvus)
            await connection.execute("INSERT INTO application.events(event_type,payload) VALUES('collection_created',$1::jsonb)",
                json.dumps({'collection_id': collection_id, 'also_write_milvus': body.also_write_milvus}))
        return {'id': collection_id, 'name': body.name, 'also_write_milvus': body.also_write_milvus,
                'status': 'empty', 'generation': 0, 'documents': 0, 'caption_pending': 0, 'caption_failed': 0}

    @app.delete('/api/collections/{collection_id}', status_code=202)
    async def delete_collection(collection_id: str, request: Request):
        ctx = _context(request)
        async with ctx.pool.acquire() as connection, connection.transaction():
            row = await _collection(connection, collection_id, lock=True, include_deleted=True)
            await _control(ctx, connection).delete_collection(collection_id)
            if not row['deleted']:
                # Match caption scheduling's asset-before-parent lock order.
                await connection.execute("UPDATE caption_control.assets a SET terminal_reason=COALESCE(terminal_reason,'CollectionDeleted'),updated_at=clock_timestamp() "
                    "WHERE collection_id=$1 AND NOT EXISTS(SELECT 1 FROM application.caption_publications p WHERE p.caption_stage_key=a.stage_key AND p.status='published')", collection_id)
                # Parent cancellation fences new and already running caption commits.
                await connection.execute("UPDATE public.jobs SET status='cancelled',updated_at=clock_timestamp() "
                    'WHERE job_id IN (SELECT job_id FROM application.jobs WHERE collection_id=$1)', collection_id)
                await connection.execute("UPDATE public.documents SET status='cancelled',updated_at=clock_timestamp() "
                    'WHERE job_id IN (SELECT job_id FROM application.jobs WHERE collection_id=$1)', collection_id)
                await connection.execute("UPDATE application.jobs SET status='cancelled',stage='cancelled',updated_at=clock_timestamp() WHERE collection_id=$1", collection_id)
                await connection.execute("UPDATE application.source_revisions SET status='cancelled' WHERE collection_id=$1 AND status NOT IN ('published','captured')", collection_id)
                await connection.execute("INSERT INTO application.events(event_type,payload) VALUES('collection_deletion_requested',$1::jsonb)",
                    json.dumps({'collection_id': collection_id, 'physical_purge_complete': False}))
        return {'id': collection_id, 'status': 'deleted', 'physical_purge_complete': False}

    @app.post('/api/collections/{collection_id}/milvus', status_code=202)
    async def enable_milvus(collection_id: str, body: EnableMilvus, request: Request):
        ctx = _context(request)
        async with ctx.pool.acquire() as connection, connection.transaction():
            row = await _collection(connection, collection_id, lock=True)
            if row['milvus_enabled']:
                await connection.execute('UPDATE application.collections SET requested_milvus=true WHERE collection_id=$1', collection_id)
                return {'id': collection_id, 'status': 'enabled', 'also_write_milvus': True}
            pending = await connection.fetchval("SELECT EXISTS(SELECT 1 FROM collection_control.operations WHERE collection_id=$1 AND status='pending')", collection_id)
            if pending:
                raise Conflict('Another collection mutation is pending. Retry Milvus duplication after it finishes.')
            operation = None
            if row['current_operation']:
                current = await connection.fetchrow("SELECT artifact_sha256 FROM collection_control.operations WHERE operation_id=$1 AND status='published'", row['current_operation'])
                if current is None:
                    raise Conflict('Current publication is unavailable')
                operation = await _control(ctx, connection).prepare(collection_id,
                    'enable-milvus:' + str(row['revision']), 'enroll_milvus', row['revision'],
                    current['artifact_sha256'], row['contract_sha256'])
            else:
                await connection.execute('UPDATE collection_control.collections SET milvus_enabled=true,target_epoch=target_epoch+1 WHERE collection_id=$1', collection_id)
            await connection.execute('UPDATE application.collections SET requested_milvus=true WHERE collection_id=$1', collection_id)
            await connection.execute("INSERT INTO application.events(event_type,payload) VALUES('milvus_requested',$1::jsonb)",
                json.dumps({'collection_id': collection_id, 'operation_id': str(operation['operation_id']) if operation else None}))
        return {'id': collection_id, 'status': 'pending' if operation else 'waiting_for_content',
                'operation_id': str(operation['operation_id']) if operation else None, 'also_write_milvus': True}

    @app.post('/api/collections/{collection_id}/ingestions', status_code=202)
    async def ingest(collection_id: str, request: Request):
        ctx = _context(request)
        await _collection(ctx.pool, collection_id)
        if not request.headers.get('content-type', '').lower().startswith('multipart/form-data'):
            raise HTTPException(415, 'Upload files using multipart/form-data')
        async with request.form(max_files=MAX_FILES, max_fields=4, max_part_size=1024) as form:
            if set(form.keys()) - {'files', 'captions_enabled'}:
                raise HTTPException(422, 'Unsupported upload fields')
            options = form.getlist('captions_enabled')
            if len(options) > 1 or (options and options[0] not in ('true', 'false')):
                raise HTTPException(422, 'captions_enabled must be exactly true or false')
            captions_enabled = bool(options and options[0] == 'true')
            files = form.getlist('files')
            if not files or len(files) > MAX_FILES:
                raise HTTPException(422, 'Choose between 1 and 16 documents')
            uploads, total = [], 0
            for file in files:
                if not isinstance(file, UploadFile):
                    raise HTTPException(422, 'Every files field must be an uploaded document')
                filename = file.filename or ''
                if (not filename or len(filename) > 255 or Path(filename).suffix.lower() not in {'.pdf','.docx','.pptx','.csv','.xls','.xlsx','.md','.markdown','.txt'}
                        or '/' in filename or '\\' in filename or any(ord(ch) < 32 for ch in filename)):
                    raise HTTPException(422, 'Choose supported document filenames without directory paths')
                parts, size = [], 0
                while chunk := await file.read(1024 * 1024):
                    size += len(chunk)
                    total += len(chunk)
                    if size > MAX_FILE_BYTES:
                        raise HTTPException(413, 'A document exceeds the 128 MiB file limit')
                    if total > MAX_TOTAL_BYTES:
                        raise HTTPException(413, 'Uploads exceed the 256 MiB aggregate limit')
                    parts.append(chunk)
                if not size:
                    raise HTTPException(422, 'Empty documents cannot be uploaded')
                uploads.append((filename, b''.join(parts)))
            if submitter is None:
                from .intake import submit
                enqueue = submit
            else:
                enqueue = submitter
            try:
                job_id = await enqueue(ctx, collection_id, uploads, captions_enabled=captions_enabled)
            except ValueError as error:
                raise HTTPException(422, str(error)) from error
        return {'job_id': str(job_id)}

    @app.post('/api/collections/{collection_id}/web-ingestions', status_code=202)
    async def ingest_website(collection_id: str, body: WebIngestion, request: Request):
        from .web_acquisition import AcquisitionPolicy, collect, validate_url
        ctx = _context(request)
        await _collection(ctx.pool, collection_id)
        policy = AcquisitionPolicy(private_hosts=tuple(h for h in body.allowed_hosts if h.lower() in {v.strip().lower() for v in os.environ.get('WEB_ASSET_PRIVATE_HOSTS','').split(',') if v.strip()}),private_ports=tuple(int(v) for v in os.environ.get('WEB_ASSET_PRIVATE_PORTS','').split(',') if v.strip()),allowed_hosts=tuple(body.allowed_hosts), max_depth=body.max_depth,
            max_pages=body.max_pages, max_asset_response_bytes=body.max_asset_response_bytes, prefer_https=body.prefer_https, page_prefixes=tuple(body.page_prefixes), include_images=body.include_images,
            included_extensions=tuple(body.included_extensions), ignored_extensions=tuple(body.ignored_extensions))
        try:
            for seed in body.seed_urls:
                validate_url(seed, policy)
            if web_submitter is None:
                from .intake import submit_web_text
                from .web_acquisition import CrawlCapture
                observation_id=__import__('uuid').uuid4().hex;jobs=[]
                async def accept_pages(pages):
                    accepted=await submit_web_text(ctx,collection_id,CrawlCapture(tuple(pages),(),True),policy,
                        observation_id=observation_id,captions_enabled=body.captions_enabled)
                    jobs.extend(j for j in accepted if j not in jobs)
                capture=await collect(body.seed_urls,policy,page_sink=accept_pages)
                if not jobs:raise ValueError('No HTML page was durably admitted')
                return {'job_id':jobs[0],'job_ids':jobs,'pages':len(capture.pages),
                    'asset_status':'queued_independently','capture_complete':capture.complete,
                    'capture_errors':list(capture.errors),'scheduled_rechecks':False}
            capture = await collect(body.seed_urls, policy)
            if not capture.complete or not capture.pages:
                raise ValueError('Website capture was incomplete or empty. Narrow the scope or correct unavailable pages and retry.')
            if web_submitter is None:
                from .intake import submit_web
                enqueue = submit_web
            else:
                enqueue = web_submitter
            job_id = await enqueue(ctx, collection_id, capture, policy, captions_enabled=body.captions_enabled) if body.captions_enabled else await enqueue(ctx, collection_id, capture, policy)
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        return {'job_id': str(job_id), 'pages': len(capture.pages),
            'asset_status': 'native_ingestion', 'scheduled_rechecks': False}

    @app.get('/api/collections/{collection_id}/assets/{document_id}')
    async def retained_web_asset(collection_id: str, document_id: str, request: Request, operation_id: UUID | None = None):
        from collection_control.web_groups import resolve_graph
        from collection_control.provenance import full_hash, verified_file
        ctx = _context(request)
        collection = await _collection(ctx.pool, collection_id)
        if operation_id is not None and str(operation_id) != str(collection.get('current_operation')):
            raise HTTPException(409, 'The requested asset generation is no longer current')
        try:
            graph = await resolve_graph(ctx.pool, collection)
            matches = [asset for page in graph['pages'] for asset in page['assets']
                       if str(asset['document_id']) == document_id]
            if not matches:
                raise HTTPException(404, 'Asset is unavailable in the published collection')
            asset = matches[0]
            if any((a['raw_uri'], a['raw_sha256'], a['media_type']) !=
                   (asset['raw_uri'], asset['raw_sha256'], asset['media_type']) for a in matches):
                raise Conflict('Conflicting published asset')
            raw = await asyncio.to_thread(verified_file, ctx.settings.raw,
                asset['raw_uri'], full_hash(asset['raw_sha256']))
        except (Conflict, KeyError, ValueError, OSError) as error:
            raise HTTPException(409, 'Published asset verification is unavailable') from error
        current = await _collection(ctx.pool, collection_id)
        if current.get('current_operation') != collection.get('current_operation'):
            raise HTTPException(409, 'The published asset generation changed during verification')
        name = Path(unquote(urlsplit(asset.get('canonical_uri', '')).path)).name
        if not name or name in ('.', '..') or any(ord(c) < 32 or ord(c) == 127 for c in name) or '\\' in name:
            name = 'asset-' + re.sub(r'[^A-Za-z0-9_-]', '_', document_id)[:120]
        return Response(raw, media_type=asset['media_type'], headers={
            'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff',
            'Content-Disposition': "attachment; filename*=UTF-8''" + quote(name, safe=''),
        })

    @app.get('/api/jobs/{job_id}')
    async def job(job_id: UUID, request: Request):
        ctx = _context(request)
        row = await ctx.pool.fetchrow('SELECT j.*,c.deleted FROM application.jobs j '
            'JOIN application.collections a USING(collection_id) '
            'JOIN collection_control.collections c USING(collection_id) WHERE j.job_id=$1', job_id)
        if row is None:
            raise HTTPException(404, 'Ingestion job is unavailable')
        documents = await ctx.pool.fetch(SOURCE_ROWS+' WHERE job_id=$1 ORDER BY filename,document_id', job_id)
        captions = await ctx.pool.fetch(CAPTION_STATES + JOB_CAPTION_SCOPE + ' ORDER BY a.created_at,a.stage_key', job_id)
        if row['deleted']:documents=[];captions=[]
        else:captions=await visible_captions(ctx.settings,captions)
        relevant=bool(documents or captions)
        if not relevant and not row['deleted'] and row['status'] in ('queued','processing'):
            relevant=not await ctx.pool.fetchval('SELECT EXISTS(SELECT 1 FROM application.source_revisions WHERE job_id=$1)',job_id)
        total_sources=await ctx.pool.fetchval('SELECT count(*) FROM application.source_revisions WHERE job_id=$1',job_id)
        display_status,display_stage,display_errors=visible_job_summary(row,documents,total_sources>len(documents))
        images, counts = [], {'pending': 0, 'completed': 0, 'failed': 0, 'generated':0, 'generating':0, 'admitting':0, 'publishing':0}
        for image in captions:
            state = image['status']
            if image.get('inference_status')=='committed' or state in ('admitting','publishing','completed'):counts['generated']+=1
            if state=='processing':counts['generating']+=1
            if state=='admitting':counts['admitting']+=1
            if state=='publishing':counts['publishing']+=1
            if row['deleted'] and state != 'completed':
                state = 'cancelled'
            if state in ('completed', 'failed'):
                counts[state] += 1
            elif state != 'cancelled':
                counts['pending'] += 1
            item = {'id': image['stage_key'], 'name': image['filename'] + ' / ' + image['stage_key'][-12:],
                    'document_id': image['document_id'], 'status': state}
            if state == 'failed':
                item['error'] = _safe_error(image['terminal_reason'])
            images.append(item)
        operation_ids = {document['operation_id'] for document in documents if document['operation_id']}
        operation_ids.update(image['operation_id'] for image in captions if image['operation_id'])
        operations = []
        if operation_ids:
            rows = await ctx.pool.fetch('SELECT operation_id,status,targets,fence FROM collection_control.operations WHERE collection_id=$1 AND operation_id=ANY($2::uuid[]) ORDER BY operation_id', row['collection_id'], list(operation_ids))
            for operation in rows:
                receipts = await ctx.pool.fetch('SELECT backend,fence,content_sha256,row_count,visible_at FROM collection_control.receipts WHERE operation_id=$1 AND fence=$2 ORDER BY backend', operation['operation_id'], operation['fence'])
                operations.append({'id': str(operation['operation_id']), 'status': operation['status'],
                    'targets': list(operation['targets']), 'receipts': [dict(receipt) for receipt in receipts]})
        return {'id': str(job_id), 'collection_id':row['collection_id'],'relevant':relevant, 'status': 'cancelled' if row['deleted'] else display_status,
                'stage': 'cancelled' if row['deleted'] else display_stage,
                'documents': [{'name': _source_name(d),
                               'status': d['status'], 'records': d['records']} for d in documents if d.get('web_role') != 'image'],
                'assets': [{'id': str(d['document_id']), 'url': d['canonical_uri'], 'kind': d['web_role'],
                            'status': 'processed' if d['web_role'] == 'image' and d['status'] in ('captured', 'published') else d['status'],
                            'records': 0 if d['web_role'] == 'image' else d['records']}
                           for d in documents if d.get('web_role') in ('image', 'binary')],
                'captions': {**counts, 'images': images}, 'errors': (_safe_errors(row['errors']) if display_errors is None else display_errors) if relevant else [], 'operations': operations}

    def search_service(request):
        ctx = _context(request)
        service = query_service
        if service is None:
            from .query import RAGQuery
            service = RAGQuery(ctx.settings, ctx.pool, ctx.http,catalog=getattr(ctx,'query_catalog',None))
        return service

    @app.post('/api/search')
    async def search(body: SearchRequest, request: Request):
        service=search_service(request)
        return await service.search(body.query, body.collection_ids, backend=body.backend, mode=body.mode)

    from application.search_stream_api import install_stream_routes
    install_stream_routes(app,search_service)

    from application.promotions import install_promotion_routes
    install_promotion_routes(app)
    from application.private_api import install_private_routes
    install_private_routes(app)
    from application.research_api import install_research_routes
    install_research_routes(app,search_service)
    from application.local_auth import install_auth_routes
    install_auth_routes(app)
    from application.admin_locations import install_admin_routes
    install_admin_routes(app)

    static_root = Path(__file__).parent / 'static'
    app.mount('/static', StaticFiles(directory=static_root), name='static')
    asset_versions={name:hashlib.sha256((static_root/name).read_bytes()).hexdigest()[:16] for name in ('app.js','app.css','admin.js','search-stream.js','workflows.js')}
    @app.get('/')
    async def index():
        body=(static_root/'index.html').read_text()
        for name,version in asset_versions.items():body=body.replace('/static/'+name+'\"','/static/'+name+'?v='+version+'\"')
        return HTMLResponse(body,headers={'Cache-Control':'no-store'})

    from application.source_api import install_source_routes
    install_source_routes(app)
    from application.location_api import install_location_routes
    install_location_routes(app)
    return app


app = create_app()


if __name__ == '__main__':
    import uvicorn
    uvicorn.run('application.api:app', host='0.0.0.0', port=8080)
