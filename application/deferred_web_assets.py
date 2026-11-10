"""Durable URL acquisition, separate from native extraction stage envelopes.

Discovery belongs to the caller's page intake transaction. Redis notifications
are advisory; PostgreSQL owns retry times, leases and completion authority.
A callback must submit native extraction using the supplied transaction.
"""
from __future__ import annotations

import asyncio
from dataclasses import asdict, is_dataclass, replace
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
from uuid import UUID, uuid4, uuid5

from collection_control.artifacts import canonical
from collection_control.provenance import verified_file
from collection_control.repository import Conflict, Fenced
from nv_ingest_durable.deferred_caption import retain
from .web_acquisition import AcquisitionPolicy, Asset, validate_url
from .web_asset_acquisition import AssetCapture, acquire_asset

STREAM = 'rag.agentic.web-acquisition.pending'
_NAMESPACE = UUID('c82fe8a1-46be-4f1f-b4ad-55c65e75283d')
_INACTIVE = ('failed', 'cancelled', 'canceled', 'deleted', 'superseded')


def public_policy(policy):
    value = asdict(policy) if is_dataclass(policy) else dict(policy)
    value.pop('private_hosts', None)
    value.pop('private_ports', None)
    return value


def download_identity(observation_id, canonical_uri, kind, policy):
    """Observation IDs must be collection-scoped (e.g. collection + run UUID)."""
    return uuid5(_NAMESPACE, canonical({'observation_id': str(observation_id),
        'requested_uri': canonical_uri, 'kind': kind, 'policy': public_policy(policy)}).decode())


def runtime_policy(policy):
    value = public_policy(policy)
    trusted = {h.strip().lower() for h in os.environ.get('WEB_ASSET_PRIVATE_HOSTS', '').split(',') if h.strip()}
    value['private_hosts'] = tuple(h for h in value.get('allowed_hosts', ()) if h.lower() in trusted)
    value['private_ports'] = tuple(int(v) for v in os.environ.get('WEB_ASSET_PRIVATE_PORTS', '').split(',') if v.strip())
    return AcquisitionPolicy(**value)


def retry_delay(attempts):
    if isinstance(attempts, bool) or attempts not in (1, 2, 3, 4):
        raise ValueError('Acquisition attempts must be between one and four')
    return (5, 30, 120, None)[attempts - 1]


def _row(row):
    if row is None: return None
    result = dict(row)
    if isinstance(result.get('policy'), str): result['policy'] = json.loads(result['policy'])
    return result


class DeferredWebAssets:
    def __init__(self, pool, raw_root, *, policy_resolver=None):
        self.pool, self.raw_root = pool, Path(raw_root)
        # A runtime resolver may restore server-controlled private host allowances;
        # private scope is never trusted from persisted caller policy.
        self.policy_resolver = policy_resolver or runtime_policy

    async def _collection(self, c, collection_id, *, exclusive=True):
        col = await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1'+(' FOR UPDATE' if exclusive else ''), collection_id)
        if not col or col['deleted']: raise Fenced('Asset collection is unavailable')

    async def _references(self, c, task):
        # Delivery/lease bookkeeping does not grant model or VDB authority.
        # Read source generations in one statement; native completion holds the
        # collection lock, which also fences source updates and deletion.
        rows=await c.fetch("""SELECT r.*,p.web_group_id AS parent_group_id,p.canonical_uri AS parent_canonical_uri
            FROM application.web_asset_references r
            JOIN application.source_revisions p ON p.source_revision_id=r.parent_source_revision_id
            JOIN application.jobs j ON j.job_id=r.parent_job_id
            JOIN public.jobs pj ON pj.job_id=r.parent_job_id
            JOIN public.documents d ON d.document_id=r.parent_document_id
            LEFT JOIN application.website_source_runs sr ON sr.run_id=r.source_run_id
            LEFT JOIN application.website_sources ws ON ws.source_id=sr.source_id
            WHERE r.download_id=$1 AND p.collection_id=$2 AND j.collection_id=$2
            AND p.document_id=r.parent_document_id AND p.job_id=r.parent_job_id
            AND NOT (p.status=ANY($3::text[])) AND NOT (j.status=ANY($3::text[]))
            AND NOT (pj.status=ANY($3::text[])) AND NOT (d.status=ANY($3::text[]))
            AND NOT EXISTS(SELECT 1 FROM application.source_revisions newer
                WHERE newer.source_id=p.source_id
                AND (newer.observed_at,newer.source_revision_id)>(p.observed_at,p.source_revision_id)
                AND NOT (newer.status=ANY($3::text[])))
            AND (r.source_run_id IS NULL OR (ws.source_id IS NOT NULL
                AND NOT (ws.paused AND sr.trigger='schedule')
                AND ws.version=r.source_version AND sr.source_version=r.source_version
                AND sr.collection_id=$2 AND sr.state NOT IN ('cancelled','canceled','deleted','superseded')))
            ORDER BY r.reference_id""",task['download_id'],task['collection_id'],list(_INACTIVE))
        return [dict(row) for row in rows]

    async def _notify(self, c, download_id, not_before=None):
        await c.execute('''INSERT INTO application.web_asset_acquisition_outbox(event_id,download_id,not_before)
            VALUES($1,$2,COALESCE($3,clock_timestamp()))''', uuid4(), download_id, not_before)

    async def discover(self, c, *, observation_id, collection_id, parent_source_revision_id,
                       parent_document_id, parent_job_id, policy, assets, captions_enabled=False,
                       source_run_id=None, source_version=None):
        """Register relations without downloading; caller commits page and these rows."""
        if not c.is_in_transaction(): raise ValueError('Discovery requires page intake transaction')
        if not isinstance(captions_enabled, bool): raise ValueError('Caption selection must be boolean')
        if (source_run_id is None) != (source_version is None): raise ValueError('Run generation is required with source run')
        await self._collection(c, collection_id)
        policy_value = public_policy(policy)
        parsed = self.policy_resolver(policy_value)
        policy_sha = hashlib.sha256(canonical(policy_value)).hexdigest()
        parent = await c.fetchrow('''SELECT p.*,j.status AS job_status FROM application.source_revisions p
            JOIN application.jobs j USING(job_id) WHERE p.source_revision_id=$1
            AND p.document_id=$2 AND p.job_id=$3 AND p.collection_id=$4 AND j.collection_id=$4''',
            parent_source_revision_id,parent_document_id,UUID(str(parent_job_id)),collection_id)
        if not parent or parent['status'] in _INACTIVE or parent['job_status'] in _INACTIVE:
            raise Fenced('Discovery parent is unavailable')
        result = []
        for asset in assets:
            if asset.kind not in ('image', 'binary'): raise ValueError('Unsupported acquisition kind')
            url = validate_url(asset.url, parsed)
            if validate_url(asset.parent_url, parsed) != parent['canonical_uri']:
                raise Conflict('Asset relationship differs from exact parent page')
            key = download_identity(observation_id, url, asset.kind, policy_value)
            await c.execute('''INSERT INTO application.web_asset_downloads
                (download_id,observation_id,collection_id,requested_uri,kind,policy,policy_sha256)
                VALUES($1,$2,$3,$4,$5,$6::jsonb,$7) ON CONFLICT DO NOTHING''',
                key, str(observation_id), collection_id, url, asset.kind, json.dumps(policy_value), policy_sha)
            task = _row(await c.fetchrow('SELECT * FROM application.web_asset_downloads WHERE download_id=$1 FOR UPDATE', key))
            if task['collection_id'] != collection_id or task['policy_sha256'] != policy_sha:
                raise Conflict('Download identity belongs to a different collection or policy')
            reference = uuid5(key, canonical({'parent_revision': parent_source_revision_id,
                'parent_document': parent_document_id, 'parent_job': str(parent_job_id),
                'parent_url': asset.parent_url, 'locator': asset.locator, 'alt': asset.alt,
                'captions_enabled': captions_enabled, 'source_run_id': str(source_run_id) if source_run_id else None,
                'source_version': source_version}).decode())
            inserted = await c.fetchval('''INSERT INTO application.web_asset_references
                (reference_id,download_id,parent_source_revision_id,parent_document_id,parent_job_id,
                parent_url,locator,alt,captions_enabled,source_run_id,source_version)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT DO NOTHING RETURNING reference_id''',
                reference,key,parent_source_revision_id,parent_document_id,UUID(str(parent_job_id)),
                asset.parent_url,asset.locator,asset.alt,captions_enabled,
                UUID(str(source_run_id)) if source_run_id else None,source_version)
            if inserted:
                # Completed acquisition may gain another parent. Requeue attachment
                # using retained bytes and existing child IDs, never silently forget it.
                if task['state'] == 'completed':
                    await c.execute("UPDATE application.web_asset_downloads SET state='pending',attempts=0,not_before=clock_timestamp(),updated_at=clock_timestamp() WHERE download_id=$1",key)
                await self._notify(c,key)
            result.append({'download_id':key,'reference_id':reference,'kind':asset.kind,'url':url,
                'parent_url':asset.parent_url,'locator':asset.locator,'alt':asset.alt})
        return result

    async def claim(self, owner, ttl=300, *, download_id=None):
        if not owner or len(owner)>255 or isinstance(ttl,bool) or not 0<ttl<=3600:
            raise ValueError('Invalid acquisition lease')
        rows = await self.pool.fetch('''SELECT download_id,collection_id FROM application.web_asset_downloads
            WHERE ($1::uuid IS NULL OR download_id=$1) AND
            ((state IN ('pending','retry') AND not_before<=clock_timestamp()) OR
             (state='running' AND lease_until<=clock_timestamp())) ORDER BY created_at LIMIT 64''',
            UUID(str(download_id)) if download_id else None)
        for candidate in rows:
            async with self.pool.acquire() as c, c.transaction():
                try: await self._collection(c,candidate['collection_id'],exclusive=False)
                except Fenced:
                    await c.execute("UPDATE application.web_asset_downloads SET state='cancelled',lease_until=NULL WHERE download_id=$1",candidate['download_id'])
                    continue
                task = _row(await c.fetchrow('SELECT *,clock_timestamp() AS db_now FROM application.web_asset_downloads WHERE download_id=$1 FOR UPDATE SKIP LOCKED',candidate['download_id']))
                if not task: continue
                now=task['db_now']
                due=(task['state'] in ('pending','retry') and task['not_before']<=now) or (task['state']=='running' and task['lease_until'] and task['lease_until']<=now)
                if not due: continue
                refs=await self._references(c,task)
                if not refs or task['attempts']>=4:
                    state='cancelled' if not refs else 'failed'
                    await c.execute('UPDATE application.web_asset_downloads SET state=$2,lease_until=NULL,error_code=$3 WHERE download_id=$1',task['download_id'],state,'parent_unavailable' if not refs else 'attempts_exhausted')
                    continue
                task=_row(await c.fetchrow('''UPDATE application.web_asset_downloads SET state='running',
                    fence=fence+1,attempts=attempts+1,owner=$2,lease_until=$3,updated_at=clock_timestamp()
                    WHERE download_id=$1 RETURNING *''',task['download_id'],owner,now+timedelta(seconds=ttl)))
                task['references']=refs;task['captions_enabled']=any(r['captions_enabled'] for r in refs)
                return task
        return None

    async def _leased(self,c,download_id,fence,*,exclusive=True):
        identity=await c.fetchrow('SELECT collection_id FROM application.web_asset_downloads WHERE download_id=$1',download_id)
        if not identity: raise Fenced('Unknown acquisition request')
        await self._collection(c,identity['collection_id'],exclusive=exclusive)
        task=_row(await c.fetchrow('SELECT *,lease_until>clock_timestamp() AS live FROM application.web_asset_downloads WHERE download_id=$1 FOR UPDATE',download_id))
        if task['state']!='running' or task['fence']!=fence or not task['live']:
            raise Fenced('Asset acquisition lease is stale')
        refs=await self._references(c,task)
        if not refs: raise Fenced('All asset parents are stale')
        task['references']=refs;task['captions_enabled']=any(r['captions_enabled'] for r in refs)
        return task

    async def renew(self,download_id,fence,ttl=300):
        if isinstance(ttl,bool) or not 0<ttl<=3600: raise ValueError('Invalid acquisition lease')
        async with self.pool.acquire() as c,c.transaction():
            await self._leased(c,download_id,fence,exclusive=False)
            await c.execute("UPDATE application.web_asset_downloads SET lease_until=clock_timestamp()+$2::double precision*interval '1 second' WHERE download_id=$1",download_id,ttl)

    async def complete(self,task,capture,submit_native):
        checksum=hashlib.sha256(capture.raw_bytes).hexdigest()
        if capture.requested_uri!=task['requested_uri'] or capture.kind!=task['kind'] or capture.raw_sha256.removeprefix('sha256:')!=checksum:
            raise Conflict('Downloaded asset identity or checksum differs')
        # Parsing/inspection can be expensive. Preparation has no publication
        # authority and runs before acquiring a connection or collection lock.
        # The subsequent transaction rechecks the current lease and parents.
        prepare=getattr(submit_native,'prepare',None)
        prepared=await prepare(task,capture) if prepare is not None else None
        # Files are immutable, and do not confer authority without committed PG rows.
        raw_uri,_=retain(self.raw_root,capture.raw_bytes,Path(capture.filename).suffix.lower())
        async with self.pool.acquire() as c,c.transaction():
            current=await self._leased(c,task['download_id'],task['fence'])
            if current['requested_uri']!=capture.requested_uri or current['kind']!=capture.kind:
                raise Conflict('Capture differs from authoritative download identity')
            if current['raw_sha256'] and current['raw_sha256']!=checksum:
                raise Conflict('Immutable observation acquired conflicting bytes')
            manifest={'schema_version':'deferred-web-asset-acquisition.v1','request_id':str(task['download_id']),
                'observation_id':current['observation_id'],'collection_id':current['collection_id'],
                'requested_uri':capture.requested_uri,'final_uri':capture.final_uri,'kind':capture.kind,
                'raw_uri':raw_uri,'raw_sha256':checksum,'media_type':capture.media_type,
                'filename':capture.filename,'http':capture.evidence,'policy_sha256':current['policy_sha256']}
            uri,sha=retain(self.raw_root,canonical(manifest),'.web-asset-acquisition.json')
            acquisition={'uri':uri,'sha256':sha}
            # Prepared inspection is bound to this exact capture by the callback;
            # it is never persisted as an authority-bearing task field.
            current['_native_prepared']=prepared
            # The callback must create/reuse the native child in this transaction.
            child=await submit_native(c,current,capture,acquisition)
            if not isinstance(child,dict) or not child.get('job_id') or not child.get('source_revision_id'):
                raise ValueError('Native completion must return job_id and source_revision_id')
            revision=await c.fetchrow('SELECT * FROM application.source_revisions WHERE source_revision_id=$1',child['source_revision_id'])
            if (not revision or revision['collection_id']!=current['collection_id'] or
                str(revision['job_id'])!=str(child['job_id']) or revision['raw_sha256'].removeprefix('sha256:')!=checksum
                or revision['canonical_uri']!=current['requested_uri']):
                raise Conflict('Native callback returned an unrelated asset revision')
            await c.execute('UPDATE application.source_revisions SET web_asset_download_id=$2 WHERE source_revision_id=$1',child['source_revision_id'],task['download_id'])
            publication=child.get('publication_manifest') or {}
            await c.execute('''UPDATE application.web_asset_downloads SET state='completed',lease_until=NULL,
                raw_uri=$2,raw_sha256=$3,acquisition_uri=$4,acquisition_sha256=$5,child_job_id=$6,
                child_source_revision_id=$7,publication_manifest_uri=$8,publication_manifest_sha256=$9,
                error_code=NULL,updated_at=clock_timestamp() WHERE download_id=$1''',task['download_id'],
                raw_uri,checksum,uri,sha,UUID(str(child['job_id'])),child['source_revision_id'],
                publication.get('uri') or child.get('publication_manifest_uri'),
                publication.get('sha256') or child.get('publication_manifest_sha256'))
            return child

    async def fail(self,download_id,fence,error):
        async with self.pool.acquire() as c,c.transaction():
            task=await self._leased(c,download_id,fence,exclusive=False)
            delay=retry_delay(task['attempts'])
            due=await c.fetchval("SELECT clock_timestamp()+$1::double precision*interval '1 second'",delay or 0)
            await c.execute('''UPDATE application.web_asset_downloads SET state=$2,lease_until=NULL,
                not_before=$3,error_code=$4,updated_at=clock_timestamp() WHERE download_id=$1''',
                download_id,'failed' if delay is None else 'retry',due,type(error).__name__)
            if delay is not None: await self._notify(c,download_id,due)

    def _cached(self,task):
        manifest=json.loads(verified_file(self.raw_root,task['acquisition_uri'],task['acquisition_sha256']))
        if (manifest.get('schema_version')!='deferred-web-asset-acquisition.v1' or
            manifest.get('request_id')!=str(task['download_id']) or manifest.get('raw_sha256')!=task['raw_sha256']
            or manifest.get('raw_uri')!=task['raw_uri'] or manifest.get('policy_sha256')!=task['policy_sha256']):
            raise Conflict('Retained acquisition proof differs')
        raw=verified_file(self.raw_root,task['raw_uri'],task['raw_sha256'])
        ref=task['references'][0]
        return AssetCapture(task['requested_uri'],manifest['final_uri'],raw,'sha256:'+task['raw_sha256'],
            manifest['media_type'],manifest['filename'],manifest['http'],ref['parent_url'],task['kind'],ref['locator'],ref['alt'])

    async def process_one(self,owner,submit_native,*,session=None,download_id=None,ttl=300):
        task=await self.claim(owner,ttl,download_id=download_id)
        if not task:return False
        parent=asyncio.current_task();lease_failure=[]
        async def heartbeat():
            try:
                while True:
                    await asyncio.sleep(max(0.05,ttl/3))
                    await self.renew(task['download_id'],task['fence'],ttl)
            except asyncio.CancelledError:raise
            except Exception as error:
                lease_failure.append(error);parent.cancel()
        heartbeat_task=asyncio.create_task(heartbeat())
        try:
            if task.get('raw_uri'):capture=self._cached(task)
            else:
                policy=self.policy_resolver(task['policy'])
                policy=replace(policy,max_response_bytes=policy.max_asset_response_bytes)
                ref=task['references'][0]
                asset=Asset(task['requested_uri'],task['kind'],ref['parent_url'],ref['alt'],ref['locator'])
                capture=await acquire_asset(asset,policy,session=session)
            await self.complete(task,capture,submit_native)
        except asyncio.CancelledError:
            if not lease_failure:raise
        except Fenced:pass
        except Exception as error:
            try:await self.fail(task['download_id'],task['fence'],error)
            except Fenced:pass
        finally:
            heartbeat_task.cancel()
            try:await heartbeat_task
            except asyncio.CancelledError:pass
        return True

    async def relay_once(self,redis,limit=32):
        """Redis crash after XADD before PG marking gives harmless duplicates."""
        rows=await self.pool.fetch('''SELECT e.* FROM application.web_asset_acquisition_outbox e
            JOIN application.web_asset_downloads d USING(download_id)
            WHERE e.published_at IS NULL AND e.not_before<=clock_timestamp()
            AND d.state IN ('pending','retry','running') ORDER BY e.created_at LIMIT $1''',limit)
        for row in rows:
            identifier=await redis.xadd(STREAM,{'schema_version':'web-asset-acquisition-request.v1',
                'download_id':str(row['download_id']),'event_id':str(row['event_id'])})
            if isinstance(identifier,bytes):identifier=identifier.decode()
            await self.pool.execute('''UPDATE application.web_asset_acquisition_outbox
                SET published_at=clock_timestamp(),redis_id=$2 WHERE event_id=$1 AND published_at IS NULL''',row['event_id'],identifier)
        return len(rows)

    async def consume_once(self,redis,owner,submit_native,*,group='web-asset-acquisition',ttl=300,block_ms=1000):
        """Consume/reclaim notifications, with PG polling when notifications vanish.

        Nonterminal attempts remain pending until their PG outcome is terminal;
        due-time retry outbox messages can wake another worker independently.
        An idle pass blocks briefly, avoiding a hot poll of either service.
        """
        if not 1<=block_ms<=10000:raise ValueError('Invalid acquisition stream wait')
        if not getattr(self,'_group_ready',False):
            try:await redis.xgroup_create(STREAM,group,id='0-0',mkstream=True)
            except Exception as error:
                if 'BUSYGROUP' not in str(error):raise
            self._group_ready=True
        reclaimed=await redis.xautoclaim(STREAM,group,owner,min_idle_time=max(1000,int(ttl*1000)),
            start_id=getattr(self,'_claim_cursor','0-0'),count=1)
        self._claim_cursor=reclaimed[0]
        messages=reclaimed[1]
        if not messages:
            incoming=await redis.xreadgroup(group,owner,{STREAM:'>'},count=1,block=block_ms)
            messages=incoming[0][1] if incoming else []
        if not messages:
            # Recovery is independent of stream retention/delivery. Claim checks
            # due times and live PG leases before doing any network work.
            return await self.process_one(owner,submit_native,ttl=ttl)
        message_id,fields=messages[0]
        fields={k.decode() if isinstance(k,bytes) else k:v.decode() if isinstance(v,bytes) else v
                for k,v in fields.items()}
        try:
            if fields.get('schema_version')!='web-asset-acquisition-request.v1':
                raise ValueError('Unknown acquisition notification schema')
            download_id=UUID(fields['download_id'])
        except (KeyError,ValueError,TypeError):
            # Invalid payload has no task authority and can never be executed.
            await redis.xack(STREAM,group,message_id)
            return False
        processed=await self.process_one(owner,submit_native,download_id=download_id,ttl=ttl)
        state=await self.pool.fetchval('SELECT state FROM application.web_asset_downloads WHERE download_id=$1',download_id)
        if state in ('completed','failed','cancelled') or state is None:
            await redis.xack(STREAM,group,message_id)
        return processed
