"""PostgreSQL authority for saved websites and daily acquisition windows (ADR0047).

All mutations acquire collection, source, then run locks. Network acquisition never
holds these locks. Only accept(), within the caller's intake transaction, links a
run to a job; a published job alone advances the comparison baseline.
"""
from datetime import datetime, time, timedelta, timezone
import json
import re
from urllib.parse import parse_qsl, urlsplit
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from collection_control.repository import Conflict, Fenced

UTC = timezone.utc
ACTIVE = ('queued', 'scanning', 'accepted', 'deferred')
DEFAULT_SCHEDULE = {'enabled': False, 'timezone': 'America/Chicago', 'start': '02:00', 'end': '06:00'}


def validate_schedule(value):
    if value is None: value = {}
    if not isinstance(value, dict) or set(value)-set(DEFAULT_SCHEDULE):
        raise ValueError('Invalid daily schedule fields')
    result = DEFAULT_SCHEDULE | value
    if not isinstance(result['enabled'], bool): raise ValueError('Schedule enabled must be boolean')
    try: ZoneInfo(result['timezone'])
    except (ZoneInfoNotFoundError, ValueError, TypeError) as error: raise ValueError('An IANA timezone is required') from error
    for key in ('start', 'end'):
        if not isinstance(result[key], str) or not re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d', result[key]):
            raise ValueError('Window times must use HH:MM')
    if result['start'] >= result['end']: raise ValueError('Daily start must precede end on the same day')
    return result


def _aware(now):
    if now.tzinfo is None: raise ValueError('Clock must include a timezone')
    return now.astimezone(UTC)


def _day_window(schedule, day):
    zone = ZoneInfo(schedule['timezone'])
    local = datetime.combine(day, time.fromisoformat(schedule['start']))
    start = local.replace(tzinfo=zone, fold=0).astimezone(UTC)
    # A round trip detects a nonexistent local start. Repeated starts use fold=0.
    if start.astimezone(zone).replace(tzinfo=None) != local: return None
    end = datetime.combine(day, time.fromisoformat(schedule['end'])).replace(tzinfo=zone, fold=0).astimezone(UTC)
    return start, end


def schedule_window(schedule, now):
    """Return today's active UTC interval, or None outside it / on a DST gap."""
    now = _aware(now)
    if not schedule['enabled']: return None
    window = _day_window(schedule, now.astimezone(ZoneInfo(schedule['timezone'])).date())
    return window if window and window[0] <= now < window[1] else None


def next_window(schedule, now):
    """Next window whose nominal start is strictly after now; bounded by days."""
    now = _aware(now)
    if not schedule['enabled']: return None
    day = now.astimezone(ZoneInfo(schedule['timezone'])).date()
    for offset in range(370):
        window = _day_window(schedule, day + timedelta(days=offset))
        if window and window[0] > now: return window
    raise ValueError('No valid daily window within one year')


def _dict(row):
    if row is None: return None
    result = dict(row)
    for key in ('seed_urls', 'policy', 'schedule', 'spec', 'observations', 'counts', 'location'):
        if isinstance(result.get(key), str): result[key] = json.loads(result[key])
    if result.get('deadline')==datetime.max:result['deadline']=datetime.max.replace(tzinfo=UTC)
    # Lease ownership is internal; callers receive fence/expiry but no credentials.
    result.setdefault('kind','website');result.setdefault('location',None);result.setdefault('captions_enabled',False)
    result.pop('owner', None)
    return result


def _id(value): return value if isinstance(value, UUID) else UUID(str(value))


def _spec(source): return {key: source.get(key) for key in ('seed_urls','policy','schedule','kind','location','captions_enabled')}


def _safe_urls(seed_urls):
    if not isinstance(seed_urls, (list, tuple)) or not 1 <= len(seed_urls) <= 100:
        raise ValueError('Provide one to one hundred seed URLs')
    result = []
    for url in seed_urls:
        if not isinstance(url, str) or len(url) > 8192: raise ValueError('Invalid seed URL')
        parsed = urlsplit(url)
        if parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username is not None or parsed.password is not None or parsed.fragment:
            raise ValueError('Seed URLs must be HTTP(S) URLs without credentials or fragments')
        if any(re.search(r'token|secret|password|credential|api.?key|signature', key, re.I) for key, _ in parse_qsl(parsed.query)):
            raise ValueError('Credential query parameters cannot be saved')
        if url not in result: result.append(url)
    return result


class SourceRegistry:
    def __init__(self, pool): self.pool = pool

    async def _collection(self, c, collection_id, *, skip=False):
        col = await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE' + (' SKIP LOCKED' if skip else ''), collection_id)
        if not col or col['deleted']:
            if skip: return None
            raise Conflict('Collection is unavailable')
        return col

    async def _source(self, c, source_id, collection_id):
        row = await c.fetchrow('SELECT * FROM application.website_sources WHERE source_id=$1 AND collection_id=$2 FOR UPDATE', _id(source_id), collection_id)
        if not row: raise Conflict('Website source is unavailable')
        return _dict(row)

    async def create(self, collection_id, *, name, seed_urls, policy, schedule, captions_enabled=False):
        if not isinstance(captions_enabled,bool):raise ValueError('Caption selection must be boolean')
        schedule = validate_schedule(schedule)
        urls = _safe_urls(seed_urls)
        if not isinstance(name, str) or not name.strip() or len(name) > 255: raise ValueError('A source name is required')
        if not isinstance(policy, dict): raise ValueError('Acquisition policy must be an object')
        from .web_acquisition import AcquisitionPolicy
        from dataclasses import asdict
        if any(policy.get(key) for key in ('private_hosts', 'private_ports')):
            raise ValueError('Private acquisition scope is server-controlled')
        try: policy = asdict(AcquisitionPolicy(**policy))
        except TypeError as error: raise ValueError('Invalid acquisition policy fields') from error
        policy.pop('private_hosts', None)
        policy.pop('private_ports', None)
        now = datetime.now(UTC)
        window = schedule_window(schedule, now) or next_window(schedule, now)
        async with self.pool.acquire() as c, c.transaction():
            await self._collection(c, collection_id)
            row=await c.fetchrow('''INSERT INTO application.website_sources(source_id,collection_id,name,seed_urls,policy,schedule,next_fire)
                VALUES($1,$2,$3,$4::jsonb,$5::jsonb,$6::jsonb,$7) RETURNING *''',
                uuid4(), collection_id, name.strip(), json.dumps(urls), json.dumps(policy), json.dumps(schedule), window[0] if window else None)
            if captions_enabled:row=await c.fetchrow('UPDATE application.website_sources SET captions_enabled=true WHERE source_id=$1 RETURNING *',row['source_id'])
            return _dict(row)

    async def create_location(self,collection_id,*,name,location,schedule,captions_enabled=False):
        from .location_acquisition import _parse_spec,_location_parts,LocationLimits
        _parse_spec(location,LocationLimits())
        uri=location.get('uri')
        if uri.startswith('location://'):_location_parts(uri)
        elif uri.startswith(('https://','http://')):_safe_urls([uri])
        else:raise ValueError('Unsupported location protocol')
        if not isinstance(name,str) or not name.strip() or len(name)>255 or not isinstance(captions_enabled,bool):raise ValueError('Invalid location selection')
        schedule=validate_schedule(schedule);now=datetime.now(UTC);window=schedule_window(schedule,now) or next_window(schedule,now)
        async with self.pool.acquire() as c,c.transaction():
            await self._collection(c,collection_id)
            row=await c.fetchrow('''INSERT INTO application.website_sources(source_id,collection_id,name,seed_urls,policy,schedule,next_fire,kind,location,captions_enabled) VALUES($1,$2,$3,'[]'::jsonb,'{}'::jsonb,$4::jsonb,$5,'location',$6::jsonb,$7) RETURNING *''',uuid4(),collection_id,name.strip(),json.dumps(schedule),window[0] if window else None,json.dumps(location),captions_enabled)
            return _dict(row)

    async def list(self, collection_id):
        return [_dict(r) for r in await self.pool.fetch('''SELECT s.* FROM application.website_sources s
            JOIN collection_control.collections c USING(collection_id) WHERE s.collection_id=$1 AND NOT c.deleted ORDER BY s.created_at,s.source_id''', collection_id)]

    async def update(self, source_id, collection_id, *, schedule=None, paused=None, captions_enabled=None):
        if captions_enabled is not None and not isinstance(captions_enabled,bool):raise ValueError('Caption selection must be boolean')
        if paused is not None and not isinstance(paused, bool): raise ValueError('Paused must be boolean')
        if schedule is not None: schedule = validate_schedule(schedule)
        async with self.pool.acquire() as c, c.transaction():
            await self._collection(c, collection_id)
            source = await self._source(c, source_id, collection_id)
            if captions_enabled is True and source.get('kind','website')=='website' and not source['policy'].get('include_images',True):raise ValueError('Captioning requires saved image acquisition scope')
            if schedule is None and paused is None and captions_enabled is None: return source
            schedule = schedule if schedule is not None else source['schedule']
            paused = paused if paused is not None else source['paused']
            now = datetime.now(UTC)
            window = (schedule_window(schedule, now) or next_window(schedule, now)) if not paused else None
            result = await c.fetchrow('''UPDATE application.website_sources SET schedule=$2::jsonb,paused=$3,version=version+1,
                next_fire=$4,last_fire=NULL,updated_at=clock_timestamp() WHERE source_id=$1 RETURNING *''',
                source['source_id'], json.dumps(schedule), paused, window[0] if window else None)
            await c.execute('''UPDATE application.website_source_runs SET state='cancelled',error_code='source_changed',
                lease_until=NULL,finished_at=clock_timestamp(),updated_at=clock_timestamp()
                WHERE source_id=$1 AND state IN ('queued','scanning','deferred')''', source['source_id'])
            if captions_enabled is not None:result=await c.fetchrow('UPDATE application.website_sources SET captions_enabled=$2 WHERE source_id=$1 RETURNING *',source['source_id'],captions_enabled)
            return _dict(result)

    async def update_details(self,source_id,collection_id,*,expected_version,changes):
        allowed={'name','seed_urls','policy','location','schedule','paused','captions_enabled'}
        if not changes or set(changes)-allowed:raise ValueError('Invalid source edit')
        async with self.pool.acquire() as c,c.transaction():
            await self._collection(c,collection_id)
            source=await self._source(c,source_id,collection_id)
            if source['version']!=expected_version:raise Conflict('Source changed; reload its current settings')
            desired={**source,**changes}
            name=desired['name'].strip()
            if not name or len(name)>120 or any(ord(x)<32 for x in name):raise ValueError('Invalid source name')
            desired['name']=name;desired['schedule']=validate_schedule(desired['schedule'])
            if any(not isinstance(desired.get(k),bool) for k in ('paused','captions_enabled')):raise ValueError('Invalid source controls')
            before={**_spec(source),'name':source['name'],'paused':source['paused']}
            after={**_spec(desired),'name':name,'paused':desired['paused']}
            if before==after:return source
            now=datetime.now(UTC);window=(schedule_window(desired['schedule'],now) or next_window(desired['schedule'],now)) if not desired['paused'] else None
            result=await c.fetchrow('''UPDATE application.website_sources SET name=$2,seed_urls=$3::jsonb,policy=$4::jsonb,location=$5::jsonb,
                schedule=$6::jsonb,paused=$7,captions_enabled=$8,version=version+1,next_fire=$9,last_fire=NULL,updated_at=clock_timestamp()
                WHERE source_id=$1 RETURNING *''',source['source_id'],name,json.dumps(desired['seed_urls']),json.dumps(desired['policy']),
                json.dumps(desired.get('location')),json.dumps(desired['schedule']),desired['paused'],desired['captions_enabled'],window[0] if window else None)
            await c.execute('''INSERT INTO application.source_configuration_history(source_id,version,previous_spec,new_spec,actor)
                VALUES($1,$2,$3::jsonb,$4::jsonb,'local-phase1')''',source['source_id'],result['version'],json.dumps(before),json.dumps(after))
            await c.execute('''UPDATE application.website_source_runs SET state='cancelled',error_code='source_changed',lease_until=NULL,
                finished_at=clock_timestamp(),updated_at=clock_timestamp() WHERE source_id=$1 AND state IN ('queued','scanning','deferred')''',source['source_id'])
            return _dict(result)

    async def _active(self, c, source_id):
        return _dict(await c.fetchrow("SELECT * FROM application.website_source_runs WHERE source_id=$1 AND state IN ('queued','scanning','accepted','deferred') FOR UPDATE", source_id))

    async def _insert(self, c, source, trigger, fire, deadline, now):
        return _dict(await c.fetchrow('''INSERT INTO application.website_source_runs
            (run_id,source_id,collection_id,source_version,spec,nominal_fire,trigger,deadline,not_before)
            VALUES($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9) RETURNING *''', uuid4(), source['source_id'],
            source['collection_id'], source['version'], json.dumps(_spec(source)), fire, trigger, deadline, now))

    async def request(self, source_id, collection_id):
        async with self.pool.acquire() as c, c.transaction():
            await self._collection(c, collection_id)
            source = await self._source(c, source_id, collection_id)
            active = await self._active(c, source['source_id'])
            if active: return active
            now = await c.fetchval('SELECT clock_timestamp()')
            return await self._insert(c, source, 'manual', now, datetime.max.replace(tzinfo=UTC), now)

    async def enqueue_due(self, now=None):
        now = _aware(now or datetime.now(UTC)); count = 0
        candidates = await self.pool.fetch('''SELECT s.source_id,s.collection_id FROM application.website_sources s
            JOIN collection_control.collections c USING(collection_id) WHERE NOT c.deleted AND NOT s.paused ORDER BY s.collection_id,s.source_id''')
        for candidate in candidates:
            async with self.pool.acquire() as c, c.transaction():
                if not await self._collection(c, candidate['collection_id'], skip=True): continue
                source = await self._source(c, candidate['source_id'], candidate['collection_id'])
                if source['paused']: continue
                schedule = source['schedule']; window = schedule_window(schedule, now)
                following = next_window(schedule, now)
                await c.execute('UPDATE application.website_sources SET next_fire=$2 WHERE source_id=$1', source['source_id'], following[0] if following else None)
                if not window: continue
                active = await self._active(c, source['source_id'])
                expired = (active and active['state'] in ('queued', 'scanning') and active['deadline'] <= now
                           and (active['state'] == 'queued' or not active['lease_until'] or active['lease_until'] <= now))
                if expired and active['trigger'] == 'schedule' and active['deadline'] <= window[0] and active['attempts'] < 3:
                    # A coordinator may have been offline at the prior boundary.
                    # Restart from seeds in this window rather than defer another day.
                    await c.execute("UPDATE application.website_source_runs SET state='queued',deadline=$2,not_before=$3,lease_until=NULL,updated_at=clock_timestamp() WHERE run_id=$1",
                                    active['run_id'], window[1], now)
                    await c.execute('UPDATE application.website_sources SET last_fire=$2 WHERE source_id=$1', source['source_id'], window[0])
                    continue
                if expired:
                    await c.execute("UPDATE application.website_source_runs SET state='failed',lease_until=NULL,error_code='deadline_expired',finished_at=clock_timestamp(),updated_at=clock_timestamp() WHERE run_id=$1", active['run_id'])
                    active = None
                if active and active['state'] == 'deferred' and active['not_before'] <= now:
                    await c.execute("UPDATE application.website_source_runs SET state='queued',deadline=$2,not_before=$3,updated_at=clock_timestamp() WHERE run_id=$1", active['run_id'], window[1], now)
                    await c.execute('UPDATE application.website_sources SET last_fire=$2 WHERE source_id=$1', source['source_id'], window[0])
                    continue
                if source['last_fire'] and source['last_fire'] >= window[0]: continue
                await c.execute('UPDATE application.website_sources SET last_fire=$2 WHERE source_id=$1', source['source_id'], window[0])
                if active: continue
                # Versioned uniqueness survives retries, competing coordinators and clock replay.
                exists = await c.fetchval("SELECT EXISTS(SELECT 1 FROM application.website_source_runs WHERE source_id=$1 AND source_version=$2 AND trigger='schedule' AND nominal_fire=$3)", source['source_id'], source['version'], window[0])
                if not exists:
                    await self._insert(c, source, 'schedule', window[0], window[1], now); count += 1
        return count

    async def _defer(self, c, source, run, now, error_code):
        following = next_window(source['schedule'], now)
        if run['trigger'] == 'schedule' and following and run['attempts'] < 3:
            await c.execute('''UPDATE application.website_source_runs SET state='deferred',lease_until=NULL,
                not_before=$2,deadline=$3,error_code=$4,updated_at=clock_timestamp() WHERE run_id=$1''',
                run['run_id'], following[0], following[1], error_code)
        else:
            await c.execute("UPDATE application.website_source_runs SET state='failed',lease_until=NULL,error_code=$2,finished_at=clock_timestamp(),updated_at=clock_timestamp() WHERE run_id=$1", run['run_id'], error_code)

    async def claim(self, owner, ttl=300):
        if not isinstance(owner, str) or not owner or len(owner) > 255: raise ValueError('Invalid lease owner')
        if isinstance(ttl, bool) or not isinstance(ttl, (int, float)) or not 0 < ttl <= 3600: raise ValueError('Invalid lease lifetime')
        candidates = await self.pool.fetch("""SELECT DISTINCT s.source_id,s.collection_id FROM application.website_sources s
            JOIN application.website_source_runs r USING(source_id) WHERE r.state IN ('queued','scanning','deferred') ORDER BY s.collection_id,s.source_id""")
        for candidate in candidates:
            async with self.pool.acquire() as c, c.transaction():
                # Skip a busy collection rather than take locks in the inverse order.
                col = await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE SKIP LOCKED', candidate['collection_id'])
                if not col: continue
                source = await self._source(c, candidate['source_id'], candidate['collection_id'])
                run = await self._active(c, source['source_id'])
                if not run or run['state'] == 'accepted': continue
                now = await c.fetchval('SELECT clock_timestamp()')
                if col['deleted'] or (source['paused'] and run['trigger'] == 'schedule') or run['source_version'] != source['version']:
                    await c.execute("UPDATE application.website_source_runs SET state='cancelled',lease_until=NULL,error_code='source_unavailable',finished_at=clock_timestamp() WHERE run_id=$1", run['run_id']); continue
                if run['state'] == 'deferred' or run['not_before'] > now: continue
                if run['state'] == 'scanning' and run['lease_until'] and run['lease_until'] > now: continue
                if run['attempts'] >= 3:
                    await c.execute("UPDATE application.website_source_runs SET state='failed',lease_until=NULL,error_code='attempts_exhausted',finished_at=clock_timestamp() WHERE run_id=$1", run['run_id']); continue
                if run['deadline'] <= now:
                    await self._defer(c, source, run, now, 'deadline_expired'); continue
                claimed = _dict(await c.fetchrow("""UPDATE application.website_source_runs SET state='scanning',fence=fence+1,
                    attempts=attempts+1,owner=$2,lease_until=$3,updated_at=clock_timestamp() WHERE run_id=$1 RETURNING *""",
                    run['run_id'], owner, now + timedelta(seconds=ttl)))
                claimed['baseline'] = _dict(await c.fetchrow("SELECT * FROM application.website_source_runs WHERE run_id=$1 AND state='published'", source['last_successful_run_id'])) if source['last_successful_run_id'] else None
                return claimed
        return None

    async def _leased(self, c, run_id, fence, *, require_deadline):
        identity = await c.fetchrow('SELECT collection_id,source_id FROM application.website_source_runs WHERE run_id=$1', _id(run_id))
        if not identity: raise Fenced('Unknown acquisition run')
        col = await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE', identity['collection_id'])
        source = await self._source(c, identity['source_id'], identity['collection_id'])
        run = _dict(await c.fetchrow('SELECT * FROM application.website_source_runs WHERE run_id=$1 FOR UPDATE', _id(run_id)))
        now = await c.fetchval('SELECT clock_timestamp()')
        if (not col or col['deleted'] or (source['paused'] and run['trigger'] == 'schedule') or source['version'] != run['source_version']
            or run['state'] != 'scanning' or run['fence'] != fence or not run['lease_until'] or run['lease_until'] <= now
            or (require_deadline and run['deadline'] <= now)):
            raise Fenced('Acquisition lease or source version is stale')
        return source, run, now

    async def renew(self,run_id,fence,ttl=300):
        if isinstance(ttl,bool) or not isinstance(ttl,(int,float)) or not 0<ttl<=3600:raise ValueError('Invalid lease lifetime')
        async with self.pool.acquire() as c,c.transaction():
            _,run,now=await self._leased(c,run_id,fence,require_deadline=True)
            return await c.fetchval('UPDATE application.website_source_runs SET lease_until=$2,updated_at=clock_timestamp() WHERE run_id=$1 RETURNING lease_until',run['run_id'],now+timedelta(seconds=ttl))

    async def finish(self, run_id, fence, *, status, fingerprint=None, manifest_uri=None, manifest_sha256=None,
                     observations=None, counts=None, error_code=None):
        if status not in ('unchanged', 'observed', 'failed', 'deferred'): raise ValueError('Invalid acquisition outcome')
        async with self.pool.acquire() as c, c.transaction():
            source, run, now = await self._leased(c, run_id, fence, require_deadline=status in ('unchanged','observed'))
            await c.execute('''UPDATE application.website_source_runs SET fingerprint=$2,manifest_uri=$3,manifest_sha256=$4,
                observations=$5::jsonb,counts=$6::jsonb,error_code=$7,updated_at=clock_timestamp() WHERE run_id=$1''',
                run['run_id'], fingerprint, manifest_uri, manifest_sha256, json.dumps(observations or []), json.dumps(counts or {}), error_code)
            if status == 'deferred': await self._defer(c, source, run, now, error_code)
            else:
                await c.execute('''UPDATE application.website_source_runs SET state=$2,lease_until=NULL,
                    finished_at=clock_timestamp() WHERE run_id=$1''', run['run_id'], status)

    async def accept(self, connection, run_id, fence, *, job_id, fingerprint, manifest_uri, manifest_sha256, observations, counts):
        if not connection.is_in_transaction(): raise Conflict('Run acceptance requires the intake transaction')
        _, run, _ = await self._leased(connection, run_id, fence, require_deadline=True)
        job_id = _id(job_id)
        if not await connection.fetchval('SELECT EXISTS(SELECT 1 FROM application.jobs WHERE job_id=$1 AND collection_id=$2)', job_id, run['collection_id']):
            raise Conflict('Accepted acquisition job must belong to the source collection')
        await connection.execute('''UPDATE application.website_source_runs SET state='accepted',job_id=$2,fingerprint=$3,
            manifest_uri=$4,manifest_sha256=$5,observations=$6::jsonb,counts=$7::jsonb,lease_until=NULL,
            updated_at=clock_timestamp() WHERE run_id=$1''', run['run_id'], job_id, fingerprint, manifest_uri,
            manifest_sha256, json.dumps(observations), json.dumps(counts))

    async def reconcile(self):
        candidates = await self.pool.fetch("""SELECT r.run_id,r.source_id,r.collection_id FROM application.website_source_runs r
            JOIN application.jobs j USING(job_id) WHERE r.state='accepted' AND j.status IN ('completed','failed','cancelled','canceled')
            ORDER BY r.collection_id,r.source_id""")
        for candidate in candidates:
            async with self.pool.acquire() as c, c.transaction():
                col = await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE', candidate['collection_id'])
                source = await self._source(c, candidate['source_id'], candidate['collection_id'])
                run = await c.fetchrow('SELECT r.*,j.status AS job_status FROM application.website_source_runs r JOIN application.jobs j USING(job_id) WHERE run_id=$1 FOR UPDATE OF r', candidate['run_id'])
                if not run or run['state'] != 'accepted': continue
                streamed=await c.fetch('SELECT j.status FROM application.website_run_jobs r JOIN application.jobs j USING(job_id) WHERE r.run_id=$1',run['run_id'])
                if streamed:
                    if any(j['status'] not in ('completed','failed','cancelled','canceled') for j in streamed):continue
                    combined='failed' if any(j['status']=='failed' for j in streamed) else 'cancelled' if any(j['status'] in ('cancelled','canceled') for j in streamed) else 'completed'
                    run={**dict(run),'job_status':combined}
                status = 'published' if run['job_status'] == 'completed' and col and not col['deleted'] else ('failed' if run['job_status'] == 'failed' else 'cancelled')
                await c.execute("UPDATE application.website_source_runs SET state=$2,finished_at=clock_timestamp(),updated_at=clock_timestamp() WHERE run_id=$1", run['run_id'], status)
                if status == 'published':
                    await c.execute('UPDATE application.website_sources SET last_successful_run_id=$2 WHERE source_id=$1', source['source_id'], run['run_id'])

    async def history(self, source_id, collection_id):
        exists = await self.pool.fetchval('''SELECT EXISTS(SELECT 1 FROM application.website_sources s JOIN collection_control.collections c USING(collection_id)
            WHERE s.source_id=$1 AND s.collection_id=$2 AND NOT c.deleted)''', _id(source_id), collection_id)
        if not exists: raise Conflict('Website source is unavailable')
        return [_dict(r) for r in await self.pool.fetch('SELECT * FROM application.website_source_runs WHERE source_id=$1 AND collection_id=$2 ORDER BY created_at DESC,run_id DESC', _id(source_id), collection_id)]
