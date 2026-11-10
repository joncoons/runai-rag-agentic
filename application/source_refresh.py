"""Fenced full-byte website observations; accepted jobs retain existing durability."""
import asyncio
from dataclasses import asdict,replace
from datetime import datetime,timezone
import hashlib,json,logging,os
from pathlib import Path
from uuid import UUID
from collection_control.artifacts import canonical
from collection_control.repository import Conflict,decoded
from collection_control.provenance import verified_file
from .web_acquisition import AcquisitionPolicy,collect,validate_url
from .web_asset_acquisition import acquire_assets
from .intake import submit_web
log=logging.getLogger(__name__)


def network_policy(spec):
 policy=dict(spec['policy']);policy.pop('private_hosts',None);policy.pop('private_ports',None)
 trusted={x.strip().lower() for x in os.environ.get('WEB_ASSET_PRIVATE_HOSTS','').split(',') if x.strip()}
 policy['private_hosts']=tuple(h for h in policy['allowed_hosts'] if h.lower() in trusted)
 policy['private_ports']=tuple(int(x) for x in os.environ.get('WEB_ASSET_PRIVATE_PORTS','').split(',') if x.strip())
 return AcquisitionPolicy(**policy)


def processing_identity(settings):
 # A stricter identity may cause safe extra processing after code changes, never
 # silently reuse outputs produced by a different adapter/model/vector contract.
 files={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in Path(__file__).parent.glob('*.py')}
 from .intake import embedding_config,PIPELINE
 from .image_extraction import image_contract
 identity_file=Path('/app/application-identity.json')
 image_identity=hashlib.sha256(identity_file.read_bytes()).hexdigest() if identity_file.is_file() else None
 return hashlib.sha256(canonical({'pipeline':PIPELINE,'image_extraction_contract':image_contract(),'packaged_application_identity':image_identity,'application_sources':files,'models':settings.models,'contract':settings.contract,'embedding':embedding_config(settings),'ocr_model_digest':os.environ.get('OCR_MODEL_DIGEST'),'acl':['local-operator']})).hexdigest()


def capture_identity(capture,assets,policy,processing):
 if not capture.complete or capture.errors or not capture.pages:raise ValueError('Incomplete source inventory')
 expected={(a.parent_url,a.url,a.kind,a.locator) for p in capture.pages for a in p.assets}
 acquired={(a.parent_url,a.requested_uri,a.kind,a.locator) for a in assets}
 if expected!=acquired:raise ValueError('Selected asset coverage differs')
 pages=[];children=[]
 for p in capture.pages:
  digest=hashlib.sha256(p.raw_bytes).hexdigest()
  if p.raw_sha256.removeprefix('sha256:')!=digest:raise ValueError('Page checksum differs')
  pages.append({'uri':p.final_uri,'requested':p.requested_uri,'sha256':digest,'assets':sorted([asdict(a) for a in p.assets],key=lambda a:canonical(a))})
 for a in assets:
  digest=hashlib.sha256(a.raw_bytes).hexdigest()
  if a.raw_sha256.removeprefix('sha256:')!=digest:raise ValueError('Asset checksum differs')
  children.append({'uri':a.requested_uri,'final':a.final_uri,'parent':a.parent_url,'kind':a.kind,'locator':a.locator,'alt':a.alt,'sha256':digest,'media_type':a.media_type})
 p=asdict(policy);p.pop('private_hosts',None);p.pop('private_ports',None)
 return hashlib.sha256(canonical({'pages':sorted(pages,key=lambda x:canonical(x)),'assets':sorted(children,key=lambda x:canonical(x)),'policy':p,'processing':processing})).hexdigest()


def retain_observations(root,capture,assets,baseline):
 from nv_ingest_durable.deferred_caption import retain
 prior={(x['kind'],x['canonical_uri']):x for x in (baseline or {}).get('observations',[]) if x['status']!='missing'}
 observed={};counts={k:0 for k in ('new','changed','unchanged','missing','failed')}
 for kind,uri,raw,evidence,suffix in [('page',p.final_uri,p.raw_bytes,p.evidence,'.html') for p in capture.pages]+[(a.kind,a.requested_uri,a.raw_bytes,a.evidence,Path(a.filename).suffix.lower()) for a in assets]:
  key=(kind,uri)
  if key in observed:
   if observed[key]['sha256']!=hashlib.sha256(raw).hexdigest():raise ValueError('Conflicting inventory observation')
   continue
  raw_uri,digest=retain(root,raw,suffix);old=prior.get(key)
  status='new' if old is None else 'unchanged' if old['sha256']==digest.removeprefix('sha256:') else 'changed'
  observed[key]={'kind':kind,'canonical_uri':uri,'sha256':digest.removeprefix('sha256:'),'raw_sha256':digest.removeprefix('sha256:'),'raw_uri':raw_uri,'status':status,'previous_sha256':old['sha256'] if old else None,'http':evidence};counts[status]+=1
 for key,old in prior.items():
  if key not in observed:observed[key]={'kind':key[0],'canonical_uri':key[1],'status':'missing','sha256':None,'raw_uri':None,'previous_sha256':old['sha256'],'deletion_authorized':False};counts['missing']+=1
 return sorted(observed.values(),key=lambda x:(x['kind'],x['canonical_uri'])),counts


class RefreshWorker:
 def __init__(self,context,*,registry=None):
  from .source_registry import SourceRegistry
  self.context=context;self.pool=context.pool;self.registry=registry or SourceRegistry(self.pool)
 async def baseline_current(self,run,*,connection=None):
  baseline=run.get('baseline')
  if not baseline or not baseline.get('job_id'):return False
  from collection_control.web_groups import resolve_graph
  if connection is None:
   async with self.pool.acquire() as c,c.transaction():return await self.baseline_current(run,connection=c)
  c=connection
  col=await c.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1',run['collection_id'])
  if not col or col['deleted'] or not col['current_operation']:return False
  graph=await resolve_graph(c,col)
  group=await c.fetchrow('SELECT * FROM collection_control.web_groups WHERE job_id=$1 AND collection_id=$2',UUID(str(baseline['job_id'])),run['collection_id'])
  if not group or group['state']!='published':return False
  retained=json.loads(verified_file(self.context.settings.raw,group['manifest_uri'],group['manifest_sha256']))
  by_uri={p['canonical_uri']:p for p in graph['pages']}
  for page in retained['pages']:
   current=by_uri.get(page['canonical_uri'])
   if not current or current['document_id']!=page['document_id']:return False
   target=lambda p:sorted((a['kind'],a['canonical_uri'],a['document_id'],a['source_revision_id']) for a in p['assets'])
   if target(current)!=target(page):return False
  return True
 async def finish_unchanged(self,run,refresh):
  from collection_control.pinned_connection import PinnedPool
  from .source_registry import SourceRegistry
  async with self.pool.acquire() as c,c.transaction():
   await c.fetchrow('SELECT collection_id FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',run['collection_id'])
   if not await self.baseline_current(run,connection=c):return False
   await SourceRegistry(PinnedPool(c)).finish(**refresh,status='unchanged')
   return True
 async def process(self,run):
  checkpoint=None;heartbeat=None;lease_fault={};parent=asyncio.current_task()
  queued_mode=run['spec'].get('kind','website')=='website' and 'page_prefixes' in run['spec'].get('policy',{})
  async def renew():
   try:
    while True:
     await asyncio.sleep(45)
     await self.registry.renew(run['run_id'],run['fence'],ttl=300)
   except asyncio.CancelledError:raise
   except Exception as error:
    lease_fault['error']=error;parent.cancel()
  try:
   if queued_mode:
    from .crawl_checkpoint import CrawlCheckpoint
    checkpoint=CrawlCheckpoint(self.context,self.registry,run)
    heartbeat=asyncio.create_task(renew())
   now=datetime.now(timezone.utc);seconds=(run['deadline']-now).total_seconds() if queued_mode else min(240.,(run['deadline']-now).total_seconds(),(run['lease_until']-now).total_seconds()-2)
   if seconds<=0:raise TimeoutError('Acquisition window ended')
   async with asyncio.timeout(seconds):
    if run['spec'].get('kind')=='location':
     from .location_sources import process_location
     await process_location(self,run);return
    policy=network_policy(run['spec'])
    for seed in run['spec']['seed_urls']:validate_url(seed,policy)
    if queued_mode:
     await self.stream_website(run,policy,checkpoint);return
    capture=await collect(run['spec']['seed_urls'],policy,capture_store=checkpoint) if checkpoint is not None else await collect(run['spec']['seed_urls'],policy)
    if not capture.complete or capture.errors or not capture.pages:raise ValueError('Incomplete source inventory')
    asset_policy=replace(policy,max_response_bytes=policy.max_asset_response_bytes)
    assets=await acquire_assets(capture,asset_policy,capture_store=checkpoint) if checkpoint is not None else await acquire_assets(capture,asset_policy)
    signature=processing_identity(self.context.settings)
    signature=hashlib.sha256(canonical({'processing':signature,'captions_enabled':bool(run['spec'].get('captions_enabled',False))})).hexdigest() if run['spec'].get('captions_enabled') else signature
    fingerprint=capture_identity(capture,assets,policy,signature)
    observations,counts=retain_observations(self.context.settings.raw,capture,assets,run.get('baseline'))
    from nv_ingest_durable.deferred_caption import retain
    manifest={'schema_version':'website-observation.v1','run_id':str(run['run_id']),'source_id':str(run.get('source_id')),'processing_identity':signature,'fingerprint':fingerprint,'observed_at':datetime.now(timezone.utc).isoformat(),'coverage':'complete','observations':observations,'counts':counts,'missing_page_deletion_authorized':False}
    if checkpoint is not None and checkpoint.reference:manifest['queued_crawl_checkpoint']=checkpoint.reference
    uri,sha=retain(self.context.settings.raw,canonical(manifest),'.website-observation.json')
    refresh={'run_id':run['run_id'],'fence':run['fence'],'fingerprint':fingerprint,'manifest_uri':uri,'manifest_sha256':sha.removeprefix('sha256:'),'observations':observations,'counts':counts}
    baseline=run.get('baseline')
    if baseline and baseline['fingerprint']==fingerprint and await self.finish_unchanged(run,refresh):return
    await submit_web(self.context,run['collection_id'],capture,policy,_acquired_assets=assets,_refresh=refresh,captions_enabled=bool(run['spec'].get('captions_enabled',False)))
  except asyncio.CancelledError:
   if not lease_fault:raise
   log.warning('website_acquisition_lease_fenced')
  except Exception as error:
   status='deferred' if isinstance(error,TimeoutError) and run.get('trigger')=='schedule' else 'failed'
   reference=checkpoint.reference if checkpoint is not None else None
   try:await self.registry.finish(run['run_id'],run['fence'],status=status,error_code=type(error).__name__,counts={'new':0,'changed':0,'unchanged':0,'missing':0,'failed':1},**({'manifest_uri':reference['uri'],'manifest_sha256':reference['sha256']} if reference else {}))
   except Conflict:pass
   log.warning('website_recheck_failed type=%s',type(error).__name__)
  finally:
   if heartbeat is not None:
    heartbeat.cancel()
    try:await heartbeat
    except asyncio.CancelledError:pass
 async def stream_website(self,run,policy,checkpoint):
  """Only HTML discovery is on the scan path; child work has its own lifecycle."""
  from .intake import submit_web_text
  from .web_acquisition import CrawlCapture
  from nv_ingest_durable.deferred_caption import retain
  jobs=[]
  async def accept_pages(pages):
   accepted=await submit_web_text(self.context,run['collection_id'],CrawlCapture(tuple(pages),(),True),policy,
    observation_id=run['run_id'],captions_enabled=bool(run['spec'].get('captions_enabled',False)),_source_run=run)
   jobs.extend(j for j in accepted if j not in jobs)
  capture=await collect(run['spec']['seed_urls'],policy,capture_store=checkpoint,page_sink=accept_pages)
  observations,counts=retain_observations(self.context.settings.raw,capture,[],run.get('baseline'))
  counts['assets_queued']=sum(len(p.assets) for p in capture.pages)
  signature=processing_identity(self.context.settings)
  fingerprint=hashlib.sha256(canonical({'schema_version':'website-html-inventory.v2','pages':[(p.final_uri,p.raw_sha256,[asdict(a) for a in p.assets]) for p in capture.pages],'policy':asdict(policy),'processing':signature})).hexdigest()
  manifest={'schema_version':'website-observation.v2','run_id':str(run['run_id']),'processing_identity':signature,
   'fingerprint':fingerprint,'coverage':'complete' if capture.complete else 'partial','observations':observations,
   'counts':counts,'page_jobs':jobs,'asset_processing':'deferred','asset_acquisition_complete':False,
   'missing_page_deletion_authorized':False,'errors':list(capture.errors)}
  if checkpoint is not None and checkpoint.reference:manifest['queued_crawl_checkpoint']=checkpoint.reference
  uri,sha=retain(self.context.settings.raw,canonical(manifest),'.website-observation.json')
  refresh={'run_id':run['run_id'],'fence':run['fence'],'fingerprint':fingerprint,'manifest_uri':uri,
   'manifest_sha256':sha.removeprefix('sha256:'),'observations':observations,'counts':counts}
  if not capture.complete or not jobs:
   await self.registry.finish(**refresh,status='failed',error_code='IncompleteHtmlInventory');return
  async with self.pool.acquire() as c,c.transaction():
   await c.fetchrow('SELECT collection_id FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',run['collection_id'])
   await self.registry.accept(c,**refresh,job_id=jobs[0])

 async def tick(self):
  await self.registry.reconcile();await self.registry.enqueue_due()
  run=await self.registry.claim(__import__('socket').gethostname(),ttl=300)
  if run:await self.process(run)
  return bool(run)
