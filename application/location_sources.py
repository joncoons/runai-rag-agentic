"""Registered-source acquisition handoff using the existing run and intake transaction."""
from dataclasses import asdict
from datetime import datetime,timezone
import hashlib,json,os
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID
from collection_control.artifacts import canonical
from collection_control.pinned_connection import PinnedPool
from .location_acquisition import capture_location

def root_config():
 return json.loads(Path(os.environ.get('LOCATION_ROOTS_FILE','/app/deployment/application/location-roots.json')).read_text())

async def baseline_current(connection,run):
 baseline=run.get('baseline')
 if not baseline or not baseline.get('job_id'):return False
 col=await connection.fetchrow('SELECT * FROM collection_control.collections WHERE collection_id=$1',run['collection_id'])
 if not col or col['deleted'] or not col['current_operation']:return False
 rows=await connection.fetch('SELECT document_id,source_revision_id FROM application.source_revisions WHERE job_id=$1 AND collection_id=$2',UUID(str(baseline['job_id'])),run['collection_id'])
 if not rows:return False
 selected=await connection.fetch("""SELECT a.document_id,a.source_revision_id FROM collection_control.record_lineage l JOIN collection_control.stage_admissions a USING(stage_key) WHERE l.operation_id=$1
 UNION SELECT a.document_id,a.source_revision_id FROM collection_control.caption_record_lineage l JOIN collection_control.caption_admissions a USING(stage_key) WHERE l.operation_id=$1""",col['current_operation'])
 if not {(r['document_id'],r['source_revision_id']) for r in rows}.issubset({(r['document_id'],r['source_revision_id']) for r in selected}):return False
 from collection_control.admission_repository import ProvenanceRepository
 op=await connection.fetchrow('SELECT * FROM collection_control.operations WHERE operation_id=$1',col['current_operation'])
 await ProvenanceRepository(PinnedPool(connection))._proof(connection,op)
 return True

async def process_location(worker,run):
 from .source_refresh import processing_identity,network_policy
 from .intake import submit
 from .source_registry import SourceRegistry
 from nv_ingest_durable.deferred_caption import retain
 spec=run['spec']['location'];uri=spec['uri'];scope=None
 if uri.startswith(('http://','https://')):
  allowed=spec.get('allowed_hosts') or [urlsplit(uri).hostname];scope=network_policy({'policy':{'allowed_hosts':allowed}})
 from .location_registry import active_roots
 roots=None
 if uri.startswith('location://'):
  from .location_acquisition import _location_parts
  alias,_=_location_parts(uri);available=await active_roots(worker.pool)
  roots={alias:available[alias]} if alias in available else {}
 capture=await capture_location(spec,roots,network_scope=scope)
 if not capture.complete:raise ValueError('Location inventory is incomplete')
 settings=worker.context.settings;caps=bool(run['spec'].get('captions_enabled',False));signature=processing_identity(settings)
 files=[];observations=[];counts={k:0 for k in ('new','changed','unchanged','missing','failed')};prior={(x['kind'],x['canonical_uri']):x for x in (run.get('baseline') or {}).get('observations',[]) if x['status']!='missing'}
 for item in capture.files:
  digest=hashlib.sha256(item.raw_bytes).hexdigest()
  if item.raw_sha256.removeprefix('sha256:')!=digest:raise ValueError('Location checksum mismatch')
  raw_uri,_=retain(settings.raw,item.raw_bytes,Path(item.filename).suffix.lower());old=prior.get(('file',item.canonical_uri));status='new' if old is None else 'unchanged' if old['sha256']==digest else 'changed';counts[status]+=1
  file={'canonical_uri':item.canonical_uri,'filename':item.filename,'raw_uri':raw_uri,'raw_sha256':digest,'media_type':item.media_type,'evidence':item.evidence};files.append(file);observations.append({'kind':'file','canonical_uri':item.canonical_uri,'status':status,'sha256':digest,'raw_sha256':digest,'raw_uri':raw_uri,'previous_sha256':old['sha256'] if old else None})
 found={f['canonical_uri'] for f in files}
 for (kind,old_uri),old in prior.items():
  if old_uri not in found:observations.append({'kind':'file','canonical_uri':old_uri,'status':'missing','sha256':None,'raw_uri':None,'previous_sha256':old['sha256'],'deletion_authorized':False});counts['missing']+=1
 fingerprint=hashlib.sha256(canonical({'files':[(f['canonical_uri'],f['raw_sha256'],f['media_type']) for f in sorted(files,key=lambda f:f['canonical_uri'])],'location':spec,'root_map_digest':capture.inventory_manifest.get('root_map_digest'),'processing':signature,'captions_enabled':caps})).hexdigest()
 manifest={'schema_version':'location-source-capture.v1','collection_id':run['collection_id'],'source_run_id':str(run['run_id']),'source_id':str(run['source_id']),'coverage':'complete','observed_at':datetime.now(timezone.utc).isoformat(),'inventory':capture.inventory_manifest,'files':files,'observations':observations,'counts':counts,'processing_identity':signature,'fingerprint':fingerprint}
 manifest_uri,manifest_sha=retain(settings.raw,canonical(manifest),'.location-capture.json');refresh={'run_id':run['run_id'],'fence':run['fence'],'manifest_uri':manifest_uri,'manifest_sha256':manifest_sha.removeprefix('sha256:'),'fingerprint':fingerprint,'observations':observations,'counts':counts}
 baseline=run.get('baseline')
 async with worker.pool.acquire() as c,c.transaction():
  await c.fetchrow('SELECT collection_id FROM collection_control.collections WHERE collection_id=$1 FOR UPDATE',run['collection_id'])
  if not files:
   await SourceRegistry(PinnedPool(c)).finish(**refresh,status='observed');return
  if baseline and baseline['fingerprint']==fingerprint and await baseline_current(c,run):
   await SourceRegistry(PinnedPool(c)).finish(**refresh,status='unchanged');return
 uploads=[];captures={}
 for item,file in zip(capture.files,files):
  name=hashlib.sha256(item.canonical_uri.encode()).hexdigest()+Path(item.filename).suffix.lower();uploads.append((name,item.raw_bytes));captures[name]={'canonical_uri':item.canonical_uri,'role':None,'acquisition':{'kind':'location','uri':manifest_uri,'sha256':manifest_sha.removeprefix('sha256:')}}
 await submit(worker.context,run['collection_id'],uploads,captions_enabled=caps,_web_captures=captures,_refresh=refresh)
