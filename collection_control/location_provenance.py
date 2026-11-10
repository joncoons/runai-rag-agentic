"""Independent retained connector acquisition binding, including no-job observations."""
import json
from .provenance import verified_file,full_hash
from .artifacts import _unique_object
from .repository import Conflict

def verify_location_capture(raw_root,ref,canonical_uri,raw_sha,collection_id,run_id=None):
 try:
  manifest=json.loads(verified_file(raw_root,ref['artifact_uri'],full_hash(ref['sha256'])),object_pairs_hook=_unique_object)
  if manifest['schema_version']!='location-source-capture.v1' or manifest['coverage']!='complete' or manifest['collection_id']!=collection_id or (run_id is not None and manifest['source_run_id']!=str(run_id)):raise Conflict('Location acquisition scope differs')
  if not manifest['inventory']['complete'] or manifest['inventory'].get('missing_means_deleted') is not False:raise Conflict('Location acquisition is incomplete or authorizes unsupported deletion')
  matches=[x for x in manifest['files'] if x['canonical_uri']==canonical_uri]
  if len(matches)!=1 or full_hash(matches[0]['raw_sha256'])!=full_hash(raw_sha):raise Conflict('Location file inventory differs')
  item=matches[0];verified_file(raw_root,item['raw_uri'],full_hash(raw_sha))
  return manifest
 except (KeyError,TypeError,ValueError) as error:raise Conflict('Invalid retained location acquisition proof') from error
