"""Current GUI work only. Retained history never becomes display authority."""
import asyncio,base64,json
from collections import Counter
from functools import lru_cache
from io import BytesIO
from pathlib import Path

GUI_SOURCE_CTES = '''WITH gui_live AS (
 SELECT r.source_revision_id,r.source_id,r.collection_id,r.document_id,r.job_id,r.canonical_uri,
        r.filename,r.status,r.records,r.operation_id,r.source_kind,r.web_role,r.web_group_id,r.observed_at,r.acquisition,
        src.current_revision_id
 FROM application.source_revisions r
 JOIN application.sources src USING(source_id)
 JOIN collection_control.collections col ON col.collection_id=r.collection_id AND NOT col.deleted
 JOIN public.documents d ON d.document_id=r.document_id
 JOIN public.jobs j ON j.job_id=r.job_id
 WHERE r.status NOT IN ('cancelled','canceled','deleted','superseded')
   AND d.status NOT IN ('cancelled','canceled','deleted','superseded')
   AND j.status NOT IN ('cancelled','canceled','deleted','superseded')
), gui_latest AS (
 SELECT DISTINCT ON (source_id) source_id,source_revision_id FROM gui_live
 WHERE web_role IS NULL OR web_role='page'
 ORDER BY source_id,observed_at DESC,source_revision_id DESC
), gui_base AS (
 SELECT r.* FROM gui_live r JOIN gui_latest latest USING(source_id)
 WHERE r.source_revision_id=r.current_revision_id OR
   (r.source_revision_id=latest.source_revision_id AND r.status IN ('queued','admitting','admitted','publishing','failed','captured'))
), gui_parents AS (
 SELECT * FROM gui_base WHERE web_role='page' AND status<>'failed'
), gui_edges AS (
 SELECT download.collection_id,download.child_source_revision_id,download.requested_uri,
        bool_or(ref.captions_enabled) AS needs_captions
 FROM application.web_asset_downloads download
 JOIN application.web_asset_references ref USING(download_id)
 JOIN gui_parents parent ON parent.source_revision_id=ref.parent_source_revision_id
 GROUP BY download.collection_id,download.child_source_revision_id,download.requested_uri
), gui_child_links AS (
 SELECT r.source_revision_id,e.needs_captions FROM gui_live r
 JOIN gui_edges e ON e.child_source_revision_id=r.source_revision_id AND e.collection_id=r.collection_id
 WHERE r.web_role IN ('image','binary')
 UNION ALL
 SELECT r.source_revision_id,e.needs_captions FROM gui_live r
 JOIN gui_edges e ON e.requested_uri=r.canonical_uri AND e.collection_id=r.collection_id
 WHERE r.web_role='binary' AND r.source_revision_id=r.current_revision_id
 UNION ALL
 SELECT r.source_revision_id,(j.metadata @> '{"ingestion_options":{"captions_enabled":true}}'::jsonb) AS needs_captions
 FROM gui_live r JOIN gui_parents parent ON parent.web_group_id=r.web_group_id
 JOIN public.jobs j ON j.job_id=parent.job_id
 WHERE r.web_role IN ('image','binary') AND r.web_group_id IS NOT NULL
), gui_children AS (
 SELECT source_revision_id,bool_or(needs_captions) AS needs_captions FROM gui_child_links GROUP BY source_revision_id
), gui_sources AS (
 SELECT * FROM gui_base
 UNION ALL
 SELECT r.* FROM gui_live r JOIN gui_children child USING(source_revision_id)
), gui_caption_sources AS (
 SELECT r.document_id FROM gui_sources r LEFT JOIN gui_children child USING(source_revision_id)
 WHERE r.web_role IS NULL OR r.web_role='page' OR child.needs_captions
)'''


CAPTION_ROWS = '''SELECT a.stage_key,a.parent_job_id,a.collection_id,a.document_id,
 r.filename,r.source_kind,r.web_role,p.operation_id,p.status AS publication_status,a.terminal_reason,
 a.manifest_uri,a.manifest_sha256,a.asset_sha256,
 CASE WHEN p.status='published' THEN 'completed'
      WHEN a.terminal_reason IS NOT NULL OR s.status='deadlettered' OR p.status='failed' THEN 'failed'
      WHEN s.status='committed' THEN 'publishing'
      WHEN s.status IN ('running','processing','leased') THEN 'processing'
      ELSE 'queued' END AS status
 FROM caption_control.assets a
 JOIN gui_sources r ON r.document_id=a.document_id AND r.collection_id=a.collection_id
 JOIN gui_caption_sources cs ON cs.document_id=a.document_id
 LEFT JOIN public.stage_units s ON s.idempotency_key=a.stage_key
 LEFT JOIN application.caption_publications p ON p.caption_stage_key=a.stage_key
 WHERE COALESCE(p.last_error_type,'') NOT IN ('StaleWebImage','SupersededSource','CollectionDeleted')
   AND COALESCE(a.terminal_reason,'') NOT IN ('StaleWebImage','SupersededSource','CollectionDeleted','below_caption_library_minimum')'''

CAPTION_STATES = GUI_SOURCE_CTES + '\n' + CAPTION_ROWS
SOURCE_ROWS = GUI_SOURCE_CTES + '\nSELECT collection_id,filename,status,records,document_id,operation_id,source_kind,web_role,canonical_uri,acquisition FROM gui_sources'

@lru_cache(maxsize=4096)
def _below_caption_minimum(root,manifest_uri,manifest_sha,asset_sha):
    from collection_control.provenance import verified_file,full_hash
    from PIL import Image
    from nemo_retriever.operators.extract.caption.caption import _image_meets_min_size
    manifest=json.loads(verified_file(Path(root),manifest_uri,full_hash(manifest_sha)))
    if full_hash(manifest['asset_sha256'])!=full_hash(asset_sha):raise ValueError('Caption input identity differs')
    raw=verified_file(Path(root),manifest['asset_uri'],full_hash(asset_sha))
    # Native predicate returns false for corrupt data too. Corruption is a true
    # error, so validate pixels before treating false as an intentional skip.
    with Image.open(BytesIO(raw)) as image:image.verify()
    # JPEG verify only checks headers. A truncated stream can still expose
    # small dimensions; decode fully before granting an intentional skip.
    with Image.open(BytesIO(raw)) as image:image.load()
    return not _image_meets_min_size(base64.b64encode(raw).decode('ascii'))

async def visible_captions(settings,rows):
    visible=[]
    for row in rows:
        if row['status']=='failed' and all(row.get(k) for k in ('manifest_uri','manifest_sha256','asset_sha256')):
            try:
                skipped=await asyncio.to_thread(_below_caption_minimum,str(settings.stages),row['manifest_uri'],row['manifest_sha256'],row['asset_sha256'])
            except Exception:
                # Missing/corrupt evidence is not grounds to hide a real error.
                skipped=False
            if skipped:continue
        visible.append(row)
    return visible


def visible_job_summary(row,documents,has_hidden_sources):
    """Project source health; never rewrite the authoritative historical job."""
    if not has_hidden_sources:return row['status'],row['stage'],None
    if any(d['status']=='failed' for d in documents):
        return 'failed',row['stage'],['Processing failed for a current source.']
    if any(d['status'] not in ('published','captured') for d in documents):
        stage='publication' if any(d['status']=='publishing' for d in documents) else 'extraction'
        return 'processing',stage,[]
    return 'completed','complete',[]


def image_origin_summary(rows,visible_failed_keys=None):
    """Current caption candidates: pixels and associations have different units."""
    origins={k:{'hashes':set(),'associations':0,'pending':0,'completed':0,'failed':0} for k in ('webpage','pdf','other')}
    for row in rows:
        if row['status']=='failed' and visible_failed_keys is not None and row['stage_key'] not in visible_failed_keys:continue
        origin=row.get('origin','other');item=origins.get(origin,origins['other'])
        if row.get('asset_sha256'):item['hashes'].add(row['asset_sha256'])
        item['associations']+=1
        state=row['status'] if row['status'] in ('completed','failed') else 'pending'
        item[state]+=1
    return {k:{'unique_images':len(v['hashes']),**{f:n for f,n in v.items() if f!='hashes'}} for k,v in origins.items()}
