"""Read-only input inventory using explicit connector/source-run associations."""
from urllib.parse import urlsplit
from .gui_health import GUI_SOURCE_CTES


def explicit_input(uri,source):
    if source.get('kind')=='location':return uri==(source.get('location') or {}).get('uri')
    return uri in (source.get('seed_urls') or [])


async def input_inventory(pool,collection_id,sources):
    rows=await pool.fetch(GUI_SOURCE_CTES+''' SELECT * FROM gui_sources WHERE collection_id=$1
      AND (web_role IS NULL OR web_role='page') ORDER BY observed_at DESC,source_revision_id DESC''',collection_id)
    selected={};published=set()
    for row in rows:
        if row['source_revision_id']==row['current_revision_id'] and row['status']=='published':published.add(row['source_id'])
        selected.setdefault(row['source_id'],row)
    associations={key:set() for key in selected}
    if sources and selected:
        links=await pool.fetch('''SELECT DISTINCT r.source_id,run.source_id AS connector_id
           FROM application.source_revisions r JOIN application.website_run_jobs mapping ON mapping.job_id=r.job_id
           JOIN application.website_source_runs run USING(run_id) WHERE r.source_id=ANY($1::text[])
           UNION SELECT DISTINCT r.source_id,run.source_id AS connector_id
           FROM application.source_revisions r JOIN application.website_source_runs run ON run.job_id=r.job_id
           WHERE r.source_id=ANY($1::text[])
           UNION SELECT DISTINCT r.source_id,run.source_id AS connector_id
           FROM application.source_revisions r JOIN public.jobs j ON j.job_id=r.job_id
           JOIN application.website_source_runs run ON run.run_id::text=j.metadata->>'source_run_id'
           WHERE r.source_id=ANY($1::text[])''',list(selected))
        for link in links:associations[link['source_id']].add(str(link['connector_id']))
    registered={str(s['source_id']) for s in sources}
    uploads=[];one_time=[]
    for row in selected.values():
        item={'document_id':row['document_id'],'filename':row['filename'],'uri':row['canonical_uri'],
              'source_kind':row['source_kind'],'status':row['status'],'published_available':row['source_id'] in published}
        if item['uri'].startswith('upload://'):uploads.append({**item,'capabilities':['replace']})
        elif not (associations[row['source_id']] & registered) and not any(explicit_input(item['uri'],s) for s in sources):one_time.append({**item,'capabilities':['register_source']})
    return {'uploads':sorted(uploads,key=lambda r:r['filename']),'one_time_sources':sorted(one_time,key=lambda r:r['uri'])}
