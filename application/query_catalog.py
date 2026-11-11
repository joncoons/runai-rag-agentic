"""Ordinary background collection metadata cache; no search-time SQL or locks."""
import asyncio,json,logging,os
from collection_control.repository import Conflict
LOG=logging.getLogger(__name__)

CATALOG_SQL='''SELECT a.collection_id,c.revision,c.contract_sha256,o.operation_id,o.artifact_sha256,o.handles,
 ARRAY(SELECT record_sha256 FROM collection_control.record_lineage WHERE operation_id=o.operation_id
       UNION SELECT record_sha256 FROM collection_control.caption_record_lineage WHERE operation_id=o.operation_id) AS versions
 FROM application.collections a JOIN collection_control.collections c USING(collection_id)
 JOIN collection_control.operations o ON o.operation_id=c.current_operation
 WHERE NOT c.deleted AND o.status='published' '''

class QueryCatalog:
    def __init__(self,pool,*,snapshots=None,refresh_seconds=None,allowed_collection_ids=None):
        self.pool=pool;self.snapshots=snapshots;self.allowed=allowed_collection_ids
        self.interval=float(refresh_seconds if refresh_seconds is not None else os.environ.get('QUERY_COLLECTION_REFRESH_SECONDS','5'))
        if not 1<=self.interval<=3600:raise ValueError('Invalid collection metadata refresh interval')
        self.entries={};self.ready=False;self.task=None

    async def refresh(self):
        rows=await self.pool.fetch(CATALOG_SQL+(' AND a.collection_id=ANY($1::text[])' if self.allowed is not None else ''),*([self.allowed] if self.allowed is not None else []),timeout=30);updated={}
        for row in rows:
            data=dict(row);previous=self.entries.get(data['collection_id'])
            data['handles']=json.loads(data['handles']) if isinstance(data['handles'],str) else dict(data['handles'])
            data['versions']=frozenset(data['versions'])
            if previous and previous['artifact_sha256']==data['artifact_sha256']:
                data['legal_versions']=previous['legal_versions']
            elif self.snapshots is not None:
                from .legal_evidence import snapshot_legal_hashes
                data['legal_versions']=await asyncio.to_thread(snapshot_legal_hashes,self.snapshots,data['artifact_sha256'])
            else:data['legal_versions']=frozenset()
            updated[data['collection_id']]=data
        self.entries=updated;self.ready=True

    def selected(self,collection_ids):
        if not self.ready:raise Conflict('Collection metadata is loading; please retry')
        current=self.entries
        if any(cid not in current for cid in collection_ids):raise Conflict('Selected collection is unavailable or outside this application')
        return [current[cid] for cid in collection_ids]

    async def start(self):
        await self.refresh()
        async def loop():
            while True:
                await asyncio.sleep(self.interval)
                try:await self.refresh()
                except asyncio.CancelledError:raise
                except Exception as error:LOG.warning('collection_metadata_refresh_failed error_type=%s',type(error).__name__)
        self.task=asyncio.create_task(loop());return self

    async def close(self):
        if self.task is not None:
            self.task.cancel();await asyncio.gather(self.task,return_exceptions=True);self.task=None
