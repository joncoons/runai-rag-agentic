"""Collection-scoped, fenced reuse of source-neutral image captions and vectors."""
import asyncio,gzip,hashlib,time
from datetime import timedelta
from pathlib import Path
from uuid import uuid4
from collection_control.artifacts import canonical
from collection_control.provenance import verified_file
from .pdf_extraction_cache import encode_native,decode_native

class ImageCaptionCache:
    def __init__(self,pool,root,*,lease_seconds=90,poll_seconds=.2,wait_seconds=900):
        self.pool=pool;self.root=Path(root);self.lease=timedelta(seconds=lease_seconds)
        self.poll=poll_seconds;self.wait=wait_seconds

    async def _heartbeat(self,key,owner,fence,lost):
        try:
            while True:
                await asyncio.sleep(max(.02,self.lease.total_seconds()/3))
                result=await self.pool.fetchval("""UPDATE application.image_caption_cache
                    SET lease_until=clock_timestamp()+$4::interval,updated_at=clock_timestamp()
                    WHERE cache_key=$1 AND owner=$2 AND fence=$3 AND status='running'
                     AND lease_until>clock_timestamp() RETURNING true""",key,owner,fence,self.lease)
                if not result:lost.set();return
        except asyncio.CancelledError:raise
        except Exception:lost.set()

    async def _load(self,row):
        raw=await asyncio.to_thread(verified_file,self.root,row['artifact_uri'],row['artifact_sha256'])
        frame=await asyncio.to_thread(decode_native,gzip.decompress(raw))
        return frame

    async def _use(self,document,key,row,hit):
        await self.pool.execute('''INSERT INTO application.image_caption_uses(document_id,cache_key,artifact_sha256,cache_hit)
            VALUES($1,$2,$3,$4) ON CONFLICT(document_id,cache_key) DO NOTHING''',document,key,row['artifact_sha256'],hit)
        return {'cache_key':key,'cache_hit':hit,'artifact_uri':row['artifact_uri'],'artifact_sha256':row['artifact_sha256']}

    async def get_or_compute(self,collection,raw_sha,contract_sha,compute,*,document_id):
        if not collection or any(len(h)!=64 or any(c not in '0123456789abcdef' for c in h) for h in (raw_sha,contract_sha)):
            raise ValueError('Cache requires exact scope, content and contract hashes')
        key=hashlib.sha256(canonical({'version':1,'collection':collection,'raw':raw_sha,'contract':contract_sha})).hexdigest()
        deadline=time.monotonic()+self.wait;owner=uuid4()
        while True:
            row=await self.pool.fetchrow('SELECT * FROM application.image_caption_cache WHERE cache_key=$1',key)
            if row and row['status']=='ready':
                frame=await self._load(row)
                return frame,await self._use(document_id,key,row,True)
            claim=await self.pool.fetchrow("""INSERT INTO application.image_caption_cache
                (cache_key,collection_id,raw_sha256,contract_sha256,status,owner,lease_until)
                VALUES($1,$2,$3,$4,'running',$5,clock_timestamp()+$6::interval)
                ON CONFLICT(cache_key) DO UPDATE SET owner=EXCLUDED.owner,fence=image_caption_cache.fence+1,
                 lease_until=EXCLUDED.lease_until,updated_at=clock_timestamp()
                WHERE image_caption_cache.status='running' AND
                 (image_caption_cache.owner IS NULL OR image_caption_cache.lease_until<=clock_timestamp())
                RETURNING *""",key,collection,raw_sha,contract_sha,owner,self.lease)
            if claim:break
            if time.monotonic()>=deadline:raise TimeoutError('Shared image caption wait exceeded retry budget')
            await asyncio.sleep(self.poll)
        fence=claim['fence'];lost=asyncio.Event();heartbeat=asyncio.create_task(self._heartbeat(key,owner,fence,lost))
        try:
            frame=await compute()
            # The caller validates native success before returning. Serialization
            # and a fresh read happen before publishing reusable cache authority.
            payload=await asyncio.to_thread(encode_native,frame)
            compressed=await asyncio.to_thread(gzip.compress,payload,mtime=0)
            from nv_ingest_durable.deferred_caption import retain
            uri,sha=await asyncio.to_thread(retain,self.root,compressed,'.image-caption.json.gz')
            row={'artifact_uri':uri,'artifact_sha256':sha.removeprefix('sha256:')}
            fresh=await self._load(row)
            if lost.is_set():raise RuntimeError('Shared image caption lease lost')
            committed=await self.pool.fetchrow("""UPDATE application.image_caption_cache
                SET status='ready',artifact_uri=$4,artifact_sha256=$5,owner=NULL,lease_until=NULL,updated_at=clock_timestamp()
                WHERE cache_key=$1 AND owner=$2 AND fence=$3 AND status='running' AND lease_until>clock_timestamp()
                RETURNING *""",key,owner,fence,uri,row['artifact_sha256'])
            if not committed:raise RuntimeError('Shared image caption fenced')
            return fresh,await self._use(document_id,key,committed,False)
        except BaseException:
            await asyncio.shield(self.pool.execute("""UPDATE application.image_caption_cache SET owner=NULL,
                lease_until=clock_timestamp(),updated_at=clock_timestamp()
                WHERE cache_key=$1 AND owner=$2 AND fence=$3 AND status='running'""",key,owner,fence))
            raise
        finally:
            heartbeat.cancel();await asyncio.gather(heartbeat,return_exceptions=True)
