"""Fenced native PDF extraction reuse; never shares URL publication authority."""
import asyncio
import base64
import gzip
import hashlib
import json
import math
import time
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

from collection_control.artifacts import canonical
from collection_control.provenance import verified_file


def _pack(value):
    import numpy as np
    import pandas as pd
    if value is pd.NA:return ['missing',None]
    if value is None or isinstance(value,(str,bool,int)):return ['scalar',value]
    if isinstance(value,float):
        return ['float',value if math.isfinite(value) else 'nan' if math.isnan(value) else 'inf' if value>0 else '-inf']
    if isinstance(value,bytes):return ['bytes',base64.b64encode(value).decode('ascii')]
    if isinstance(value,np.ndarray):
        if value.dtype.kind not in 'biufUSO':raise TypeError('Unsupported native array dtype')
        return ['array',[value.dtype.str,list(value.shape),_pack(value.tolist())]]
    if isinstance(value,np.generic):return _pack(value.item())
    if isinstance(value,dict):return ['dict',[[_pack(k),_pack(v)] for k,v in value.items()]]
    if isinstance(value,(tuple,list)):return ['tuple' if isinstance(value,tuple) else 'list',[_pack(v) for v in value]]
    raise TypeError('Unsupported native extraction value: '+type(value).__name__)


def _unpack(node):
    import numpy as np
    import pandas as pd
    kind,value=node
    if kind=='scalar':return value
    if kind=='missing':return pd.NA
    if kind=='float':return float(value)
    if kind=='bytes':return base64.b64decode(value,validate=True)
    if kind in ('tuple','list'):
        items=[_unpack(v) for v in value];return tuple(items) if kind=='tuple' else items
    if kind=='dict':return {_unpack(k):_unpack(v) for k,v in value}
    if kind=='array':
        dtype=np.dtype(value[0])
        if dtype.kind not in 'biufUSO':raise ValueError('Invalid native array dtype')
        return np.array(_unpack(value[2]),dtype=dtype).reshape(value[1])
    raise ValueError('Invalid native extraction value tag')


def encode_native(frame):
    return canonical({'schema':'native-pdf-extraction.v1','columns':_pack(list(frame.columns)),
        'rows':_pack(frame.to_numpy(dtype=object).tolist()),'index':_pack(list(frame.index)),
        'dtypes':[str(dtype) for dtype in frame.dtypes],'attrs':_pack(frame.attrs)})


def decode_native(raw):
    import pandas as pd
    body=json.loads(raw)
    if body['schema']!='native-pdf-extraction.v1':raise ValueError('Invalid native extraction schema')
    frame=pd.DataFrame(_unpack(body['rows']),columns=_unpack(body['columns']),index=_unpack(body['index']),dtype=object)
    for column,dtype in zip(frame.columns,body['dtypes']):frame[column]=frame[column].astype(dtype)
    frame.attrs=_unpack(body['attrs']);return frame


class PdfExtractionCache:
    def __init__(self,pool,root,*,lease_seconds=90,poll_seconds=.2,wait_seconds=900):
        self.pool=pool;self.root=Path(root);self.lease=timedelta(seconds=lease_seconds)
        self.poll=poll_seconds;self.wait=wait_seconds

    async def _heartbeat(self,key,owner,fence,lost):
        try:
            while True:
                await asyncio.sleep(max(.02,self.lease.total_seconds()/3))
                result=await self.pool.fetchval("""UPDATE application.pdf_extraction_cache
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
        await self.pool.execute('''INSERT INTO application.pdf_extraction_uses(document_id,cache_key,artifact_sha256,cache_hit)
            VALUES($1,$2,$3,$4) ON CONFLICT(document_id,cache_key) DO NOTHING''',document,key,row['artifact_sha256'],hit)
        return {'cache_key':key,'cache_hit':hit,'artifact_uri':row['artifact_uri'],'artifact_sha256':row['artifact_sha256']}

    async def get_or_compute(self,collection,raw_sha,contract_sha,compute,*,document_id):
        if not collection or any(len(h)!=64 or any(c not in '0123456789abcdef' for c in h) for h in (raw_sha,contract_sha)):
            raise ValueError('Cache requires exact scope, content and contract hashes')
        key=hashlib.sha256(canonical({'version':1,'collection':collection,'raw':raw_sha,'contract':contract_sha})).hexdigest()
        deadline=time.monotonic()+self.wait;owner=uuid4()
        while True:
            row=await self.pool.fetchrow('SELECT * FROM application.pdf_extraction_cache WHERE cache_key=$1',key)
            if row and row['status']=='ready':
                frame=await self._load(row)
                return frame,await self._use(document_id,key,row,True)
            claim=await self.pool.fetchrow("""INSERT INTO application.pdf_extraction_cache
                (cache_key,collection_id,raw_sha256,contract_sha256,status,owner,lease_until)
                VALUES($1,$2,$3,$4,'running',$5,clock_timestamp()+$6::interval)
                ON CONFLICT(cache_key) DO UPDATE SET owner=EXCLUDED.owner,fence=pdf_extraction_cache.fence+1,
                 lease_until=EXCLUDED.lease_until,updated_at=clock_timestamp()
                WHERE pdf_extraction_cache.status='running' AND
                 (pdf_extraction_cache.owner IS NULL OR pdf_extraction_cache.lease_until<=clock_timestamp())
                RETURNING *""",key,collection,raw_sha,contract_sha,owner,self.lease)
            if claim:break
            if time.monotonic()>=deadline:raise TimeoutError('Shared PDF extraction wait exceeded retry budget')
            await asyncio.sleep(self.poll)
        fence=claim['fence'];lost=asyncio.Event();heartbeat=asyncio.create_task(self._heartbeat(key,owner,fence,lost))
        try:
            frame=await compute()
            # The caller validates native success before returning. Serialization
            # and a fresh read happen before publishing reusable cache authority.
            payload=await asyncio.to_thread(encode_native,frame)
            compressed=await asyncio.to_thread(gzip.compress,payload,mtime=0)
            from nv_ingest_durable.deferred_caption import retain
            uri,sha=await asyncio.to_thread(retain,self.root,compressed,'.native-pdf.json.gz')
            row={'artifact_uri':uri,'artifact_sha256':sha.removeprefix('sha256:')}
            fresh=await self._load(row)
            if lost.is_set():raise RuntimeError('Shared PDF extraction lease lost')
            committed=await self.pool.fetchrow("""UPDATE application.pdf_extraction_cache
                SET status='ready',artifact_uri=$4,artifact_sha256=$5,owner=NULL,lease_until=NULL,updated_at=clock_timestamp()
                WHERE cache_key=$1 AND owner=$2 AND fence=$3 AND status='running' AND lease_until>clock_timestamp()
                RETURNING *""",key,owner,fence,uri,row['artifact_sha256'])
            if not committed:raise RuntimeError('Shared PDF extraction fenced')
            return fresh,await self._use(document_id,key,committed,False)
        except BaseException:
            await asyncio.shield(self.pool.execute("""UPDATE application.pdf_extraction_cache SET owner=NULL,
                lease_until=clock_timestamp(),updated_at=clock_timestamp()
                WHERE cache_key=$1 AND owner=$2 AND fence=$3 AND status='running'""",key,owner,fence))
            raise
        finally:
            heartbeat.cancel();await asyncio.gather(heartbeat,return_exceptions=True)


def extraction_contract(extract_config):
    """Fingerprint effective parameters, model artifacts and installed code."""
    from dataclasses import asdict
    from importlib.metadata import distributions,version
    import nemo_retriever
    import nv_ingest_durable.nemo_blueprints as blueprint
    import os
    source_root=Path(nemo_retriever.__file__).resolve().parent
    files={str(p.relative_to(source_root)):hashlib.sha256(p.read_bytes()).hexdigest()
           for p in sorted(source_root.rglob('*.py'))}
    model_path=Path(os.environ.get('MODEL_CONTRACTS_FILE','/app/deployment/application/workspaces-model-contracts.json'))
    if not model_path.is_file():model_path=Path(__file__).resolve().parents[1]/'deployment/application/workspaces-model-contracts.json'
    body={'schema':'pdf-extraction-contract.v1','library_version':version('nemo-retriever'),
          'library_sources':files,'parameters':asdict(extract_config),
          'model_contracts':json.loads(model_path.read_text())['extraction_model_contract'],
          'blueprint_sha256':hashlib.sha256(Path(blueprint.__file__).read_bytes()).hexdigest(),
          'cache_adapter_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
          'dependencies':sorted((d.metadata['Name'],d.version) for d in distributions() if d.metadata['Name'])}
    return hashlib.sha256(canonical(body)).hexdigest()


def validate_native_extraction(frame,expected_pages):
    from nv_ingest_durable.admission import require_successful_upstream
    require_successful_upstream(frame)
    if not isinstance(expected_pages,int) or isinstance(expected_pages,bool) or expected_pages<1:
        raise ValueError('PDF reuse requires positive observed page coverage')
    pages=[]
    for _,row in frame.iterrows():
        number=row.get('page_number',row.get('_page_number'))
        if isinstance(number,bool) or number is None or int(number)!=number:
            raise ValueError('Native PDF page identity is invalid')
        pages.append(int(number))
    if len(pages)!=expected_pages or set(pages)!=set(range(1,expected_pages+1)):
        raise ValueError('Native PDF extraction page coverage is incomplete')
    return frame


class PdfReuseRunner:
    def __init__(self,extraction,*,cache,tail_factory,contract,raw_root):
        self.extraction=extraction;self.cache=cache;self.tail_factory=tail_factory
        self.contract=contract;self.raw_root=Path(raw_root)

    async def run(self,*,graph_name,artifact_uri,configuration):
        from nv_ingest_durable.nemo_blueprints import BLUEPRINT_NAME
        if artifact_uri!=configuration['raw_uri']:raise ValueError('PDF extraction input differs from capture')
        await asyncio.to_thread(verified_file,self.raw_root,artifact_uri,configuration['raw_sha256'])
        async def compute():
            frame=await self.extraction.run(graph_name=BLUEPRINT_NAME,artifact_uri=artifact_uri,configuration={})
            return validate_native_extraction(frame,configuration['page_count'])
        frame,proof=await self.cache.get_or_compute(configuration['collection_id'],configuration['raw_sha256'],
            self.contract,compute,document_id=configuration['document_id'])
        validate_native_extraction(frame,configuration['page_count'])
        import logging
        logging.getLogger(__name__).info('pdf_extraction_reuse document=%s cache_hit=%s key=%s',
            configuration['document_id'],proof['cache_hit'],proof['cache_key'])
        # Every consumer receives an independently decoded native result; only
        # this tail may create caption associations, embeddings and checkpoints.
        return await asyncio.to_thread(self.tail_factory().ingest,frame)
