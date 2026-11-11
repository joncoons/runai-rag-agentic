"""Create-only attempt generations with complete source-byte visibility checks.

Adapters never mutate an existing generation, alias or application collection.
Their credentials belong only to an independent trusted persistence process.
"""
import asyncio
import json
import re
from uuid import uuid4

from .artifacts import canonical, content_digest, record_digest
from .record_payloads import encode_payload,decode_payload
from .repository import Conflict


def require_handle(handle,suffix):
    if not re.fullmatch(r'(?:rag_(?:(?:agentic|analysis)_)?[0-9a-f]{32}_[1-9][0-9]*|rag_agentic_(?:inc|native)_[0-9a-f]{32})_'+suffix,handle):
        raise Conflict('Only generated attempt-scoped physical handles are writable')


class ElasticsearchProjection:
    def __init__(self,session,endpoint):
        self.session=session
        self.endpoint=endpoint.rstrip('/')

    async def _request(self,method,path,**kwargs):
        async with self.session.request(method,self.endpoint+'/'+path,**kwargs) as r:
            if r.status>=300:raise Conflict('Elasticsearch operation failed: HTTP '+str(r.status))
            body=await r.json()
            if body.get('_shards',{}).get('failed',0):raise Conflict('Elasticsearch shard operation failed')
            return body

    async def write_verify(self,handle,artifact):
        require_handle(handle,'es')
        records=artifact['records']
        await self._request('PUT',handle,json={'mappings':{'dynamic':'strict','properties':{'record_id':{'type':'keyword'},'text':{'type':'text'},'metadata':{'type':'object','enabled':False},'record_sha256':{'type':'keyword'},'vector':{'type':'dense_vector','dims':2048,'index':True,'similarity':'dot_product','index_options':{'type':'hnsw'}}}}})
        for offset in range(0,len(records),64):
            batch=records[offset:offset+64];parts=[]
            for record in batch:
                parts.extend([canonical({'create':{'_index':handle,'_id':record['record_id']}}),canonical({**record,'record_sha256':record_digest(record)})])
            body=await self._request('POST','_bulk',data=b'\n'.join(parts)+b'\n',headers={'Content-Type':'application/x-ndjson'})
            items=body.get('items',[])
            if body.get('errors') or len(items)!=len(batch):raise Conflict('ES per-item write incomplete')
            for record,item in zip(batch,items):
                result=item.get('create',{})
                if result.get('status')!=201 or result.get('_id')!=record['record_id']:raise Conflict('ES create receipt identity/status mismatch')
        await self._request('POST',handle+'/_refresh')
        count=await self._request('GET',handle+'/_count')
        if count['count']!=len(records):raise Conflict('ES visible count mismatch')
        recovered=[]
        for offset in range(0,len(records),64):
            batch=records[offset:offset+64]
            response=await self._request('POST',handle+'/_mget',json={'ids':[r['record_id'] for r in batch]})
            docs=response.get('docs',[])
            if len(docs)!=len(batch):raise Conflict('ES visible identities incomplete')
            for expected,doc in zip(batch,docs):
                if not doc.get('found') or doc.get('_id')!=expected['record_id']:raise Conflict('ES record is not visible')
                source=doc['_source'];stored=source.pop('record_sha256')
                if record_digest(source)!=stored or stored!=record_digest(expected):raise Conflict('ES stored vector/payload digest mismatch')
                recovered.append(source)
        return content_digest(recovered),len(recovered)


class MilvusProjection:
    def __init__(self,uri,token=None,payload_root=None):
        self.uri=uri;self.token=token;self.payload_root=payload_root

    async def write_verify(self,handle,artifact):
        require_handle(handle,'mv')
        return await asyncio.to_thread(self._write_verify,handle,artifact)

    def _write_verify(self,handle,artifact):
        from pymilvus import Collection,CollectionSchema,FieldSchema,DataType,connections,utility
        alias='generation_'+uuid4().hex
        options={'alias':alias,'uri':self.uri}
        if self.token:options['token']=self.token
        connections.connect(**options)
        try:
            if utility.has_collection(handle,using=alias):raise Conflict('Refusing to mutate an existing Milvus generation')
            schema=CollectionSchema([
                FieldSchema('record_id',DataType.VARCHAR,is_primary=True,max_length=1024),
                FieldSchema('vector',DataType.FLOAT_VECTOR,dim=2048),
                FieldSchema('payload',DataType.JSON),
                FieldSchema('record_sha256',DataType.VARCHAR,max_length=64),
            ],description='PostgreSQL-controlled immutable generation attempt')
            c=Collection(handle,schema,using=alias,consistency_level='Strong')
            records=artifact['records']
            for offset in range(0,len(records),128):
                batch=records[offset:offset+128]
                c.insert([[r['record_id'] for r in batch],[r['vector'] for r in batch],[encode_payload(r,self.payload_root) for r in batch],[record_digest(r) for r in batch]])
            c.flush(timeout=300)
            if c.num_entities!=len(records):raise Conflict('Milvus flushed row count mismatch')
            c.create_index('vector',{'index_type':'GPU_CAGRA','metric_type':'IP','params':{'intermediate_graph_degree':64,'graph_degree':32,'build_algo':'NN_DESCENT','cache_dataset_on_device':'true','adapt_for_cpu':'false'}})
            utility.wait_for_index_building_complete(handle,using=alias,timeout=900)
            c.load(timeout=900);utility.wait_for_loading_complete(handle,using=alias,timeout=900)
            if not any(i.params.get('index_type')=='GPU_CAGRA' and i.params.get('metric_type')=='IP' for i in c.indexes):raise Conflict('Milvus GPU_CAGRA/IP index not visible')
            recovered=[]
            for offset in range(0,len(records),64):
                batch=records[offset:offset+64];ids=[r['record_id'] for r in batch]
                rows=c.query(expr='record_id in '+json.dumps(ids),output_fields=['record_id','vector','payload','record_sha256'],consistency_level='Strong',timeout=120)
                if len(rows)!=len(batch) or {r['record_id'] for r in rows}!=set(ids):raise Conflict('Milvus visible identities incomplete')
                expected={r['record_id']:r for r in batch}
                for row in rows:
                    restored={'record_id':row['record_id'],'vector':row['vector'],**decode_payload(row['payload'],self.payload_root)}
                    if record_digest(restored)!=row['record_sha256'] or row['record_sha256']!=record_digest(expected[row['record_id']]):raise Conflict('Milvus stored vector/payload digest mismatch')
                    recovered.append(restored)
            # A real GPU search barrier complements row visibility checks.
            hits=c.search([records[0]['vector']],'vector',{'metric_type':'IP','params':{'itopk_size':64,'search_width':8}},limit=5,consistency_level='Strong')
            if not hits or not hits[0]:raise Conflict('Milvus search visibility check failed')
            return content_digest(recovered),len(recovered)
        finally:
            connections.disconnect(alias)
