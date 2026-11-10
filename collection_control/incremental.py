"""Persistent immutable record-version projections; PostgreSQL selects visibility.

Insertion never overwrites a different logical record version. Backend rows are
not authorization: readers must filter exact published hashes before top-k.
"""
import asyncio,hashlib,json,re
from uuid import uuid4
from .artifacts import canonical,content_digest,record_digest
from .repository import Conflict
from .native_milvus import native_handle,schema_fields,columns,restore
from .stores import ElasticsearchProjection,MilvusProjection

def version_handle(collection_id,backend):
 if backend not in ('elasticsearch','milvus'):raise Conflict('Unknown projection backend')
 return ('rag_agentic_inc_' if backend=='elasticsearch' else 'rag_agentic_native_')+hashlib.sha256(collection_id.encode()).hexdigest()[:32]+('_es' if backend=='elasticsearch' else '_mv')

def incremental_handle(handle):
 return isinstance(handle,str) and re.fullmatch(r'rag_agentic_(?:inc_[0-9a-f]{32}_(?:es|mv)|native_[0-9a-f]{32}_mv)',handle) is not None

def _require(handle,suffix):
 if not incremental_handle(handle) or not handle.endswith('_'+suffix):raise Conflict('Invalid incremental projection identity')

def _verify(expected,actual):
 body={k:actual[k] for k in ('record_id','text','metadata','vector')}
 if record_digest(body)!=record_digest(expected):raise Conflict('Stored immutable record-version differs')

class VerificationLedger:
 pool=None
 async def verified(self,handle,backend,records):
  if self.pool is None:return set()
  rows=await self.pool.fetch('SELECT record_sha256 FROM collection_control.projection_versions WHERE handle=$1 AND backend=$2 AND record_sha256=ANY($3::text[])',handle,backend,[record_digest(r) for r in records])
  return {r['record_sha256'] for r in rows}
 async def remember(self,handle,backend,records):
  if self.pool is not None and records:
   await self.pool.execute('INSERT INTO collection_control.projection_versions(handle,backend,record_sha256) SELECT $1,$2,unnest($3::text[]) ON CONFLICT DO NOTHING',handle,backend,[record_digest(r) for r in records])

class IncrementalElasticsearch(VerificationLedger,ElasticsearchProjection):
 async def write_verify(self,handle,artifact):
  if not incremental_handle(handle):return await super().write_verify(handle,artifact)
  _require(handle,'es');records=artifact['records'];verified=await self.verified(handle,'elasticsearch',records);needs_verification=[]
  try:await self._request('GET',handle)
  except Conflict as error:
   if 'HTTP 404' not in str(error):raise
   verified=set()
   await self._request('PUT',handle,json={'mappings':{'dynamic':'strict','properties':{'record_id':{'type':'keyword'},'text':{'type':'text'},'metadata':{'type':'object','enabled':False},'record_sha256':{'type':'keyword'},'vector':{'type':'dense_vector','dims':2048,'index':True,'similarity':'dot_product','index_options':{'type':'hnsw'}}}}})
  missing=[];unverified=[r for r in records if record_digest(r) not in verified]
  for offset in range(0,len(unverified),128):
   batch=unverified[offset:offset+128];response=await self._request('POST',handle+'/_mget',json={'ids':[record_digest(r) for r in batch]},params={'_source':'record_sha256'})
   docs=response.get('docs',[])
   if len(docs)!=len(batch):raise Conflict('Version existence check incomplete')
   for record,doc in zip(batch,docs):
    version=record_digest(record)
    if doc.get('_id')!=version:raise Conflict('Version identity mismatch')
    if not doc.get('found'):missing.append(record);needs_verification.append(record)
    elif doc.get('_source',{}).get('record_sha256')!=version:raise Conflict('Existing version digest differs')
    elif version not in verified:needs_verification.append(record)
  for offset in range(0,len(missing),128):
   batch=missing[offset:offset+128];parts=[]
   for record in batch:
    version=record_digest(record);parts.extend([canonical({'create':{'_index':handle,'_id':version}}),canonical({**record,'record_sha256':version})])
   response=await self._request('POST','_bulk',data=b'\n'.join(parts)+b'\n',headers={'Content-Type':'application/x-ndjson'})
   items=response.get('items',[])
   if len(items)!=len(batch):raise Conflict('Incremental ES bulk incomplete')
   for record,item in zip(batch,items):
    result=item.get('create',{})
    if result.get('_id')!=record_digest(record) or result.get('status') not in (201,409):raise Conflict('Incremental ES create failed')
  # Search visibility is required by the publication contract; no corpus rewrite.
  if records:await self._request('POST',handle+'/_refresh')
  for offset in range(0,len(needs_verification),128):
   batch=needs_verification[offset:offset+128];response=await self._request('POST',handle+'/_mget',json={'ids':[record_digest(r) for r in batch]})
   docs=response.get('docs',[])
   if len(docs)!=len(batch):raise Conflict('Incremental ES verification incomplete')
   for record,doc in zip(batch,docs):
    if not doc.get('found') or doc.get('_id')!=record_digest(record):raise Conflict('Written version missing')
    _verify(record,doc['_source'])
  await self.remember(handle,'elasticsearch',needs_verification)
  return content_digest(records),len(records)

class IncrementalMilvus(VerificationLedger,MilvusProjection):
 async def write_verify(self,handle,artifact):
  if not native_handle(handle):raise Conflict('New Milvus writes require the native text/vector/metadata projection')
  verified=await self.verified(handle,'milvus',artifact['records'])
  result,new=await asyncio.to_thread(self._incremental,handle,artifact,verified)
  await self.remember(handle,'milvus',new)
  return result
 def _incremental(self,handle,artifact,verified):
  _require(handle,'mv')
  from pymilvus import Collection,CollectionSchema,FieldSchema,DataType,connections,utility
  alias='incremental_'+uuid4().hex;options={'alias':alias,'uri':self.uri}
  if self.token:options['token']=self.token
  connections.connect(**options)
  try:
   if not utility.has_collection(handle,using=alias):
    verified=set()
    fields=schema_fields()
    schema=CollectionSchema(fields,description='Immutable versions; PostgreSQL authorizes exact visible set')
    c=Collection(handle,schema,using=alias,consistency_level='Strong')
   else:c=Collection(handle,using=alias,consistency_level='Strong')
   if not c.indexes:
    c.create_index('vector',{'index_type':'GPU_CAGRA','metric_type':'IP','params':{'intermediate_graph_degree':64,'graph_degree':32,'build_algo':'NN_DESCENT','cache_dataset_on_device':'true','adapt_for_cpu':'false'}})
   from pymilvus.client.types import LoadState
   if utility.load_state(handle,using=alias)!=LoadState.Loaded:c.load(timeout=120)
   if not any(i.params.get('index_type')=='GPU_CAGRA' and i.params.get('metric_type')=='IP' for i in c.indexes):raise Conflict('Persistent Milvus projection lacks GPU_CAGRA/IP')
   expected_fields={'version_id','record_id','vector','text','metadata'}
   if {f.name for f in c.schema.fields}!=expected_fields:raise Conflict('Milvus physical layout differs from its native/version handle')
   records=artifact['records']
   missing=[];needs_verification=[];unverified=[r for r in records if record_digest(r) not in verified]
   for offset in range(0,len(unverified),128):
    batch=unverified[offset:offset+128];versions=[record_digest(r) for r in batch]
    rows=c.query(expr='version_id in '+json.dumps(versions),output_fields=['version_id'],consistency_level='Strong',timeout=120)
    found={r['version_id'] for r in rows}
    missing.extend(r for r in batch if record_digest(r) not in found)
    needs_verification.extend(r for r in batch if record_digest(r) not in found or record_digest(r) not in verified)
   for offset in range(0,len(missing),128):
    batch=missing[offset:offset+128]
    # Identical content-addressed key always means identical bytes, including
    # concurrent late retries. Upsert deduplicates the same immutable version.
    c.upsert(columns(batch))
   for offset in range(0,len(needs_verification),128):
    batch=needs_verification[offset:offset+128]
    rows=c.query(expr='version_id in '+json.dumps([record_digest(r) for r in batch]),output_fields=['version_id','record_id','vector','text','metadata'],consistency_level='Strong',timeout=120)
    recovered={r['version_id']:r for r in rows}
    if set(recovered)!={record_digest(r) for r in batch}:raise Conflict('Incremental Milvus versions not strongly visible')
    for record in batch:
     row=recovered[record_digest(record)];_verify(record,restore(row))
   return (content_digest(records),len(records)),needs_verification
  finally:connections.disconnect(alias)
