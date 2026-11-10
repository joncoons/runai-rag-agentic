import asyncio,os,tempfile,unittest,json
from pathlib import Path
from uuid import uuid4

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'isolated PostgreSQL required')
class PdfExtractionCacheTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  import asyncpg
  self.pool=await asyncpg.create_pool(os.environ['COLLECTION_TEST_DSN'])
  await self.pool.execute(Path('deployment/application/011_pdf_extraction_cache.sql').read_text())
  await self.pool.execute('TRUNCATE application.pdf_extraction_uses,application.pdf_extraction_cache')
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.addAsyncCleanup(self.pool.close)
  self.root=Path(self.temp.name)
 async def test_concurrent_duplicates_share_extraction_and_mutable_results_do_not_leak(self):
  import pandas as pd
  from application.pdf_extraction_cache import PdfExtractionCache
  calls=0
  async def compute():
   nonlocal calls;calls+=1;await asyncio.sleep(.15)
   frame=pd.DataFrame([{'text':'native text','metadata':{'page':1},'bytes':b'abc'}]);frame.attrs['pages']=[1];return frame
  caches=[PdfExtractionCache(self.pool,self.root,poll_seconds=.01) for _ in range(4)]
  results=await asyncio.gather(*(c.get_or_compute('corpus','a'*64,'b'*64,compute,document_id='doc-'+str(i)) for i,c in enumerate(caches)))
  self.assertEqual(calls,1);self.assertEqual(sum(p['cache_hit'] for _,p in results),3)
  results[0][0].iloc[0]['metadata']['page']=99
  self.assertEqual(results[1][0].iloc[0]['metadata']['page'],1)
  self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.pdf_extraction_uses'),4)
 async def test_changed_content_contract_or_scope_does_not_reuse(self):
  import pandas as pd
  from application.pdf_extraction_cache import PdfExtractionCache
  calls=0
  async def compute():
   nonlocal calls;calls+=1;return pd.DataFrame([{'text':'x'}])
  cache=PdfExtractionCache(self.pool,self.root)
  for i,(scope,raw,contract) in enumerate([('a','a','b'),('a','c','b'),('a','a','d'),('b','a','b')]):await cache.get_or_compute(scope,raw*64,contract*64,compute,document_id=str(i))
  self.assertEqual(calls,4)
 async def test_failed_computation_not_reusable_and_abandoned_owner_can_be_recovered(self):
  import pandas as pd
  from application.pdf_extraction_cache import PdfExtractionCache
  cache=PdfExtractionCache(self.pool,self.root,lease_seconds=.15,poll_seconds=.01)
  async def bad():raise ValueError('partial native output')
  with self.assertRaises(ValueError):await cache.get_or_compute('a','a'*64,'b'*64,bad,document_id='bad')
  async def good():return pd.DataFrame([{'text':'complete'}])
  _,proof=await cache.get_or_compute('a','a'*64,'b'*64,good,document_id='good')
  self.assertFalse(proof['cache_hit'])
  await self.pool.execute("UPDATE application.pdf_extraction_cache SET status='running',lease_until=clock_timestamp()-interval '1 second',owner=$1",uuid4())
  _,proof=await cache.get_or_compute('a','a'*64,'b'*64,good,document_id='recovered');self.assertFalse(proof['cache_hit'])
 async def test_corrupted_artifact_rejected(self):
  import pandas as pd
  from application.pdf_extraction_cache import PdfExtractionCache
  cache=PdfExtractionCache(self.pool,self.root)
  async def compute():return pd.DataFrame([{'text':'x'}])
  _,proof=await cache.get_or_compute('a','a'*64,'b'*64,compute,document_id='one')
  from urllib.parse import urlsplit,unquote
  Path(unquote(urlsplit(proof['artifact_uri']).path)).write_bytes(b'corrupt')
  with self.assertRaises(Exception):await cache.get_or_compute('a','a'*64,'b'*64,compute,document_id='two')
  self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.pdf_extraction_uses'),1)

class NativeExtractionCodecTests(unittest.TestCase):
 def test_scalar_object_columns_preserve_none_and_integer_types(self):
  import pandas as pd
  from application.pdf_extraction_cache import encode_native,decode_native
  frame=pd.DataFrame({'value':pd.Series([1,None],dtype=object)})
  out=decode_native(encode_native(frame))
  self.assertIsNone(out.iloc[1]['value'])
  self.assertIs(type(out.iloc[0]['value']),int)

 def test_nested_native_types_and_attrs_roundtrip_without_executable_objects(self):
  import pandas as pd,numpy as np
  from application.pdf_extraction_cache import encode_native,decode_native
  frame=pd.DataFrame([{'text':'é','bytes':b'\x00\xff','metadata':{'tuple':(1,2),'array':np.array([[1,2]],dtype='float32'),'nan':float('nan')}}]);frame.attrs={'page_completions':[{'page':1}]}
  result=decode_native(encode_native(frame))
  self.assertEqual(result.iloc[0]['bytes'],b'\x00\xff');self.assertEqual(result.attrs,frame.attrs)
  self.assertEqual(result.iloc[0]['metadata']['tuple'],(1,2));self.assertEqual(result.iloc[0]['metadata']['array'].dtype,np.dtype('float32'))
  self.assertTrue(np.isnan(result.iloc[0]['metadata']['nan']))

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'isolated PostgreSQL required')
class PdfReusePipelineTests(unittest.IsolatedAsyncioTestCase):
 asyncSetUp=PdfExtractionCacheTests.asyncSetUp
 # Do not inherit fixture tests: explicit suite discovery below avoids duplicate
 # imported helper classes; these integration cases use the same isolated setup.
 async def test_distinct_source_jobs_share_native_extraction_but_get_own_caption_keys(self):
  import hashlib,base64
  from io import BytesIO
  from PIL import Image
  import pandas as pd
  from unittest.mock import patch
  from nv_ingest_durable.contracts import Stage,StageEnvelope
  from nv_ingest_durable.pipeline_worker import build_processor,PipelineWorkerSettings
  from tests.ingestion.test_native_content import NativeContentTests
  h=NativeContentTests();h.setUp();self.addCleanup(h.doCleanups)
  root=self.root/'stages';root.mkdir();raw_root=self.root/'raw';raw_root.mkdir();data=b'%PDF controlled raw bytes';raw=raw_root/'input.pdf';raw.write_bytes(data)
  row={'collection_id':'corpus','raw_sha256':hashlib.sha256(data).hexdigest(),'raw_uri':raw.as_uri(),'source_kind':'pdf','page_count':1}
  # Persisted source lookup is independent of cache queries and can be isolated
  # here; cache authority itself uses real PostgreSQL.
  settings=PipelineWorkerSettings(role='fused',postgres_dsn='unused',redis_url='unused',artifact_root=root,stream='test',consumer_group='test',consumer='test',concurrency=1,read_count=1,caption_enabled=True)
  processor=build_processor(settings,pool=self.pool)
  buf=BytesIO();Image.new('RGB',(64,64),'blue').save(buf,'PNG');image=base64.b64encode(buf.getvalue()).decode()
  calls=0
  class Native:
   async def run(self,**kwargs):
    nonlocal calls;calls+=1
    return pd.DataFrame([{'page_number':1,'text':'Grounded PDF text','metadata':{},'images':[{'image_b64':image,'text':''}]}])
  processor.runner.extraction=Native()
  async def config(envelope):return {**row,'document_id':envelope.document_id}
  processor.configuration_loader=config
  def embedding(actor,frame,**kwargs):
   out=frame.copy();out[actor._params.output_column]=[{'embedding':[1.0]+[0.0]*2047} for _ in range(len(out))];return out
  outcomes=[]
  with patch('nemo_retriever.operators.embed.cpu_operator._BatchEmbedCPUActor.process',embedding):
   for n in range(2):
    e=StageEnvelope(job_id=str(uuid4()),document_id='url-document-'+str(n),stage=Stage.EXTRACTION,artifact_uri=raw.as_uri(),idempotency_key='sha256:'+str(n+1)*64,pipeline_version='test',config_digest='sha256:'+'1'*64,model_digest='sha256:'+'1'*64)
    result=await processor.process(e);outcomes.append(json.loads(Path(result.artifact.temporary_uri.removeprefix('file://')).read_text()))
  self.assertEqual(calls,1)
  self.assertNotEqual(outcomes[0]['document_id'],outcomes[1]['document_id'])
  self.assertNotEqual(outcomes[0]['caption_jobs'][0]['stage_key'],outcomes[1]['caption_jobs'][0]['stage_key'])
  self.assertEqual(outcomes[0]['native_page_completions'],outcomes[1]['native_page_completions'])
  self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.pdf_extraction_uses'),2)
  from dataclasses import replace
  disabled=build_processor(replace(settings,caption_enabled=False),pool=self.pool)
  disabled.runner.extraction=Native();disabled.configuration_loader=config
  with patch('nemo_retriever.operators.embed.cpu_operator._BatchEmbedCPUActor.process',embedding):
   e=replace(e,job_id=str(uuid4()),document_id='captions-disabled',idempotency_key='sha256:'+'c'*64)
   result=await disabled.process(e)
  body=json.loads(Path(result.artifact.temporary_uri.removeprefix('file://')).read_text())
  self.assertEqual(calls,1)
  self.assertFalse(body.get('caption_jobs'))

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'isolated PostgreSQL required')
class PdfCacheLeaseTests(unittest.IsolatedAsyncioTestCase):
 asyncSetUp=PdfExtractionCacheTests.asyncSetUp
 async def test_renewed_lease_prevents_duplicate_work(self):
  import pandas as pd
  from application.pdf_extraction_cache import PdfExtractionCache
  cache=PdfExtractionCache(self.pool,self.root,lease_seconds=.09,poll_seconds=.01);calls=0
  async def compute():
   nonlocal calls;calls+=1;await asyncio.sleep(.35);return pd.DataFrame([{'text':'complete'}])
  first=asyncio.create_task(cache.get_or_compute('a','a'*64,'b'*64,compute,document_id='1'))
  await asyncio.sleep(.15)
  await asyncio.gather(first,cache.get_or_compute('a','a'*64,'b'*64,compute,document_id='2'))
  self.assertEqual(calls,1)
 async def test_stale_owner_cannot_publish_and_wait_is_bounded(self):
  import pandas as pd
  from application.pdf_extraction_cache import PdfExtractionCache
  cache=PdfExtractionCache(self.pool,self.root,poll_seconds=.01,wait_seconds=.05)
  entered=asyncio.Event();release=asyncio.Event()
  async def compute():entered.set();await release.wait();return pd.DataFrame([{'text':'complete'}])
  task=asyncio.create_task(cache.get_or_compute('a','a'*64,'b'*64,compute,document_id='old'))
  await entered.wait()
  with self.assertRaises(TimeoutError):await cache.get_or_compute('a','a'*64,'b'*64,compute,document_id='waiter')
  await self.pool.execute('UPDATE application.pdf_extraction_cache SET owner=$1,fence=fence+1',uuid4())
  release.set()
  with self.assertRaisesRegex(RuntimeError,'fenced|lease lost'):await task
  self.assertEqual(await self.pool.fetchval("SELECT count(*) FROM application.pdf_extraction_cache WHERE status='ready'"),0)

class NativeExtractionCoverageTests(unittest.TestCase):
 def test_partial_or_error_output_cannot_be_cached_as_complete(self):
  import pandas as pd
  from application.pdf_extraction_cache import validate_native_extraction
  for frame,pages in [(pd.DataFrame([{'page_number':1,'text':'partial'}]),2),(pd.DataFrame([{'page_number':1,'ocr':{'error':'failed'}}]),1)]:
   with self.assertRaises(ValueError):validate_native_extraction(frame,pages)
