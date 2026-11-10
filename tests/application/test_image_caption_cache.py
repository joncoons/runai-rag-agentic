import asyncio,os,tempfile,unittest,json
from pathlib import Path
from uuid import uuid4

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'isolated PostgreSQL required')
class ImageCaptionCacheTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  import asyncpg
  self.pool=await asyncpg.create_pool(os.environ['COLLECTION_TEST_DSN'])
  await self.pool.execute(Path('deployment/application/013_image_caption_cache.sql').read_text())
  await self.pool.execute('TRUNCATE application.image_caption_uses,application.image_caption_cache')
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.addAsyncCleanup(self.pool.close)
  self.root=Path(self.temp.name)
 async def test_concurrent_duplicates_share_extraction_and_mutable_results_do_not_leak(self):
  import pandas as pd
  from application.image_caption_cache import ImageCaptionCache
  calls=0
  async def compute():
   nonlocal calls;calls+=1;await asyncio.sleep(.15)
   frame=pd.DataFrame([{'text':'native text','metadata':{'page':1},'bytes':b'abc'}]);frame.attrs['pages']=[1];return frame
  caches=[ImageCaptionCache(self.pool,self.root,poll_seconds=.01) for _ in range(4)]
  results=await asyncio.gather(*(c.get_or_compute('corpus','a'*64,'b'*64,compute,document_id='doc-'+str(i)) for i,c in enumerate(caches)))
  self.assertEqual(calls,1);self.assertEqual(sum(p['cache_hit'] for _,p in results),3)
  results[0][0].iloc[0]['metadata']['page']=99
  self.assertEqual(results[1][0].iloc[0]['metadata']['page'],1)
  self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.image_caption_uses'),4)
 async def test_changed_content_contract_or_scope_does_not_reuse(self):
  import pandas as pd
  from application.image_caption_cache import ImageCaptionCache
  calls=0
  async def compute():
   nonlocal calls;calls+=1;return pd.DataFrame([{'text':'x'}])
  cache=ImageCaptionCache(self.pool,self.root)
  for i,(scope,raw,contract) in enumerate([('a','a','b'),('a','c','b'),('a','a','d'),('b','a','b')]):await cache.get_or_compute(scope,raw*64,contract*64,compute,document_id=str(i))
  self.assertEqual(calls,4)
 async def test_failed_computation_not_reusable_and_abandoned_owner_can_be_recovered(self):
  import pandas as pd
  from application.image_caption_cache import ImageCaptionCache
  cache=ImageCaptionCache(self.pool,self.root,lease_seconds=.15,poll_seconds=.01)
  async def bad():raise ValueError('partial native output')
  with self.assertRaises(ValueError):await cache.get_or_compute('a','a'*64,'b'*64,bad,document_id='bad')
  async def good():return pd.DataFrame([{'text':'complete'}])
  _,proof=await cache.get_or_compute('a','a'*64,'b'*64,good,document_id='good')
  self.assertFalse(proof['cache_hit'])
  await self.pool.execute("UPDATE application.image_caption_cache SET status='running',lease_until=clock_timestamp()-interval '1 second',owner=$1",uuid4())
  _,proof=await cache.get_or_compute('a','a'*64,'b'*64,good,document_id='recovered');self.assertFalse(proof['cache_hit'])
 async def test_corrupted_artifact_rejected(self):
  import pandas as pd
  from application.image_caption_cache import ImageCaptionCache
  cache=ImageCaptionCache(self.pool,self.root)
  async def compute():return pd.DataFrame([{'text':'x'}])
  _,proof=await cache.get_or_compute('a','a'*64,'b'*64,compute,document_id='one')
  from urllib.parse import urlsplit,unquote
  Path(unquote(urlsplit(proof['artifact_uri']).path)).write_bytes(b'corrupt')
  with self.assertRaises(Exception):await cache.get_or_compute('a','a'*64,'b'*64,compute,document_id='two')
  self.assertEqual(await self.pool.fetchval('SELECT count(*) FROM application.image_caption_uses'),1)

 async def test_invalid_vector_never_becomes_reusable_and_valid_retry_recomputes(self):
  import pandas as pd
  from nv_ingest_durable.caption_worker import validate_caption_inference
  from application.image_caption_cache import ImageCaptionCache
  cache=ImageCaptionCache(self.pool,self.root);calls=0
  async def compute():
   nonlocal calls;calls+=1
   vector=[0.0]*2048 if calls==1 else [1.0]+[0.0]*2047
   return validate_caption_inference(pd.DataFrame([{'text':'caption','metadata':{'embedding':vector}}]))
  with self.assertRaisesRegex(ValueError,'unit norm'):await cache.get_or_compute('a','a'*64,'b'*64,compute,document_id='bad')
  self.assertEqual(await self.pool.fetchval("SELECT count(*) FROM application.image_caption_cache WHERE status='ready'"),0)
  _,proof=await cache.get_or_compute('a','a'*64,'b'*64,compute,document_id='good')
  self.assertFalse(proof['cache_hit']);self.assertEqual(calls,2)

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'isolated PostgreSQL required')
class ImageCacheLeaseTests(unittest.IsolatedAsyncioTestCase):
 asyncSetUp=ImageCaptionCacheTests.asyncSetUp
 async def test_renewed_lease_prevents_duplicate_work(self):
  import pandas as pd
  from application.image_caption_cache import ImageCaptionCache
  cache=ImageCaptionCache(self.pool,self.root,lease_seconds=.09,poll_seconds=.01);calls=0
  async def compute():
   nonlocal calls;calls+=1;await asyncio.sleep(.35);return pd.DataFrame([{'text':'complete'}])
  first=asyncio.create_task(cache.get_or_compute('a','a'*64,'b'*64,compute,document_id='1'))
  await asyncio.sleep(.15)
  await asyncio.gather(first,cache.get_or_compute('a','a'*64,'b'*64,compute,document_id='2'))
  self.assertEqual(calls,1)
 async def test_stale_owner_cannot_publish_and_wait_is_bounded(self):
  import pandas as pd
  from application.image_caption_cache import ImageCaptionCache
  cache=ImageCaptionCache(self.pool,self.root,poll_seconds=.01,wait_seconds=.05)
  entered=asyncio.Event();release=asyncio.Event()
  async def compute():entered.set();await release.wait();return pd.DataFrame([{'text':'complete'}])
  task=asyncio.create_task(cache.get_or_compute('a','a'*64,'b'*64,compute,document_id='old'))
  await entered.wait()
  with self.assertRaises(TimeoutError):await cache.get_or_compute('a','a'*64,'b'*64,compute,document_id='waiter')
  await self.pool.execute('UPDATE application.image_caption_cache SET owner=$1,fence=fence+1',uuid4())
  release.set()
  with self.assertRaisesRegex(RuntimeError,'fenced|lease lost'):await task
  self.assertEqual(await self.pool.fetchval("SELECT count(*) FROM application.image_caption_cache WHERE status='ready'"),0)
