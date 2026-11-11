import hashlib,os,unittest
from pathlib import Path
from application.legal_embedding_cache import LegalEmbeddingCache

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'isolated PostgreSQL required')
class LegalEmbeddingCacheTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  import asyncpg
  dsn=os.environ['COLLECTION_TEST_DSN']
  from urllib.parse import urlsplit
  if urlsplit(dsn).hostname not in ('127.0.0.1','localhost'):raise RuntimeError('Loopback qualification DB only')
  self.pool=await asyncpg.create_pool(dsn,min_size=1,max_size=2)
  if tuple(await self.pool.fetchrow('SELECT current_database(),current_user'))!=('qualification','qualification'):
   await self.pool.close();raise RuntimeError('Isolated qualification DB identity required')
  await self.pool.execute(Path('deployment/application/010_legal_content_reuse.sql').read_text())
  await self.pool.execute('TRUNCATE application.legal_text_embeddings,application.legal_text_content')
  self.cache=LegalEmbeddingCache(self.pool);self.text='The fee is $10. No exceptions.';self.sha=hashlib.sha256(self.text.encode()).hexdigest();self.key='a'*64
 async def asyncTearDown(self):await self.pool.close()
 async def test_exact_content_and_contract_hit(self):
  vector=[1.0]+[0.0]*2047
  await self.cache.put(self.key,self.text,vector)
  self.assertEqual(await self.cache.get(self.key,{self.sha:self.text}),{self.sha:vector})
  self.assertEqual(await self.cache.get('b'*64,{self.sha:self.text}),{})
 async def test_wording_change_is_different_content(self):
  await self.cache.put(self.key,self.text,[1.0]+[0.0]*2047)
  changed='The fee is $11. No exceptions.';sha=hashlib.sha256(changed.encode()).hexdigest()
  self.assertEqual(await self.cache.get(self.key,{sha:changed}),{})
 async def test_hash_match_must_also_match_actual_text(self):
  await self.cache.put(self.key,self.text,[1.0]+[0.0]*2047)
  await self.pool.execute('UPDATE application.legal_text_content SET text=$1 WHERE content_sha256=$2','Corrupted condition',self.sha)
  with self.assertRaises(ValueError):await self.cache.get(self.key,{self.sha:self.text})
 async def test_bad_vectors_are_rejected(self):
  for vector in ([1.0], [float('nan')]+[0.0]*2047,[0.0]*2048):
   with self.assertRaises(ValueError):await self.cache.put(self.key,self.text,vector)
 async def test_extraction_reuses_only_vector_with_new_source_metadata(self):
  from types import SimpleNamespace
  import pandas as pd
  from application.typed_extraction import TypedExtractionProcessor
  from uuid import uuid4
  calls=[]
  class Embedding:
   def process(self,frame):
    calls.append(len(frame));frame=frame.copy()
    for index in frame.index:frame.at[index,'metadata']={**frame.at[index,'metadata'],'embedding':[1.0]+[0.0]*2047}
    return frame
  processor=TypedExtractionProcessor(self.pool,None,root='/tmp',embedding=Embedding())
  units={'source_kind':'html','transform':{'version':'4'},'units':[{'unit_id':'page:1','status':'complete','text':'Main page','anchors':{'section':'page'}},{'unit_id':'legal:'+self.sha,'status':'complete','text':self.text,'anchors':{'section':'legal_disclosure','legal_content_sha256':self.sha,'input_contract_sha256':'f'*64}}]}
  def env(uri,config='b'*64):return SimpleNamespace(artifact_uri=uri,model_digest='sha256:'+'c'*64,config_digest='sha256:'+config)
  first=await processor._embed_with_reuse(units,env('file:///source-one.html'))
  second=await processor._embed_with_reuse(units,env('file:///source-two.html'))
  self.assertEqual(calls,[2,1]);legal=second.to_dict(orient='records')[1]
  self.assertEqual(legal['metadata']['source']['source_uri'],'file:///source-two.html')
  self.assertTrue(legal['metadata']['legal_embedding_reuse']['cache_hit'])
  await processor._embed_with_reuse(units,env('file:///source-three.html','d'*64))
  self.assertEqual(calls,[2,1,2])
