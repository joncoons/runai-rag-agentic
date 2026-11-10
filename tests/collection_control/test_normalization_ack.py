import json,os,unittest
from pathlib import Path
from unittest.mock import patch
import asyncpg

@unittest.skipUnless(os.environ.get('COLLECTION_TEST_DSN'),'requires disposable qualification PostgreSQL')
class NormalizationAckTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.c=await asyncpg.connect(os.environ['COLLECTION_TEST_DSN']);self.addAsyncCleanup(self.c.close)
  who=await self.c.fetchrow('SELECT current_database() db,current_user usr');self.assertEqual((who['db'],who['usr']),('qualification','qualification'))
  await self.c.execute(Path('deployment/collection-control/007_html_normalization.sql').read_text())
  await self.c.execute('TRUNCATE collection_control.html_normalizations')
 def native(self,raw):
  from ingestion.document_formats import inspect_document
  result=inspect_document('captured-page.html',raw)
  return {'schema_version':'native-source-units.v1',**{k:result[k] for k in ('source_kind','transform','units')}}
 async def test_ack_skips_only_repeat_transform_and_rejects_changed_native_units(self):
  from collection_control.html_normalization import verify_normalization
  from collection_control.repository import Conflict
  raw=b'<main>Unique immutable normalization proof</main>';native=self.native(raw)
  await verify_normalization(self.c,raw,native)
  with patch('collection_control.html_normalization.inspect_retained_html',side_effect=AssertionError('Repeated parser work')):
   await verify_normalization(self.c,raw,native)
   bad=json.loads(json.dumps(native));bad['units'][0]['text']='unrelated content'
   with self.assertRaises(Conflict):await verify_normalization(self.c,raw,bad)
  self.assertEqual(await self.c.fetchval('SELECT count(*) FROM collection_control.html_normalizations'),1)
 async def test_changed_raw_or_normalizer_requires_fresh_validation(self):
  from collection_control.html_normalization import verify_normalization
  from collection_control.repository import Conflict
  raw=b'<main>First bytes</main>';native=self.native(raw);await verify_normalization(self.c,raw,native)
  with self.assertRaises(Conflict):await verify_normalization(self.c,b'<main>Changed bytes</main>',native)
  with patch('collection_control.html_normalization.normalizer_digest',return_value='f'*64),patch('collection_control.html_normalization.inspect_retained_html',side_effect=RuntimeError('Fresh validation required')):
   with self.assertRaises(RuntimeError):await verify_normalization(self.c,raw,native)

 async def test_parser_dependency_identity_changes_digest(self):
  from collection_control.html_normalization import normalizer_digest
  normalizer_digest.cache_clear();before=normalizer_digest()
  try:
   with patch('collection_control.html_normalization.parser_identity',return_value=('changed-runtime','changed-bs4','changed-soupsieve')):
    normalizer_digest.cache_clear();self.assertNotEqual(before,normalizer_digest())
  finally:normalizer_digest.cache_clear()
