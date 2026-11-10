import copy,unittest
from types import SimpleNamespace
from unittest.mock import patch
from tests.collection_control.test_writer import fixture
from collection_control.artifacts import record_digest

class NativeFieldTests(unittest.TestCase):
 def test_native_schema_and_columns_match_original_crawler_field_separation(self):
  from collection_control.native_milvus import schema_fields,columns,restore
  from pymilvus import DataType
  fields={f.name:f for f in schema_fields()}
  self.assertEqual(set(fields),{'version_id','record_id','vector','text','metadata'})
  self.assertEqual(fields['text'].dtype,DataType.TEXT);self.assertNotIn('max_length',fields['text'].params)
  self.assertEqual(fields['metadata'].dtype,DataType.JSON)
  record=fixture()['records'][0];record['text']='Complete original page é\n'*100000
  data=columns([record]);self.assertEqual(data[3],[record['text']]);self.assertEqual(data[4],[record['metadata']])
  self.assertEqual(restore(dict(zip(['version_id','record_id','vector','text','metadata'],[a[0] for a in data]))),record)
 def test_new_handles_are_native_and_old_incremental_handles_remain_readable(self):
  from collection_control.incremental import version_handle,incremental_handle
  from collection_control.native_milvus import native_handle
  from collection_control.stores import require_handle
  h=version_handle('collection','milvus');self.assertTrue(native_handle(h));self.assertTrue(incremental_handle(h));require_handle(h,'mv')
  self.assertTrue(incremental_handle('rag_agentic_inc_'+'a'*32+'_mv'))
 def test_query_reads_native_fields_without_payload_or_file_hydration(self):
  from collection_control.incremental import version_handle
  from application.query import RAGQuery
  record=fixture()['records'][0];record['text']='Original complete text é\n'*10000
  client=SimpleNamespace(list_indexes=lambda *a,**kw:['vector_index'],describe_index=lambda *a,**kw:{'index_type':'GPU_CAGRA','metric_type':'IP'},close=lambda:None)
  def search(*args,**kwargs):
   self.assertIn('text',kwargs['output_fields']);self.assertIn('metadata',kwargs['output_fields']);self.assertNotIn('payload',kwargs['output_fields'])
   return [[{'entity':copy.deepcopy(record),'distance':.9}]]
  client.search=search
  q=object.__new__(RAGQuery);q.settings=SimpleNamespace(milvus_uri='http://unused',milvus_token=None)
  with patch('pymilvus.MilvusClient',return_value=client),patch('collection_control.record_payloads.decode_payload',side_effect=AssertionError('Native reads must not hydrate payload files')):
   rows=q._milvus_sync(version_handle('collection','milvus'),record['vector'],None,['a'*64])
  self.assertEqual(rows[0]['text'],record['text']);self.assertEqual(rows[0]['metadata'],record['metadata'])

 def test_active_writer_refuses_legacy_json_handles(self):
  import asyncio
  from collection_control.incremental import IncrementalMilvus
  from collection_control.repository import Conflict
  store=IncrementalMilvus('http://unused')
  with self.assertRaises(Conflict):asyncio.run(store.write_verify('rag_agentic_inc_'+'a'*32+'_mv',fixture()))
