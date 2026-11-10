import unittest
from unittest.mock import AsyncMock
from types import SimpleNamespace
from application.query import RAGQuery
from application.legal_evidence import legal_role
from collection_control.artifacts import record_digest

class LegalEvidenceTests(unittest.IsolatedAsyncioTestCase):
 def test_document_role_requires_explicit_document_not_html_product(self):
  def record(uri,role=None):return {'metadata':{'content_role':role,'provenance':{'canonical_uri':uri}}}
  self.assertEqual(legal_role(record('https://a/credit-card-agreement.pdf')),'legal_document')
  self.assertEqual(legal_role(record('https://a/certificate-of-insurance.pdf')),'legal_document')
  self.assertEqual(legal_role(record('https://a/travel-insurance.html')),'primary')
  self.assertEqual(legal_role(record('https://a/product.html','legal_disclosure')),'legal_disclosure')
 async def test_legal_hashes_are_excluded_before_backend_top_k(self):
  query=object.__new__(RAGQuery);query.pool=SimpleNamespace(fetch=AsyncMock(return_value=[{'record_sha256':'main'},{'record_sha256':'legal'}]))
  query._legal_hashes=AsyncMock(return_value={'legal'})
  fetch=AsyncMock(return_value=[])
  await query._published_fetch(fetch,{'handles':{'elasticsearch':'rag_agentic_inc_'+'a'*32+'_es'},'operation_id':'op','versions':{'main','legal'},'legal_versions':{'legal'}},'elasticsearch',[1],channel='primary')
  self.assertEqual(fetch.await_args.kwargs['versions'],['main'])
 async def test_llm_legal_decision_is_strict_bool(self):
  query=object.__new__(RAGQuery);query._chat=AsyncMock(return_value='{"include_legal":true}')
  self.assertTrue(await query._needs_legal('What exclusions apply?'))
  query._chat.return_value='{"include_legal":false}'
  self.assertFalse(await query._needs_legal('Compare the cards'))

 async def test_invalid_llm_decision_is_rejected(self):
  from collection_control.repository import Conflict
  query=object.__new__(RAGQuery);query._chat=AsyncMock(return_value='{"include_legal":"yes"}')
  with self.assertRaises(Conflict):await query._needs_legal('Question')

 async def test_legal_channel_selects_only_legal_versions(self):
  query=object.__new__(RAGQuery);query.pool=SimpleNamespace(fetch=AsyncMock(return_value=[{'record_sha256':'main'},{'record_sha256':'legal'}]))
  query._legal_hashes=AsyncMock(return_value={'legal'})
  fetch=AsyncMock(return_value=[])
  await query._published_fetch(fetch,{'handles':{'milvus':'rag_agentic_native_'+'a'*32+'_mv'},'operation_id':'op','versions':{'main','legal'},'legal_versions':{'legal'}},'milvus',[1],channel='legal')
  self.assertEqual(fetch.await_args.kwargs['versions'],['legal'])
