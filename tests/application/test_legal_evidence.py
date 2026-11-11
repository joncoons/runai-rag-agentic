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
 async def test_reading_published_legal_roles_does_not_reapply_new_admission_policy(self):
  import hashlib,tempfile
  from pathlib import Path
  from application.legal_evidence import snapshot_legal_hashes
  from collection_control.artifacts import canonical,load_artifact
  from collection_control.repository import Conflict
  record={'record_id':'historically-published','text':'Retained legal terms.','vector':[1.0]+[0.0]*2047,'metadata':{'origin':'derived','source_id':'historical-source','source_revision_id':'historical-revision','content_role':'legal_disclosure','web_enrichment_derivation':{'policy':'retained-web-enrichment.v1'}}}
  artifact={'contract':{'dimensions':2048,'dtype':'float32','normalization':'unit','metric':'IP','schema_version':1,'embedding_checkpoint_sha256':'a'*64},'records':[record]}
  raw=canonical(artifact);sha=hashlib.sha256(raw).hexdigest()
  with tempfile.TemporaryDirectory() as folder:
   root=Path(folder);path=root/(sha+'.json');path.write_bytes(raw)
   with self.assertRaisesRegex(Conflict,'Derived web page'):load_artifact(root,sha)
   self.assertEqual(snapshot_legal_hashes(root,sha),frozenset([record_digest(record)]))
   snapshot_legal_hashes.cache_clear();path.write_bytes(raw+b' ')
   with self.assertRaisesRegex(Conflict,'SHA-256 mismatch'):snapshot_legal_hashes(root,sha)
 async def test_background_legal_classification_streams_snapshot_records(self):
  import hashlib,tempfile
  from pathlib import Path
  from unittest.mock import patch
  import application.legal_evidence as module
  from collection_control.artifacts import canonical
  records=[{'record_id':'primary','text':'Literal brace } and café 😀','vector':[1.0]+[0.0]*2047,'metadata':{'content_role':'primary'}},{'record_id':'legal','text':'Terms \\"quoted\\" café 😀','vector':[1.0]+[0.0]*2047,'metadata':{'content_role':'legal_disclosure'}}]
  raw=canonical({'contract':{'dimensions':2048},'records':records});sha=hashlib.sha256(raw).hexdigest()
  with tempfile.TemporaryDirectory() as folder:
   root=Path(folder);(root/(sha+'.json')).write_bytes(raw)
   with patch.object(module,'_CHUNK_BYTES',23):
    self.assertEqual(module.snapshot_legal_hashes(root,sha),frozenset([record_digest(records[1])]))
