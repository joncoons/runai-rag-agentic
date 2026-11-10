import copy,hashlib
from tests.application.test_typed_provenance import TypedProvenanceTests
from ingestion.document_formats import inspect_document
from collection_control.repository import Conflict

class LegalGroupProvenanceTests(TypedProvenanceTests):
 def setUp(self):
  super().setUp()
  raw=b'<main>Product<section class="disclaimer"><div class="disclaimer-a">Fee $10.</div><div class="disclaimer-b">Fee $10.</div></section></main>'
  self.raw.write_bytes(raw);d=inspect_document('page.html',raw,html_version='4')
  self.units={'schema_version':'native-source-units.v1',**{k:d[k] for k in ('source_kind','transform','units')}}
  self.revision.update(source_kind='html',filename='page.html',raw_sha256=hashlib.sha256(raw).hexdigest(),source_units=self.units)
  self.checkpoint.update(native_source_units=self.units,documents=[{'text':u['text'],'vector':[1.0]+[0.0]*2047,'metadata':{'content_metadata':{'source_unit_id':u['unit_id']}}} for u in self.units['units']])
 def test_shared_unit_preserves_both_audiences_and_complete_proof(self):
  enriched,manifest=self.enrich();records,_,_=self.validate(enriched,manifest)
  self.assertEqual(len(records),2)
  legal=next(r for r in records if r['metadata'].get('content_role')=='legal_disclosure')
  self.assertEqual(legal['metadata']['audiences'],['disclaimer-a','disclaimer-b'])
  self.assertEqual(len(legal['metadata']['legal_occurrences']),2)
  self.assertEqual(legal['metadata']['legal_content_sha256'],hashlib.sha256(b'Fee $10.').hexdigest())
 def test_legal_occurrences_cannot_be_forged_or_dropped(self):
  enriched,manifest=self.enrich();broken=copy.deepcopy(enriched)
  broken['documents'][1]['metadata']['legal_occurrences']=[]
  with self.assertRaises(Conflict):self.validate(broken,manifest)
for name in list(vars(TypedProvenanceTests)):
 if name.startswith('test_'):setattr(LegalGroupProvenanceTests,name,None)
