import copy
from tests.application.test_typed_provenance import TypedProvenanceTests
from tests.test_html_disclosures import RAW
from ingestion.document_formats import inspect_document
from collection_control.repository import Conflict

class DisclosureProvenanceTests(TypedProvenanceTests):
 def setUp(self):
  super().setUp()
  self.raw.write_bytes(RAW)
  import hashlib
  doc=inspect_document('page.html',RAW,html_version='3')
  self.units={'schema_version':'native-source-units.v1',**{k:doc[k] for k in ('source_kind','transform','units')}}
  self.revision.update(source_kind='html',filename='page.html',raw_sha256=hashlib.sha256(RAW).hexdigest(),source_units=self.units)
  self.checkpoint.update(native_source_units=self.units,documents=[{'text':u['text'],'vector':[1.0]+[0.0]*2047,'metadata':{'content_metadata':{'source_unit_id':u['unit_id']}}} for u in self.units['units']])

 def test_disclosure_variants_have_exact_parent_and_coverage(self):
  enriched,manifest=self.enrich();records,_,_=self.validate(enriched,manifest)
  self.assertEqual(len(records),4)
  legal=[r for r in records if r['metadata'].get('content_role')=='legal_disclosure']
  self.assertEqual(len(legal),3)
  for r in legal:
   self.assertEqual(r['metadata']['parent_page_id'],'source')
   self.assertEqual(r['metadata']['parent_page_revision_id'],'revision')
   self.assertEqual(r['metadata']['variant_id'],r['metadata']['provenance']['anchors']['variant_id'])
  self.assertEqual(sum(len(x) for x in enriched['coverage']['completed_units'].values()),4)
  broken=copy.deepcopy(enriched);broken['documents'].pop()
  with self.assertRaises(Conflict):self.validate(broken,manifest)

 def test_legal_metadata_cannot_relabel_audience_or_parent(self):
  for key in ['parent_page_id','variant_id','content_role']:
   enriched,manifest=self.enrich();enriched['documents'][1]['metadata'][key]='forged'
   with self.assertRaises(Conflict):self.validate(enriched,manifest)

# Inherit fixture helpers, not unrelated tests whose text-only premises differ.
for name in list(vars(TypedProvenanceTests)):
 if name.startswith('test_'):setattr(DisclosureProvenanceTests,name,None)
