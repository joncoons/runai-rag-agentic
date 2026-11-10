import unittest
from ingestion.document_formats import inspect_document as _inspect_document

def inspect_document(filename,raw,**kwargs):
 return _inspect_document(filename,raw,**({"html_version":"2"}|kwargs))

RAW=b'''<html><main><h1>Accounts</h1><p>Monthly fee conditions apply <a href="#student-note">1</a>.</p><section id="disclaimer" class="disclaimer"><h2>Legal disclosures</h2><div class="disclaimer-student"><div id="student-note">Student account requires current enrollment.</div><span>Shared eligibility wording.</span></div><div class="disclaimer-new-to-canada"><div>Newcomer account requires residency evidence.</div><span>Shared eligibility wording.</span></div></section></main></html>'''

class DisclosureTests(unittest.TestCase):
 def test_variants_are_distinct_complete_source_units(self):
  doc=inspect_document('page.html',RAW);units=doc['units']
  self.assertEqual(len(units),3)
  self.assertEqual(units[0]['unit_id'],'page:1')
  self.assertNotIn('current enrollment',units[0]['text'])
  self.assertIn('Monthly fee',units[0]['text'])
  self.assertEqual({u['anchors']['variant_id'] for u in units[1:]},{'disclaimer-student','disclaimer-new-to-canada'})
  self.assertTrue(all('Shared eligibility wording.' in u['text'] for u in units[1:]))
  self.assertTrue(all(u['anchors']['section']=='legal_disclosure' for u in units[1:]))
  self.assertNotIn('residency evidence',units[1]['text'])
  self.assertIn('current enrollment',units[1]['text'])
  self.assertEqual(doc['transform']['version'],'2')

 def test_legacy_is_reproducible_and_preserves_old_wholepage(self):
  doc=inspect_document('page.html',RAW,html_version='1')
  self.assertEqual(len(doc['units']),1)
  self.assertIn('current enrollment',doc['units'][0]['text'])
  self.assertEqual(doc['transform']['version'],'1')

 def test_no_legal_marker_preserves_ordinary_hidden_and_inline_terms(self):
  doc=inspect_document('page.html',b'<main><p>Terms apply.</p><div hidden>Meaningful variant</div></main>')
  self.assertEqual(len(doc['units']),1)
  self.assertIn('Meaningful variant',doc['units'][0]['text'])

 def test_common_section_content_is_retained_for_each_audience(self):
  raw=RAW.replace(b'<h2>Legal disclosures</h2>',b'<h2>Legal disclosures</h2><div>Common applicable condition.</div>')
  units=inspect_document('page.html',raw)['units']
  self.assertTrue(all('Common applicable condition.' in u['text'] for u in units[1:]))

 def test_single_disclosure_and_duplicate_dom_ids_are_not_lost(self):
  raw=b'<main>Main<section id="disclaimer">All conditions</section><section id="disclaimer">Additional terms</section></main>'
  units=inspect_document('page.html',raw)['units']
  self.assertEqual(len(units),3)
  self.assertEqual(len({u['unit_id'] for u in units}),3)
  self.assertIn('All conditions',units[1]['text']);self.assertIn('Additional terms',units[2]['text'])

 def test_disclaimer_container_class_does_not_become_an_audience(self):
  raw=b'<main>Product<section id="disclaimer" class="disclaimer-section"><div class="disclaimer-student">student terms</div><div class="disclaimer-new">new terms</div></section></main>'
  units=inspect_document('page.html',raw)['units']
  self.assertEqual(len(units),3)
  self.assertIn('student terms',units[1]['text']);self.assertNotIn('new terms',units[1]['text'])
  self.assertIn('new terms',units[2]['text']);self.assertNotIn('student terms',units[2]['text'])

 def test_all_legal_articles_and_footer_conditions_are_preserved(self):
  raw=b'<main>Product<section id="disclaimer"><article>First required condition</article><article>Second required condition</article><footer>Effective-date condition</footer></section></main>'
  units=inspect_document('page.html',raw)['units']
  self.assertEqual(len(units),2)
  self.assertTrue(all(t in units[1]['text'] for t in ['First required condition','Second required condition','Effective-date condition']))

 def test_empty_disclosure_has_explicit_complete_zero_text_unit(self):
  units=inspect_document('page.html',b'<main>Product<section id="disclaimer"></section></main>')['units']
  self.assertEqual(len(units),2);self.assertEqual(units[1]['text'],'');self.assertEqual(units[1]['status'],'complete')
