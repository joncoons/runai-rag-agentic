import unittest
from ingestion.document_formats import inspect_document

class LegalSubtextV3Tests(unittest.TestCase):
 def inspect(self,raw):return inspect_document('page.html',raw,html_version='3')
 def test_anchored_clauses_are_distinct_with_complete_shared_prose(self):
  raw=b'<main>Product facts<section class="disclaimer"><h2>Legal conditions</h2><p>All clauses are subject to this common condition.</p><div id="legal-1">First clause</div><div id="legal-2">Second clause</div></section></main>'
  d=self.inspect(raw);units=d['units'];self.assertEqual(d['transform']['version'],'3')
  self.assertEqual(len(units),4);self.assertEqual(units[0]['text'],'Product facts')
  texts=[u['text'] for u in units[1:]]
  self.assertEqual(sum('First clause' in t for t in texts),1);self.assertEqual(sum('Second clause' in t for t in texts),1)
  self.assertEqual(sum('common condition' in t for t in texts),1)
  self.assertEqual({u['anchors'].get('clause_id') for u in units[1:]},{None,'legal-1','legal-2'})
 def test_audience_is_retained_on_each_clause(self):
  d=self.inspect(b'<main>Facts<section id="disclaimer"><div class="disclaimer-student"><div id="legal-a">Student clause A</div><div id="legal-b">Student clause B</div></div><div class="disclaimer-new"><div id="legal-c">Newcomer clause</div></div></section></main>')
  legal=d['units'][1:];self.assertEqual(len(legal),3)
  self.assertEqual([u['anchors']['variant_id'] for u in legal],['disclaimer-student','disclaimer-student','disclaimer-new'])
 def test_standalone_legal_anchors_are_removed_from_main(self):
  d=self.inspect(b'<main><p>Product facts</p><div id="legal-1">A condition</div><div id="legal-2">Another condition</div></main>')
  self.assertEqual(d['units'][0]['text'],'Product facts');self.assertEqual(len(d['units']),3)
 def test_main_metadata_is_compact_with_many_clauses(self):
  raw=('<main>Product<section class="disclaimer">'+''.join(f'<div id="legal-{i}">Condition{i}</div>' for i in range(200))+'</section></main>').encode()
  units=self.inspect(raw)['units'];self.assertEqual(len(units),201)
  import json
  self.assertLess(len(json.dumps(units[0]['anchors'])),2000)
 def test_legacy_v2_is_unchanged(self):
  raw=b'<main>Product<section class="disclaimer"><div id="legal-1">A</div><div id="legal-2">B</div></section></main>'
  self.assertEqual(len(inspect_document('page.html',raw,html_version='2')['units']),2)

 def test_repeated_audience_groups_have_distinct_unit_identities(self):
  units=self.inspect(b'<main>Facts<section class="disclaimer"><div class="disclaimer-student"><div id="legal-a">First scope</div></div><div class="disclaimer-student"><div id="legal-a">Second scope</div></div></section></main>')['units']
  self.assertEqual(len(units),3);self.assertEqual(len({u['unit_id'] for u in units}),3)

 def test_root_container_class_is_not_an_audience(self):
  units=self.inspect(b'<main>Product<section id="disclaimer" class="disclaimer-section"><div class="disclaimer-student">student terms</div><div class="disclaimer-new">new terms</div></section></main>')['units']
  self.assertEqual(len(units),3);self.assertEqual(units[1]['text'],'student terms');self.assertEqual(units[2]['text'],'new terms')

 def test_legal_disclaimers_container_is_not_itself_a_clause(self):
  units=self.inspect(b'<main>Facts<section id="legal-disclaimers"><div id="legal-1">First</div><div id="legal-2">Second</div></section></main>')['units']
  self.assertEqual(len(units),3);self.assertEqual({u['anchors'].get('clause_id') for u in units[1:]},{'legal-1','legal-2'})

 def test_empty_anchor_uses_its_containing_or_immediate_paragraph(self):
  for raw in (b'<main>Facts<section class="disclaimer"><a id="legal-1"></a><p>First condition</p><p><a id="legal-2"></a>Second condition</p></section></main>',b'<main>Facts<a id="legal-1"></a><p>First condition</p><p>Product benefit</p></main>'):
   units=self.inspect(raw)['units']
   self.assertNotIn('First condition',units[0]['text']);self.assertEqual(units[1]['text'],'First condition')
   if b'Second' in raw:self.assertEqual(units[2]['text'],'Second condition')
   else:self.assertIn('Product benefit',units[0]['text'])

 def test_standalone_clause_inventory_is_compact(self):
  import json
  units=self.inspect(('<main>Facts'+''.join(f'<div id="legal-{i}">Clause{i}</div>' for i in range(200))+'</main>').encode())['units']
  self.assertEqual(len(units),201);self.assertLess(len(json.dumps(units[0]['anchors'])),2000)

 def test_nested_legal_wrapper_preserves_prose_and_leaf_clauses(self):
  units=self.inspect(b'<main>Facts<section class="disclaimer"><div id="legal-wrapper">Common<div id="legal-1">First</div><div id="legal-2">Second</div></div></section></main>')['units']
  self.assertEqual(len(units),4);self.assertEqual([u['text'] for u in units[1:]],['First','Second','Common'])
