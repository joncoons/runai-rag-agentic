import unittest
from application.html_disclosures import html_units

class CharacterBudget:
 identity='fixture-budget'
 def __init__(self,limit=10000):self.limit=limit
 def fits(self,text):return len(text)<=self.limit

class LegalGroupingV4Tests(unittest.TestCase):
 def units(self,raw,limit=10000):return html_units(raw,budget=CharacterBudget(limit))
 def test_complete_audience_group_stays_together(self):
  u=self.units(b'<main>Facts<section class="disclaimer"><div class="disclaimer-student"><div id="legal-1">Enrollment required.</div><div id="legal-2">Minimum age 18.</div></div></section></main>')
  self.assertEqual(len(u),2);self.assertEqual(u[1]['text'],'Enrollment required.\nMinimum age 18.')
  self.assertEqual(u[1]['anchors']['occurrences'][0]['variant_id'],'disclaimer-student')
 def test_ui_controls_are_excluded_not_legal_evidence(self):
  u=self.units(b'<main>Facts<section class="disclaimer"><button aria-controls="terms">View Legal Disclaimers<span>Hide Legal Disclaimers</span></button><div id="legal-1">Actual condition.</div></section></main>')
  self.assertEqual(len(u),2);self.assertEqual(u[1]['text'],'Actual condition.')
 def test_control_only_section_yields_no_vector(self):
  self.assertEqual(len(self.units(b'<main>Facts<section class="disclaimer"><button>View Legal Disclaimers</button></section></main>')),1)
 def test_equal_group_text_keeps_both_occurrences(self):
  u=self.units(b'<main>Facts<section class="disclaimer"><div class="disclaimer-a">Fee $10.</div><div class="disclaimer-b">Fee $10.</div></section></main>')
  self.assertEqual(len(u),2);self.assertEqual(len(u[1]['anchors']['occurrences']),2)
  self.assertEqual({a['variant_id'] for a in u[1]['anchors']['occurrences']},{'disclaimer-a','disclaimer-b'})
 def test_different_amounts_or_negation_never_merge(self):
  u=self.units(b'<main>Facts<section class="disclaimer"><div class="disclaimer-a">Fee $10.</div><div class="disclaimer-b">Fee $11.</div><div class="disclaimer-c">No fee $10.</div></section></main>')
  self.assertEqual(len(u),4)
 def test_only_oversized_group_is_packed_at_clause_boundaries(self):
  raw=b'<main>Facts<section class="disclaimer"><div id="legal-1">First condition.</div><div id="legal-2">Second condition.</div><div id="legal-3">Third condition.</div></section></main>'
  self.assertEqual(len(self.units(raw)),2)
  u=self.units(raw,35);self.assertEqual(len(u),3)
  self.assertEqual('\n'.join(x['text'] for x in u[1:]),'First condition.\nSecond condition.\nThird condition.')
 def test_indivisible_oversized_clause_fails_without_truncation(self):
  with self.assertRaises(ValueError):self.units(b'<main>Facts<section class="disclaimer"><div id="legal-1">An indivisible long condition.</div></section></main>',10)
 def test_shared_condition_is_retained_and_associated(self):
  u=self.units(b'<main>Facts<section class="disclaimer"><p>All products require residency.</p><div class="disclaimer-a">Fee $10.</div><div class="disclaimer-b">Fee $11.</div></section></main>')
  self.assertEqual(len(u),4);self.assertIn('All products require residency.',[x['text'] for x in u])
  groups={a['group_id'] for x in u[1:] for a in x['anchors']['occurrences']}
  self.assertEqual(len(groups),3)
 def test_nested_legal_content_and_tables_are_not_lost(self):
  u=self.units(b'<main>Facts<section id="legal-disclaimers"><div id="legal-wrapper">Common<div id="legal-1">First</div><div id="legal-2"><table><tr><th>Fee</th><td>$10</td></tr></table>Second</div></div></section></main>')
  self.assertEqual(len(u),2)
  for t in ('Common','First','Fee','$10','Second'):self.assertIn(t,u[1]['text'])
 def test_nested_audience_hierarchy_is_preserved(self):
  u=self.units(b'<main>Facts<section class="disclaimer"><div class="disclaimer-student">Students must enroll.<div class="disclaimer-canadian">Canadian terms.</div><div class="disclaimer-international">International terms.</div></div></section></main>')
  occurrences=[o for x in u[1:] for o in x['anchors']['occurrences']]
  self.assertIn(['disclaimer-student','disclaimer-international'],[o['audience_path'] for o in occurrences])
  self.assertEqual(sum(x['text'].count('Students must enroll.') for x in u[1:]),1)
 def test_root_and_non_div_audiences_are_preserved(self):
  for raw in (b'<main>Facts<section id="legal-disclaimers" class="disclaimer-student">Student terms.</section></main>', b'<main>Facts<section class="disclaimer"><ul class="disclaimer-student"><li>Student terms.</li></ul></section></main>'):
   u=self.units(raw);self.assertEqual(u[1]['anchors']['occurrences'][0]['variant_id'],'disclaimer-student')
 def test_table_caption_and_footer_qualifications_are_not_lost(self):
  u=self.units(b'<main>Facts<section class="disclaimer"><table><caption>Rates do not apply to international students.</caption><tr><th>Fee</th><td>$10</td></tr><tfoot>Exceptions apply to residents.</tfoot></table></section></main>')
  self.assertIn('Rates do not apply to international students.',u[1]['text']);self.assertIn('Exceptions apply to residents.',u[1]['text'])
 def test_root_toggle_is_excluded(self):
  self.assertEqual(len(self.units(b'<main>Facts<button id="legal-toggle">View Legal Disclaimers</button></main>')),1)
