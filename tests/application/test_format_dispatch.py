"""Typed intake preserves source identity and explicit caption capabilities."""
import unittest
from application.intake import inspect_upload

class TypedIntakeTests(unittest.TestCase):
    def test_text_keeps_typed_units_without_pdf_pages(self):
        result=inspect_upload('notes.txt',b'first\nlast\n',captions_enabled=False)
        self.assertEqual(result['source_kind'],'txt')
        self.assertIsNone(result['page_count'])
        self.assertEqual(result['source_units']['schema_version'],'native-source-units.v1')
        self.assertIn('last',''.join(x.get('text','') for x in result['source_units']['units']))
    def test_structural_adapter_does_not_claim_image_caption_parity(self):
        with self.assertRaisesRegex(ValueError,'caption'):
            inspect_upload('notes.md',b'content',captions_enabled=True)
    def test_unknown_extension_rejected(self):
        with self.assertRaises(ValueError):inspect_upload('bad.exe',b'bad',captions_enabled=False)
