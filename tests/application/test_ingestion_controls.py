import unittest
from application.api import WebIngestion
class IngestionControls(unittest.TestCase):
 def test_web_caption_optin_cannot_expand_image_acquisition_scope(self):
  with self.assertRaises(ValueError):WebIngestion(seed_urls=['https://example.com/'],allowed_hosts=['example.com'],captions_enabled=True,include_images=False)
  enabled=WebIngestion(seed_urls=['https://example.com/'],allowed_hosts=['example.com'],captions_enabled=True,include_images=True);self.assertTrue(enabled.captions_enabled)
