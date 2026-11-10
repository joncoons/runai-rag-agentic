import unittest
from unittest.mock import patch

class HtmlProofCacheTests(unittest.TestCase):
 def test_identical_verified_html_reuses_pure_transform_without_sharing_mutable_results(self):
  import ingestion.document_formats as formats
  raw=b'<main>Unique pure retained proof cache qualification 20261109</main>'
  original=formats.inspect_document
  with patch.object(formats,'inspect_document',wraps=original) as inspect:
   first=formats.inspect_retained_html(raw);expected=first['units'][0]['text'];first['units'][0]['text']='caller mutation'
   second=formats.inspect_retained_html(raw)
   self.assertEqual(second['units'][0]['text'],expected);self.assertEqual(inspect.call_count,1)
   formats.inspect_retained_html(raw+b'<p>Changed original bytes</p>');self.assertEqual(inspect.call_count,2)
