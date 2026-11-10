"""Use the packaged native PDF library, not a mocked upload parser."""
import io
import unittest
from application.intake import pdf_page_count

class PDFIntakeTests(unittest.TestCase):
    def test_actual_native_pdf_bytes_counted_and_closed(self):
        import pypdfium2 as pdfium
        document=pdfium.PdfDocument.new()
        try:
            document.new_page(200,200)
            document.new_page(200,200)
            stream=io.BytesIO();document.save(stream)
        finally:document.close()
        self.assertEqual(pdf_page_count(stream.getvalue()),2)

    def test_invalid_pdf_rejected(self):
        with self.assertRaises(ValueError):pdf_page_count(b'not a PDF')
