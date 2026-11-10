"""Source-format coverage uses generated, nonprivate fixtures."""
import base64
import hashlib
import io
import unittest
import zipfile

from ingestion.document_formats import DocumentFormatError, inspect_document


def archive(parts):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, 'w', zipfile.ZIP_DEFLATED) as bundle:
        for name, content in parts.items():
            bundle.writestr(name, content)
    return stream.getvalue()


def fixtures():
    """Public, generated nonprivate documents for offline and live qualification.

    Minimal OOXML containers exercise structural extraction; they do not assert
    visual Office rendering or model equivalence. XLS was generated with xlwt
    solely as fixture tooling, which is not a runtime dependency.
    """
    w = 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'
    p = 'http://schemas.openxmlformats.org/presentationml/2006/main'
    a = 'http://schemas.openxmlformats.org/drawingml/2006/main'
    s = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
    r = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
    rel = 'http://schemas.openxmlformats.org/package/2006/relationships'
    def relationships(items):
        return '<Relationships xmlns="' + rel + '">' + ''.join('<Relationship Id="' + key + '" Target="' + target + '" Type="' + r + '/' + kind + '"/>' for key, target, kind in items) + '</Relationships>'
    def types(parts):
        return '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/>' + ''.join('<Override PartName="/' + name + '" ContentType="' + content_type + '"/>' for name, content_type in parts) + '</Types>'
    docx = {
        '[Content_Types].xml': types([('word/document.xml', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml')]),
        '_rels/.rels': relationships([('rId1', 'word/document.xml', 'officeDocument')]),
        'word/document.xml': f'<w:document xmlns:w="{w}"><w:body><w:p><w:r><w:t>DOCX_MAIN_SENTINEL</w:t></w:r></w:p><w:tbl><w:tr><w:tc><w:p><w:r><w:t>DOCX_TABLE_SENTINEL</w:t></w:r></w:p></w:tc></w:tr></w:tbl><w:p><w:r><w:t>DOCX_LATE_SENTINEL</w:t></w:r></w:p></w:body></w:document>',
        'word/header1.xml': f'<w:hdr xmlns:w="{w}"><w:p><w:r><w:t>DOCX_HEADER_SENTINEL</w:t></w:r></w:p></w:hdr>',
        'word/footnotes.xml': f'<w:footnotes xmlns:w="{w}"><w:footnote w:id="1"><w:p><w:r><w:t>DOCX_FOOTNOTE_SENTINEL</w:t></w:r></w:p></w:footnote></w:footnotes>',
    }
    pptx = {
        '[Content_Types].xml': types([('ppt/presentation.xml', 'application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml'), ('ppt/slides/slide1.xml', 'application/vnd.openxmlformats-officedocument.presentationml.slide+xml'), ('ppt/slides/slide2.xml', 'application/vnd.openxmlformats-officedocument.presentationml.slide+xml'), ('ppt/notesSlides/notesSlide2.xml', 'application/vnd.openxmlformats-officedocument.presentationml.notesSlide+xml')]),
        '_rels/.rels': relationships([('rId1', 'ppt/presentation.xml', 'officeDocument')]),
        'ppt/presentation.xml': f'<p:presentation xmlns:p="{p}" xmlns:r="{r}"><p:sldIdLst><p:sldId id="256" r:id="s1"/><p:sldId id="257" r:id="s2"/></p:sldIdLst></p:presentation>',
        'ppt/_rels/presentation.xml.rels': relationships([('s1', 'slides/slide1.xml', 'slide'), ('s2', 'slides/slide2.xml', 'slide')]),
        'ppt/slides/slide1.xml': f'<p:sld xmlns:p="{p}" xmlns:a="{a}"><p:cSld><p:spTree><p:sp><p:txBody><a:p><a:r><a:t>PPTX_MAIN_SENTINEL</a:t></a:r></a:p></p:txBody></p:sp></p:spTree></p:cSld></p:sld>',
        'ppt/slides/slide2.xml': f'<p:sld xmlns:p="{p}" xmlns:a="{a}" show="0"><p:cSld><p:spTree><p:sp><p:txBody><a:p><a:r><a:t>PPTX_HIDDEN_LATE_SENTINEL</a:t></a:r></a:p></p:txBody></p:sp></p:spTree></p:cSld></p:sld>',
        'ppt/slides/_rels/slide2.xml.rels': relationships([('n1', '../notesSlides/notesSlide2.xml', 'notesSlide')]),
        'ppt/notesSlides/notesSlide2.xml': f'<p:notes xmlns:p="{p}" xmlns:a="{a}"><a:p><a:r><a:t>PPTX_NOTES_SENTINEL</a:t></a:r></a:p></p:notes>',
    }
    xlsx = {
        '[Content_Types].xml': types([('xl/workbook.xml', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml'), ('xl/worksheets/sheet1.xml', 'application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml'), ('xl/worksheets/sheet2.xml', 'application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml')]),
        '_rels/.rels': relationships([('rId1', 'xl/workbook.xml', 'officeDocument')]),
        'xl/workbook.xml': f'<workbook xmlns="{s}" xmlns:r="{r}"><sheets><sheet name="Main" sheetId="1" r:id="s1"/><sheet name="Hidden" sheetId="2" state="hidden" r:id="s2"/></sheets></workbook>',
        'xl/_rels/workbook.xml.rels': relationships([('s1', 'worksheets/sheet1.xml', 'worksheet'), ('s2', 'worksheets/sheet2.xml', 'worksheet')]),
        'xl/worksheets/sheet1.xml': f'<worksheet xmlns="{s}"><dimension ref="A1:B3"/><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>XLSX_MAIN_SENTINEL</t></is></c><c r="B1"><f>SUM(1,2)</f><v>3</v></c></row><row r="3"><c r="A3" t="inlineStr"><is><t>XLSX_LATE_SENTINEL</t></is></c><c r="B3"/></row></sheetData></worksheet>',
        'xl/worksheets/sheet2.xml': f'<worksheet xmlns="{s}"><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>XLSX_HIDDEN_SENTINEL</t></is></c></row></sheetData></worksheet>',
    }
    return {
        'fixture.docx': archive(docx), 'fixture.pptx': archive(pptx),
        'fixture.xls': base64.b64decode('0M8R4KGxGuEAAAAAAAAAAAAAAAAAAAAAPgADAP7/CQAGAAAAAAAAAAAAAAABAAAACQAAAAAAAAAAEAAA/v///wAAAAD+////AAAAAAgAAAD///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////8JCBAAAAYFALsNzAcAAAAABgAAAOEAAgCwBMEAAgAAAOIAAABcAHAATm9uZSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIEIAAgCwBGEBAgAAAD0BBAABAAIAnAACAA4AGQACAAAAEgACAAAAYwACAAAAEwACAAAArwECAAAAvAECAAAAQAACAAAAjQACAAAAPQASAOABWgDPP04qOAAAAAAAAQBYAiIAAgAAAA4AAgABALcBAgAAANoAAgAAADEAFQDIAAAA/3+QAQAAAAABAAUAQXJpYWwxABUAyAAAAP9/kAEAAAAAAQAFAEFyaWFsMQAVAMgAAAD/f5ABAAAAAAEABQBBcmlhbDEAFQDIAAAA/3+QAQAAAAABAAUAQXJpYWwxABUAyAAAAP9/kAEAAAAA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AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAIAAAADAAAABAAAAAUAAAAGAAAABwAAAP7////9/////v////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////9SAG8AbwB0ACAARQBuAHQAcgB5AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFgAFAf//////////AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP7///8AAAAAAAAAAFcAbwByAGsAYgBvAG8AawAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAASAAIB////////////////AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAH///////////////8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD+////AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAf///////////////wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP7///8AAAAAAAAAAA=='),
        'fixture.xlsx': archive(xlsx),
        'fixture.csv': b'name,note\r\nCSV_MAIN_SENTINEL,"line one\r\nline two"\r\nlate,CSV_LATE_SENTINEL\r\n',
        'fixture.md': b'# Markdown fixture\n\nMD_MAIN_SENTINEL\n\n## Later\nMD_LATE_SENTINEL\n',
        'fixture.txt': b'TXT_MAIN_SENTINEL\n\n' + b'complete text ' * 1500 + b'TXT_LATE_SENTINEL\n',
        'fixture.html': b'<html><head><title>HTML title</title></head><body><nav>excluded navigation</nav><main><h1>HTML_MAIN_SENTINEL</h1><p>HTML_LATE_SENTINEL</p></main></body></html>',
    }


class DocumentFormatsTests(unittest.TestCase):
    def test_text_retains_whitespace_blank_lines_and_long_tail(self):
        raw = ('  first\r\n\r\n' + 'x' * 12000 + 'TAIL\n').encode()
        result = inspect_document('SOURCE.TXT', raw)
        self.assertEqual(result['source_kind'], 'txt')
        self.assertEqual(result['raw_sha256'], hashlib.sha256(raw).hexdigest())
        self.assertEqual(''.join(u['text'] for u in result['units']), raw.decode())
        self.assertEqual(result['units'][1]['anchors']['line_start'], 2)
        self.assertTrue(all(u['status'] == 'complete' for u in result['units']))
        self.assertEqual(result, inspect_document('SOURCE.TXT', raw))

    def test_markdown_heading_and_blank_document_have_units(self):
        result = inspect_document('notes.md', b'# Heading\n\nbody\n')
        self.assertEqual(result['source_kind'], 'markdown')
        self.assertEqual(result['units'][0]['anchors']['heading'], 'Heading')
        self.assertEqual(inspect_document('empty.txt', b'')['units'][0]['text'], '')

    def test_csv_quoted_multiline_preserves_record_and_physical_spans(self):
        result = inspect_document('table.csv', b'\xef\xbb\xbfcol,note\r\n1,"two\r\nlines"\r\n\r\n2,tail\r\n')
        units = result['units']
        self.assertEqual(len(units), 4)
        self.assertEqual(units[1]['anchors']['line_start'], 2)
        self.assertEqual(units[1]['anchors']['line_end'], 3)
        self.assertEqual(units[1]['cells'], ['1', 'two\r\nlines'])
        self.assertEqual(units[2]['cells'], [])
        self.assertEqual(units[3]['anchors']['record_number'], 4)
        self.assertEqual(units[3]['text'], '2,tail\r\n')

    def test_docx_keeps_tables_headers_notes_and_media(self):
        ns = 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'
        raw = archive({'word/document.xml': f'<w:document xmlns:w="{ns}"><w:body><w:p><w:r><w:t xml:space="preserve">  paragraph </w:t><w:tab/><w:t>end</w:t></w:r></w:p><w:tbl><w:tr><w:tc><w:p><w:r><w:t>cell</w:t></w:r></w:p></w:tc></w:tr></w:tbl><w:p/></w:body></w:document>', 'word/header1.xml': f'<w:hdr xmlns:w="{ns}"><w:p><w:r><w:t>header</w:t></w:r></w:p></w:hdr>', 'word/footnotes.xml': f'<w:footnotes xmlns:w="{ns}"><w:footnote w:id="2"><w:p><w:r><w:t>footnote</w:t></w:r></w:p></w:footnote></w:footnotes>', 'word/media/image1.png': b'fake-retained-image', 'word/charts/chart1.xml': '<chart/>'})
        result = inspect_document('a.docx', raw)
        complete = [u for u in result['units'] if u['status'] == 'complete']
        self.assertIn('  paragraph \tend', [u.get('text') for u in complete])
        self.assertIn('cell', [u.get('text') for u in complete])
        cell = next(u for u in complete if u.get('text') == 'cell')
        self.assertEqual(cell['anchors']['table_index'], 1)
        self.assertEqual(cell['anchors']['cell_column'], 1)
        self.assertIn('header', [u.get('text') for u in complete])
        self.assertIn('footnote', [u.get('text') for u in complete])
        self.assertEqual(result['images'][0]['part'], 'word/media/image1.png')
        self.assertTrue(any(u['status'] == 'excluded' and u['anchors']['part'] == 'word/charts/chart1.xml' for u in result['units']))

    def test_pptx_keeps_hidden_slide_order_and_notes(self):
        a = 'http://schemas.openxmlformats.org/drawingml/2006/main'
        p = 'http://schemas.openxmlformats.org/presentationml/2006/main'
        r = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
        rel = 'http://schemas.openxmlformats.org/package/2006/relationships'
        raw = archive({'ppt/presentation.xml': f'<p:presentation xmlns:p="{p}" xmlns:r="{r}"><p:sldIdLst><p:sldId id="99" r:id="rId1"/></p:sldIdLst></p:presentation>', 'ppt/_rels/presentation.xml.rels': f'<Relationships xmlns="{rel}"><Relationship Id="rId1" Target="slides/slide9.xml" Type="{r}/slide"/></Relationships>', 'ppt/slides/slide9.xml': f'<p:sld xmlns:p="{p}" xmlns:a="{a}" show="0"><p:cSld><p:spTree><p:sp><p:txBody><a:p><a:r><a:t>Hidden slide</a:t></a:r></a:p></p:txBody></p:sp></p:spTree></p:cSld></p:sld>', 'ppt/slides/_rels/slide9.xml.rels': f'<Relationships xmlns="{rel}"><Relationship Id="n1" Target="../notesSlides/notesSlide2.xml" Type="{r}/notesSlide"/></Relationships>', 'ppt/notesSlides/notesSlide2.xml': f'<p:notes xmlns:p="{p}" xmlns:a="{a}"><a:p><a:r><a:t>Speaker note</a:t></a:r></a:p></p:notes>'})
        result = inspect_document('deck.pptx', raw)
        slide = next(u for u in result['units'] if u.get('text') == 'Hidden slide')
        self.assertEqual(slide['anchors']['slide_number'], 1)
        self.assertTrue(slide['anchors']['hidden'])
        notes = next(u for u in result['units'] if u.get('text') == 'Speaker note')
        self.assertEqual(notes['anchors']['slide_number'], 1)
        self.assertEqual(notes['anchors']['part_kind'], 'notes')

    def test_xlsx_preserves_hidden_sheet_formula_cache_and_blank_cells(self):
        s = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
        r = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
        rel = 'http://schemas.openxmlformats.org/package/2006/relationships'
        raw = archive({'xl/workbook.xml': f'<workbook xmlns="{s}" xmlns:r="{r}"><sheets><sheet name="Hidden" sheetId="7" state="veryHidden" r:id="rId1"/><sheet name="Empty" sheetId="8" r:id="rId2"/></sheets></workbook>', 'xl/_rels/workbook.xml.rels': f'<Relationships xmlns="{rel}"><Relationship Id="rId1" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Target="worksheets/sheet2.xml"/></Relationships>', 'xl/sharedStrings.xml': f'<sst xmlns="{s}"><si><r><t xml:space="preserve"> first </t></r><r><t>tail</t></r></si></sst>', 'xl/worksheets/sheet1.xml': f'<worksheet xmlns="{s}"><dimension ref="A1:C5"/><sheetData><row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1"><f>SUM(A2:A5)</f><v>12</v></c><c r="C1"/></row><row r="5"/></sheetData></worksheet>', 'xl/worksheets/sheet2.xml': f'<worksheet xmlns="{s}"><sheetData/></worksheet>'})
        result = inspect_document('book.xlsx', raw)
        a1 = next(u for u in result['units'] if u['anchors'].get('cell') == 'A1')
        self.assertEqual(a1['text'], ' first tail')
        self.assertEqual(a1['anchors']['sheet_state'], 'veryHidden')
        b1 = next(u for u in result['units'] if u['anchors'].get('cell') == 'B1')
        self.assertEqual(b1['formula'], 'SUM(A2:A5)')
        self.assertEqual(b1['cached_value'], '12')
        self.assertEqual(next(u for u in result['units'] if u['anchors'].get('cell') == 'C1')['text'], '')
        self.assertTrue(any(u['anchors'].get('row') == 5 and u.get('text') == '' for u in result['units']))
        self.assertTrue(any(u['anchors'].get('sheet_name') == 'Empty' for u in result['units']))
        self.assertTrue(any(u['anchors'].get('range') == 'A1:C5' and u['status'] == 'excluded' for u in result['units']))

    def test_rejects_unknown_bad_encoding_bad_csv_and_unsafe_archives(self):
        for filename, raw in [('x.exe', b'x'), ('x.txt', b'\xff'), ('x.csv', b'a,"unterminated'), ('x.docx', archive({'../escape.xml': '<x/>'})), ('x.docx', b'bad zip')]:
            with self.subTest(filename=filename):
                with self.assertRaises(DocumentFormatError):
                    inspect_document(filename, raw)
        huge = archive({'word/document.xml': 'x' * (17 * 1024 * 1024)})
        with self.assertRaises(DocumentFormatError):
            inspect_document('bomb.docx', huge)

    def test_xls_missing_dependency_is_explicit(self):
        try:
            import xlrd  # noqa: F401
        except ImportError:
            with self.assertRaisesRegex(DocumentFormatError, 'xlrd'):
                inspect_document('legacy.xls', b'not parsed without dependency')

    def test_html_keeps_dom_text_and_records_nontext_exclusions(self):
        result = inspect_document('page.html', b'<html><body><h1>Heading</h1><p> A <b>bold</b> tail </p><script>skip()</script><img src="a.png" alt="Figure"/></body></html>')
        self.assertIn('bold', result['units'][0]['text'])
        self.assertEqual(result['units'][0]['unit_id'], 'page:1')
        self.assertTrue(any('script' in item['reason'] for item in result['units'][0]['exclusions']))
        self.assertEqual(result['images'][0]['src'], 'a.png')

    def test_html_whole_page_normalization_excludes_navigation(self):
        result = inspect_document('page.html', b'<html><head><title>My title</title></head><body><nav>Skip nav</nav><h1>Heading</h1><p> A <b>bold</b> tail </p></body></html>')
        self.assertEqual(result['units'][0]['text'], 'Heading\nA\nbold\ntail')
        self.assertEqual(result['units'][0]['anchors'], {'section': 'page', 'heading_path': ['page']})
        self.assertNotIn('Skip nav', result['units'][0]['text'])

    def test_csv_vertical_tab_is_a_cell_character_not_physical_record_line(self):
        units = inspect_document('a.csv', b'a,b\n1,"a\vb"\n2,tail\n')['units']
        self.assertEqual(units[1]['text'], '1,"a\vb"\n')
        self.assertEqual(units[2]['text'], '2,tail\n')

    def test_txt_vertical_tab_does_not_forge_a_line_anchor(self):
        units = inspect_document('a.txt', b'a\vb\nnext\n')['units']
        self.assertEqual(units[0]['text'], 'a\vb\n')
        self.assertEqual(units[1]['anchors']['line_start'], 2)

    def test_xlsx_noncell_structures_are_explicit_exclusions(self):
        raw = fixtures()['fixture.xlsx']
        with zipfile.ZipFile(io.BytesIO(raw)) as bundle:
            parts = {name: bundle.read(name) for name in bundle.namelist()}
        parts['xl/worksheets/sheet1.xml'] = parts['xl/worksheets/sheet1.xml'].replace(b'</worksheet>', b'<mergeCells><mergeCell ref="A1:B1"/></mergeCells></worksheet>')
        units = inspect_document('x.xlsx', archive(parts))['units']
        self.assertTrue(any(u['status'] == 'excluded' and u['anchors'].get('element') == 'mergeCells' for u in units))

    def test_generated_xls_retains_late_and_hidden_cells_when_xlrd_available(self):
        try:
            import xlrd  # noqa: F401
        except ImportError:
            self.skipTest('locked optional xlrd not installed in host interpreter')
        result = inspect_document('legacy.xls', fixtures()['fixture.xls'])
        text = [u.get('text') for u in result['units']]
        self.assertIn('XLS_MAIN_SENTINEL', text)
        self.assertIn('XLS_LATE_SENTINEL', text)
        hidden = next(u for u in result['units'] if u.get('text') == 'XLS_HIDDEN_SENTINEL')
        self.assertEqual(hidden['anchors']['sheet_state'], 'hidden')
        self.assertTrue(any(u['status'] == 'excluded' and 'formula' in u['reason'] for u in result['units']))

    def test_pdf_units_match_actual_page_count_when_pdfium_available(self):
        try:
            import pypdfium2 as pdfium
        except ImportError:
            self.skipTest('PDFium is supplied by the pinned application image')
        document = pdfium.PdfDocument.new()
        try:
            document.new_page(100, 100)
            document.new_page(100, 100)
            stream = io.BytesIO()
            document.save(stream)
        finally:
            document.close()
        result = inspect_document('two.pdf', stream.getvalue())
        self.assertEqual(result['page_count'], 2)
        self.assertEqual([u['unit_id'] for u in result['units']], ['page:1', 'page:2'])
        with self.assertRaises(DocumentFormatError):
            inspect_document('bad.pdf', b'not pdf')

    def test_ooxml_wrong_roots_and_strict_namespaces_fail_closed(self):
        strict = 'http://purl.oclc.org/ooxml/wordprocessingml/main'
        cases = [
            ('fixture.docx', 'word/document.xml', '<garbage/>'),
            ('fixture.docx', 'word/document.xml', f'<w:document xmlns:w="{strict}"><w:body><w:p><w:r><w:t>DO_NOT_DROP</w:t></w:r></w:p></w:body></w:document>'),
            ('fixture.docx', 'word/header1.xml', '<garbage/>'),
            ('fixture.docx', 'word/footer1.xml', '<garbage/>'),
            ('fixture.docx', 'word/endnotes.xml', '<garbage/>'),
            ('fixture.docx', 'word/comments.xml', '<garbage/>'),
            ('fixture.docx', 'word/footnotes.xml', '<garbage/>'),
            ('fixture.pptx', 'ppt/presentation.xml', '<garbage/>'),
            ('fixture.pptx', 'ppt/slides/slide1.xml', '<garbage/>'),
            ('fixture.pptx', 'ppt/notesSlides/notesSlide2.xml', '<garbage/>'),
            ('fixture.xlsx', 'xl/workbook.xml', '<garbage/>'),
            ('fixture.xlsx', 'xl/worksheets/sheet1.xml', '<garbage/>'),
            ('fixture.xlsx', 'xl/sharedStrings.xml', '<garbage/>'),
            ('fixture.pptx', 'ppt/_rels/presentation.xml.rels', '<garbage/>'),
            ('fixture.xlsx', 'xl/_rels/workbook.xml.rels', '<garbage/>'),
            ('fixture.docx', 'word/document.xml', '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main" xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006"><w:body><mc:AlternateContent><mc:Choice Requires="x">DO_NOT_DROP</mc:Choice></mc:AlternateContent></w:body></w:document>'),
            ('fixture.pptx', 'ppt/presentation.xml', '<p:presentation xmlns:p="http://purl.oclc.org/ooxml/presentationml/main"/>'),
            ('fixture.xlsx', 'xl/workbook.xml', '<workbook xmlns="http://purl.oclc.org/ooxml/spreadsheetml/main"/>'),
            ('fixture.xlsx', 'xl/worksheets/sheet1.xml', '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData xmlns="http://purl.oclc.org/ooxml/spreadsheetml/main"><row><c><v>DO_NOT_DROP</v></c></row></sheetData></worksheet>'),
        ]
        for name, part, replacement in cases:
            with self.subTest(name=name, part=part, replacement=replacement):
                with zipfile.ZipFile(io.BytesIO(fixtures()[name])) as bundle:
                    parts = {path: bundle.read(path) for path in bundle.namelist()}
                parts[part] = replacement
                with self.assertRaises(DocumentFormatError):
                    inspect_document(name, archive(parts))

    def test_docx_missing_body_and_altchunk_are_not_empty_successes(self):
        w = 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'
        for content in [f'<w:document xmlns:w="{w}"/>', f'<w:document xmlns:w="{w}"><w:body><w:altChunk/></w:body></w:document>']:
            with self.subTest(content=content):
                with self.assertRaises(DocumentFormatError):
                    inspect_document('a.docx', archive({'word/document.xml': content}))
        result = inspect_document('a.docx', archive({'word/document.xml': f'<w:document xmlns:w="{w}"><w:body/></w:document>'}))
        self.assertEqual(len(result['units']), 1)
        self.assertEqual(result['units'][0]['text'], '')
        self.assertEqual(result['units'][0]['status'], 'complete')

    def test_empty_presentation_does_not_hide_invalid_relationship_root(self):
        p = 'http://schemas.openxmlformats.org/presentationml/2006/main'
        raw = archive({'ppt/presentation.xml': f'<p:presentation xmlns:p="{p}"/>', 'ppt/_rels/presentation.xml.rels': '<garbage/>'})
        with self.assertRaises(DocumentFormatError):
            inspect_document('bad.pptx', raw)


if __name__ == '__main__':
    unittest.main()
