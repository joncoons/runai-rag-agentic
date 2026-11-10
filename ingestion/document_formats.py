"""Bounded structural source inspection; no rendering, network, or model calls.

Office units describe source structure, not equivalent visual extraction. The
original archive remains authoritative for explicitly excluded package parts.
"""
from __future__ import annotations

import csv
import hashlib
import io
import posixpath
import re
import zipfile
from html.parser import HTMLParser
from pathlib import PurePosixPath
import xml.etree.ElementTree as ET


class DocumentFormatError(ValueError):
    """The source cannot be completely inventoried under this adapter contract."""


MAX_RAW_BYTES = 128 * 1024 * 1024
MAX_ZIP_BYTES = 128 * 1024 * 1024
MAX_PART_BYTES = 16 * 1024 * 1024
MAX_PARTS = 10000
MAX_UNITS = 100000
W = 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'
A = 'http://schemas.openxmlformats.org/drawingml/2006/main'
P = 'http://schemas.openxmlformats.org/presentationml/2006/main'
S = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
R = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
TYPES = {
    '.pdf': ('pdf', 'application/pdf', 'page'),
    '.docx': ('docx', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'paragraph'),
    '.pptx': ('pptx', 'application/vnd.openxmlformats-officedocument.presentationml.presentation', 'slide'),
    '.txt': ('txt', 'text/plain', 'line_block'),
    '.md': ('markdown', 'text/markdown', 'line_block'),
    '.markdown': ('markdown', 'text/markdown', 'line_block'),
    '.csv': ('csv', 'text/csv', 'record'),
    '.xls': ('xls', 'application/vnd.ms-excel', 'cell'),
    '.xlsx': ('xlsx', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'cell'),
    '.html': ('html', 'text/html', 'page'),
    '.htm': ('html', 'text/html', 'page'),
}


class _Units(list):
    def append(self, value):
        if len(self) >= MAX_UNITS:
            raise DocumentFormatError('Source exceeds structural unit limit')
        super().append(value)


def _unit(unit_id, anchors, text='', **extra):
    return {'unit_id': unit_id, 'anchors': anchors, 'status': 'complete', 'text': text, **extra}


def _decode(raw):
    # Reject decoding loss: a replacement character must never silently stand
    # in for source bytes. UTF BOMs explicitly identify supported encodings.
    encoding = 'utf-8-sig'
    if raw.startswith((b'\xff\xfe\x00\x00', b'\x00\x00\xfe\xff')):
        encoding = 'utf-32'
    elif raw.startswith((b'\xff\xfe', b'\xfe\xff')):
        encoding = 'utf-16'
    return raw.decode(encoding, errors='strict')


class _Package:
    def __init__(self, raw):
        self.zip = zipfile.ZipFile(io.BytesIO(raw))
        infos = self.zip.infolist()
        if len(infos) > MAX_PARTS:
            raise DocumentFormatError('OOXML archive exceeds part count limit')
        total = 0
        self.names = set()
        for info in infos:
            name = info.filename
            if name.startswith('/') or '\\' in name or '..' in PurePosixPath(name).parts:
                raise DocumentFormatError('Unsafe OOXML part path')
            if name in self.names:
                raise DocumentFormatError('Duplicate OOXML part path')
            self.names.add(name)
            total += info.file_size
            if info.flag_bits & 1 or info.file_size > MAX_PART_BYTES or total > MAX_ZIP_BYTES:
                raise DocumentFormatError('OOXML archive encrypted or exceeds expansion limit')
        self.used = set()

    def read(self, name):
        if name not in self.names:
            raise DocumentFormatError(f'Missing OOXML part: {name}')
        self.used.add(name)
        with self.zip.open(name) as stream:
            value = stream.read(MAX_PART_BYTES + 1)
        if len(value) > MAX_PART_BYTES:
            raise DocumentFormatError('OOXML part exceeds expansion limit')
        return value

    def xml(self, name):
        value = self.read(name)
        # ElementTree does not resolve external entities; forbid declarations
        # as well, including internal expansion, before parsing.
        if b'<!DOCTYPE' in value.upper() or b'<!ENTITY' in value.upper():
            raise DocumentFormatError('DTD/entity declarations are unsupported')
        return ET.fromstring(value)

    def relations(self, part):
        base, filename = posixpath.split(part)
        relpath = posixpath.join(base, '_rels', filename + '.rels')
        if relpath not in self.names:
            return {}
        result = {}
        relationship_namespace = 'http://schemas.openxmlformats.org/package/2006/relationships'
        root = _checked_root(self, relpath, relationship_namespace, 'Relationships')
        for rel in root:
            if rel.tag != f'{{{relationship_namespace}}}Relationship':
                raise DocumentFormatError(f'Unsupported relationship element in {relpath}')
            key = rel.get('Id')
            if not key or key in result or not rel.get('Target'):
                raise DocumentFormatError(f'Invalid or duplicate OOXML relationship in {relpath}')
            target = rel.get('Target', '')
            if rel.get('TargetMode') == 'External':
                result[key] = {'external': True, 'target': target, 'type': rel.get('Type', '')}
                continue
            resolved = posixpath.normpath(posixpath.join(base, target)) if not target.startswith('/') else target.lstrip('/')
            if resolved.startswith('../'):
                raise DocumentFormatError('Unsafe OOXML relationship target')
            result[key] = {'external': False, 'target': resolved, 'type': rel.get('Type', '')}
        return result

    def finish(self, units):
        images = []
        for name in sorted(self.names):
            if name.endswith('/'):
                continue
            if '/media/' in name:
                raw = self.read(name)
                images.append({'part': name, 'sha256': hashlib.sha256(raw).hexdigest(), 'size_bytes': len(raw), 'retention': 'original_archive_part'})
                units.append({'unit_id': 'part:' + name, 'anchors': {'part': name}, 'status': 'excluded', 'reason': 'Embedded media retained in original archive; visual extraction is a separate transform'})
            elif name not in self.used:
                units.append({'unit_id': 'part:' + name, 'anchors': {'part': name}, 'status': 'excluded', 'reason': 'Non-extracted OOXML package part retained in original archive'})
        return images


def _checked_root(package, part, namespace, local):
    root = package.xml(part)
    if root.tag != f'{{{namespace}}}{local}':
        raise DocumentFormatError(f'Unsupported OOXML root or namespace in {part}: {root.tag}')
    for node in root.iter():
        if node.tag.startswith('{http://purl.oclc.org/ooxml/'):
            raise DocumentFormatError(f'Strict OOXML semantic variants are unsupported: {part}')
        if node.tag == '{http://schemas.openxmlformats.org/markup-compatibility/2006}AlternateContent':
            raise DocumentFormatError(f'OOXML AlternateContent requires unsupported branch selection: {part}')
    return root


def _paragraph_text(element, namespace):
    pieces = []
    for node in element.iter():
        local = node.tag.rsplit('}', 1)[-1]
        if node.tag in {f'{{{namespace}}}t', f'{{{namespace}}}delText', f'{{{namespace}}}instrText'}:
            pieces.append(node.text or '')
        elif local == 'tab':
            pieces.append('\t')
        elif local in {'br', 'cr'}:
            pieces.append('\n')
    return ''.join(pieces)


def _docx(package):
    if 'word/document.xml' not in package.names:
        raise DocumentFormatError('Missing DOCX document part')
    units = _Units()
    parts = [name for name in sorted(package.names) if re.fullmatch(r'word/(document|header\d+|footer\d+|footnotes|endnotes|comments)\.xml', name)]
    for part in parts:
        filename = posixpath.basename(part)
        expected_root = ('document' if filename == 'document.xml' else
                         'hdr' if filename.startswith('header') else
                         'ftr' if filename.startswith('footer') else filename[:-4])
        root = _checked_root(package, part, W, expected_root)
        if filename == 'document.xml' and len(root.findall(f'{{{W}}}body')) != 1:
            raise DocumentFormatError('DOCX document must contain exactly one supported body')
        if any(node.tag == f'{{{W}}}altChunk' for node in root.iter()):
            raise DocumentFormatError('DOCX altChunk content requires an unsupported transform')
        table_cells = {}
        for ti, table in enumerate(root.iter(f'{{{W}}}tbl'), 1):
            for ri, row in enumerate(table.findall(f'{{{W}}}tr'), 1):
                for ci, cell in enumerate(row.findall(f'{{{W}}}tc'), 1):
                    for para in cell.iter(f'{{{W}}}p'):
                        table_cells[para] = {'table_index': ti, 'cell_row': ri, 'cell_column': ci}
        paragraphs = list(root.iter(f'{{{W}}}p'))
        for index, para in enumerate(paragraphs, 1):
            anchors = {'part': part, 'paragraph_index': index, **table_cells.get(para, {})}
            units.append(_unit(f'{part}:p:{index}', anchors, _paragraph_text(para, W)))
        if not paragraphs:
            units.append(_unit(part + ':empty', {'part': part, 'paragraph_index': 0}))
    return units


def _drawing_text(root):
    return '\n'.join(_paragraph_text(p, A) for p in root.iter(f'{{{A}}}p'))


def _pptx(package):
    root = _checked_root(package, 'ppt/presentation.xml', P, 'presentation')
    rels = package.relations('ppt/presentation.xml')
    units = _Units()
    visited = set()
    slides = list(root.iter(f'{{{P}}}sldId'))
    for index, slide in enumerate(slides, 1):
        relation = rels.get(slide.get(f'{{{R}}}id'))
        if not relation or relation['external']:
            raise DocumentFormatError('PPTX slide has no internal relationship')
        part = relation['target']
        if part in visited:
            raise DocumentFormatError('PPTX repeats a slide part')
        visited.add(part)
        slide_root = _checked_root(package, part, P, 'sld')
        anchors = {'part': part, 'slide_number': index, 'hidden': slide_root.get('show', '1') in {'0', 'false'}, 'part_kind': 'slide'}
        units.append(_unit(f'slide:{index}', anchors, _drawing_text(slide_root)))
        for rel in package.relations(part).values():
            if rel['type'].endswith('/notesSlide') and not rel['external']:
                notes = rel['target']
                units.append(_unit(f'slide:{index}:notes', {**anchors, 'part': notes, 'part_kind': 'notes'}, _drawing_text(_checked_root(package, notes, P, 'notes'))))
    if not slides:
        units.append(_unit('presentation:empty', {'part': 'ppt/presentation.xml', 'slide_number': 0}))
    return units


def _text(raw, markdown):
    text = _decode(raw)
    units = _Units()
    offset = 0
    heading = None
    for line, value in enumerate(io.StringIO(text, newline=''), 1):
        match = re.match(r'^ {0,3}#{1,6}[ \t]+(.*?)(?:\r?\n)?$', value) if markdown else None
        if match:
            heading = match.group(1)
        anchors = {'line_start': line, 'line_end': line, 'char_start': offset, 'char_end': offset + len(value)}
        if heading is not None:
            anchors['heading'] = heading
        units.append(_unit(f'line:{line}', anchors, value))
        offset += len(value)
    return units or [_unit('line:1', {'line_start': 1, 'line_end': 1, 'char_start': 0, 'char_end': 0})]


def _csv(raw):
    text = _decode(raw)
    lines = list(io.StringIO(text, newline=''))
    reader = csv.reader(io.StringIO(text, newline=''), strict=True)
    units = _Units()
    previous = 0
    for index, row in enumerate(reader, 1):
        end = reader.line_num
        units.append(_unit(f'record:{index}', {'record_number': index, 'line_start': previous + 1, 'line_end': end}, ''.join(lines[previous:end]), cells=row))
        previous = end
    return units or [_unit('record:1', {'record_number': 1, 'line_start': 1, 'line_end': 1}, cells=[])]


def _column_number(cell):
    match = re.fullmatch(r'([A-Z]+)([1-9][0-9]*)', cell)
    if not match:
        raise DocumentFormatError('Invalid XLSX cell coordinate')
    column = 0
    for char in match.group(1):
        column = column * 26 + ord(char) - 64
    return int(match.group(2)), column


def _xlsx(package):
    workbook = _checked_root(package, 'xl/workbook.xml', S, 'workbook')
    rels = package.relations('xl/workbook.xml')
    strings = []
    if 'xl/sharedStrings.xml' in package.names:
        strings = [''.join(node.text or '' for node in si.iter(f'{{{S}}}t')) for si in _checked_root(package, 'xl/sharedStrings.xml', S, 'sst').findall(f'{{{S}}}si')]
    units = _Units()
    for sheet_index, sheet in enumerate(workbook.iter(f'{{{S}}}sheet'), 1):
        rel = rels.get(sheet.get(f'{{{R}}}id'))
        if not rel or rel['external']:
            raise DocumentFormatError('XLSX sheet has no internal relationship')
        part = rel['target']
        root = _checked_root(package, part, S, 'worksheet')
        base = {'part': part, 'sheet_id': sheet.get('sheetId'), 'sheet_name': sheet.get('name', ''), 'sheet_state': sheet.get('state', 'visible'), 'sheet_index': sheet_index}
        prefix = f'sheet:{sheet_index}'
        dimension = root.find(f'{{{S}}}dimension')
        if dimension is not None and dimension.get('ref'):
            units.append({'unit_id': prefix + ':range', 'anchors': {**base, 'range': dimension.get('ref')}, 'status': 'excluded', 'reason': 'Worksheet extent is not materialized; absent cells are implicit blank cells, explicit cells and rows inventoried separately'})
        for element_index, element in enumerate(root, 1):
            local = element.tag.rsplit('}', 1)[-1]
            if local not in {'sheetData', 'dimension'}:
                units.append({'unit_id': prefix + f':structure:{element_index}', 'anchors': {**base, 'element': local, 'element_index': element_index}, 'status': 'excluded', 'reason': 'Non-cell worksheet structure retained in original archive; no semantic extraction'})
        rows = list(root.iter(f'{{{S}}}row'))
        seen = set()
        for row_index, row in enumerate(rows, 1):
            row_number = int(row.get('r', str(row_index)))
            cells = row.findall(f'{{{S}}}c')
            if not cells:
                units.append(_unit(prefix + f':row:{row_number}', {**base, 'row': row_number}))
            for cell_index, cell in enumerate(cells, 1):
                coordinate = cell.get('r')
                if not coordinate:
                    raise DocumentFormatError('XLSX cell missing explicit coordinate')
                if coordinate in seen:
                    raise DocumentFormatError('XLSX repeats a cell coordinate')
                seen.add(coordinate)
                rn, cn = _column_number(coordinate)
                value_node = cell.find(f'{{{S}}}v')
                value = value_node.text or '' if value_node is not None else ''
                kind = cell.get('t', 'n')
                if kind == 's':
                    try:
                        string_index = int(value)
                        if string_index < 0:
                            raise IndexError
                        text = strings[string_index]
                    except (ValueError, IndexError) as exc:
                        raise DocumentFormatError('Invalid XLSX shared string index') from exc
                elif kind == 'inlineStr':
                    text = ''.join(node.text or '' for node in cell.iter(f'{{{S}}}t'))
                else:
                    text = value
                anchors = {**base, 'cell': coordinate, 'row': rn, 'column': cn, 'cell_type': kind, 'row_hidden': row.get('hidden', '0') in {'1', 'true'}}
                extra = {}
                formula = cell.find(f'{{{S}}}f')
                if formula is not None:
                    extra = {'formula': formula.text or '', 'formula_attributes': dict(formula.attrib), 'cached_value': value if value_node is not None else None, 'formula_cache_status': 'present' if value_node is not None else 'missing'}
                    text = '=' + (formula.text or '') + ('\n' + value if value_node is not None else '')
                units.append(_unit(prefix + ':cell:' + coordinate, anchors, text, **extra))
        if not rows:
            units.append(_unit(prefix + ':empty', base))
    if not list(workbook.iter(f'{{{S}}}sheet')):
        raise DocumentFormatError('XLSX workbook contains no sheets')
    return units


def _xls(raw):
    try:
        import xlrd
    except ImportError as exc:
        raise DocumentFormatError('XLS ingestion requires the locked optional xlrd dependency') from exc
    book = xlrd.open_workbook(file_contents=raw, on_demand=True, formatting_info=True)
    units = _Units()
    try:
        for si in range(book.nsheets):
            sheet = book.sheet_by_index(si)
            base = {'sheet_index': si + 1, 'sheet_name': sheet.name, 'sheet_state': {0: 'visible', 1: 'hidden', 2: 'veryHidden'}.get(sheet.visibility, 'unknown')}
            if sheet.nrows * sheet.ncols > MAX_UNITS:
                raise DocumentFormatError('XLS worksheet exceeds unit limit')
            for ri in range(sheet.nrows):
                for ci in range(sheet.ncols):
                    cell = sheet.cell(ri, ci)
                    value = '' if cell.ctype in {xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK} else str(cell.value)
                    units.append(_unit(f'sheet:{si + 1}:row:{ri + 1}:column:{ci + 1}', {**base, 'row': ri + 1, 'column': ci + 1, 'cell_type': cell.ctype}, value))
            units.append({'unit_id': f'sheet:{si + 1}:formula-exclusion', 'anchors': base, 'status': 'excluded', 'reason': 'xlrd exposes cached values; XLS formulas, drawings and non-cell workbook records remain in original bytes'})
            if not sheet.nrows or not sheet.ncols:
                units.append(_unit(f'sheet:{si + 1}:empty', base))
    finally:
        book.release_resources()
    return units


class _HTML(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.text = []
        self.images = []
        self.exclusions = []
        self.stack = []

    def handle_starttag(self, tag, attrs):
        if tag in {'script', 'style', 'nav'}:
            self.stack.append(tag)
            self.exclusions.append({'reason': f'{tag} content excluded from document text', 'line': self.getpos()[0]})
        if tag == 'img':
            self.images.append({**dict(attrs), 'line': self.getpos()[0], 'retention': 'external_reference_not_fetched'})

    def handle_endtag(self, tag):
        if self.stack and tag == self.stack[-1]:
            self.stack.pop()

    def handle_data(self, data):
        if not self.stack:
            self.text.append(data)


def html_page_text(raw: bytes) -> str:
    """Use exactly the crawler's source-locked whole-page normalization."""
    from application.web_acquisition import normalize_html_text
    return normalize_html_text(raw)


def inspect_document(filename: str, raw: bytes, *, html_version='4', html_input_contract=None) -> dict:
    """Inventory supported original bytes, rejecting loss or incomplete parsing.

    ``units`` retain untruncated source text. Token splitting and any optional
    rendered visual view belong to subsequent, independently identified stages.
    """
    suffix = PurePosixPath(filename).suffix.lower()
    if suffix not in TYPES:
        raise DocumentFormatError(f'Unsupported source extension: {suffix or "<none>"}')
    if not isinstance(raw, bytes) or len(raw) > MAX_RAW_BYTES:
        raise DocumentFormatError('Source must be bytes within the raw size limit')
    kind, media_type, unit_kind = TYPES[suffix]
    result = {'source_kind': kind, 'media_type': media_type, 'unit_kind': unit_kind, 'raw_sha256': hashlib.sha256(raw).hexdigest(), 'transform': {'name': 'agentic-structural-source', 'version': '1'}}
    package = None
    try:
        if kind in {'docx', 'pptx', 'xlsx'}:
            package = _Package(raw)
            units = {'docx': _docx, 'pptx': _pptx, 'xlsx': _xlsx}[kind](package)
            result['images'] = package.finish(units)
        elif kind in {'txt', 'markdown'}:
            units = _text(raw, kind == 'markdown')
        elif kind == 'csv':
            units = _csv(raw)
        elif kind == 'xls':
            units = _xls(raw)
        elif kind == 'html':
            parser = _HTML()
            parser.feed(_decode(raw))
            parser.close()
            if html_version == '1':
                from application.web_acquisition import normalize_html_text_legacy
                units = [_unit('page:1', {'section': 'page', 'heading_path': ['page']}, normalize_html_text_legacy(raw), exclusions=parser.exclusions)]
            elif html_version in ('2','3','4'):
                if html_version=='2':
                    from application.html_disclosures_v2 import html_units
                elif html_version=='3':
                    from application.html_disclosures_v3 import html_units
                else:
                    from application.html_disclosures import html_units
                if html_version=='4':
                    from application.legal_text_budget import load_budget
                    budget=load_budget(html_input_contract)
                    units=html_units(raw,budget=budget)
                    result['transform']['input_contract_sha256']=budget.identity
                else:units=html_units(raw)
                units[0]['exclusions'] = units[0].get('exclusions',[])+parser.exclusions
                result['transform']['version'] = html_version
            else:
                raise DocumentFormatError('Unknown HTML normalization version')
            result['images'] = parser.images
        else:
            try:
                import pypdfium2 as pdfium
            except ImportError as exc:
                raise DocumentFormatError('PDF ingestion requires pypdfium2') from exc
            document = pdfium.PdfDocument(raw)
            try:
                pages = len(document)
                if pages < 1:
                    raise DocumentFormatError('PDF contains no pages')
                units = [_unit(f'page:{page}', {'page_number': page}) for page in range(1, pages + 1)]
                result['page_count'] = pages
            finally:
                document.close()
        if len(units) > MAX_UNITS:
            raise DocumentFormatError('Source exceeds structural unit limit')
        ids = [unit['unit_id'] for unit in units]
        if len(ids) != len(set(ids)):
            raise DocumentFormatError('Source contains duplicate structural unit identities')
        result['units'] = units
        return result
    except DocumentFormatError:
        raise
    except Exception as exc:
        raise DocumentFormatError(f'Invalid {kind} source: {exc}') from exc
    finally:
        if package is not None:
            package.zip.close()

# Proof gates still read/hash the retained original on every validation. Only
# this deterministic transform of those exact verified bytes is memoized.
# JSON bytes prevent callers from mutating a shared cached parsed structure.
from functools import lru_cache
import json as _json

@lru_cache(maxsize=128)
def _retained_html_json(raw, html_version, html_input_contract):
    return _json.dumps(inspect_document('captured-page.html',raw,html_version=html_version,html_input_contract=html_input_contract),sort_keys=True,ensure_ascii=False,allow_nan=False).encode('utf-8')

def inspect_retained_html(raw, html_version='4', html_input_contract=None):
    if len(raw)>1024*1024:
        return inspect_document('captured-page.html',raw,html_version=html_version,html_input_contract=html_input_contract)
    return _json.loads(_retained_html_json(raw,html_version,html_input_contract))
