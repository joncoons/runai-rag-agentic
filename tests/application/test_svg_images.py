import io

import unittest
from PIL import Image

from application.svg_images import SVGValidationError, rasterize_svg, validate_svg


def svg(body='', dimensions='width="80" height="40"'):
    return f'<svg xmlns="http://www.w3.org/2000/svg" {dimensions}>{body}</svg>'.encode()


class SVGImagesTests(unittest.TestCase):
    def test_real_chart_renders_deterministically_on_white_with_recorded_settings(self):
        raw = svg('<defs><linearGradient id="g"><stop stop-color="red"/><stop offset="1" stop-color="blue"/></linearGradient></defs><rect width="30" height="30" fill="url(#g)"/><text x="35" y="20">42</text>')
        png, metadata = rasterize_svg(raw)
        assert png == rasterize_svg(raw)[0]
        image = Image.open(io.BytesIO(png)).convert('RGB')
        assert image.size == (80, 40)
        assert image.getpixel((79, 39)) == (255, 255, 255)
        assert image.getpixel((10, 10)) != (255, 255, 255)
        assert metadata['renderer'] == 'cairosvg'
        assert metadata['renderer_version']
        assert metadata['original_dimensions'] == {'width': '80', 'height': '40', 'viewBox': None}
        assert metadata['settings']['background_color'] == '#ffffff'
        assert metadata['settings']['dpi'] == 96

    def test_viewbox_and_physical_units_have_bounded_dimensions(self):
        assert validate_svg(svg(dimensions='viewBox="0 0 120 60"'))['width'] == 120
        assert validate_svg(svg(dimensions='width="1in" height="0.5in"'))['height'] == 48
        with self.assertRaises(SVGValidationError):
            validate_svg(svg(dimensions='width="8000" height="8000"'))
        with self.assertRaises(SVGValidationError):
            validate_svg(svg(), max_pixels=100)

    def test_untrusted_active_or_external_content_rejected_before_render(self):
        for body in [
            '<script>alert(1)</script>', '<image href="#local"/>', '<g xml:base="https://example.invalid/"/>', '<rect onclick="x()"/>', '<foreignObject/>',
            '<animate attributeName="x"/>', '<image href="https://example.invalid/x.png"/>',
            '<use href="file:///etc/passwd"/>', '<use href="data:image/svg+xml,xx"/>',
            '<rect fill="url(https://example.invalid/paint)"/>',
            '<style>@import "https://example.invalid/style";</style>',
            '<style>@font-face {font-family:x; src:url(file:///etc/passwd)}</style>',
            '<style>rect{fill:u\\72l(https://example.invalid/x)}</style>',
            '<rect style="fill:u\\72l(\\68 ttps://example.invalid/x)"/>',
            '<style>rect{fill:url(/**/https://example.invalid/x)}</style>',
            '<style>@\\69mport \"https://example.invalid/x\";</style>', '<rect style="fill:url(&quot;file:///etc/passwd&quot;)"/>', '<use href="#missing"/>', '<g id="a"><use href="#a"/></g>',
            '<defs><g id="a"><use href="#b"/></g><g id="b"><use href="#a"/></g></defs>',
        ]:
            with self.subTest(body=body), self.assertRaises(SVGValidationError):
                rasterize_svg(svg(body))

    def test_bad_xml_and_oversized_input_rejected(self):
        for raw in [
            b'<!DOCTYPE svg [<!ENTITY x "hello">]><svg>&x;</svg>',
            b'<?xml-stylesheet href="https://example.invalid/x"?><svg/>',
            b'<html/>', b'<svg><g></svg>', b'<svg>' + b' ' * 2_100_000 + b'</svg>',
        ]:
            with self.subTest(raw=raw[:100]), self.assertRaises(SVGValidationError):
                validate_svg(raw)

    def test_complexity_limits_apply_before_render(self):
        for body in ['<g>' * 70 + '</g>' * 70, '<path d="' + 'M0 0 ' * 60_000 + '"/>', '<rect/>' * 10_001]:
            with self.assertRaises(SVGValidationError):
                validate_svg(svg(body))
        defs = '<g id="a0"><rect/></g>' + ''.join(f'<g id="a{i}"><use href="#a{i-1}"/><use href="#a{i-1}"/></g>' for i in range(1, 20))
        with self.assertRaises(SVGValidationError):
            validate_svg(svg('<defs>' + defs + '</defs><use href="#a19"/>'))

    def test_local_use_clip_and_escaped_css_gradient_references_render(self):
        raw = svg('<defs><clipPath id="clip"><rect width="20" height="20"/></clipPath><linearGradient id="g"><stop stop-color="red"/></linearGradient><rect id="r" width="40" height="40"/></defs><style>use { fill:u\\72l(#g); clip-path:url(#clip) }</style><use href="#r"/>')
        png, _ = rasterize_svg(raw)
        image = Image.open(io.BytesIO(png)).convert('RGB')
        assert image.getpixel((5, 5)) == (255, 0, 0)
        assert image.getpixel((30, 30)) == (255, 255, 255)
    def test_invalid_dimensions_and_xml_encodings_fail_closed(self):
        for dimensions in ('width="0" height="2"', 'width="nan" height="4"',
                           'width="99999999999" height="1"', 'viewBox="0 0 -1 2"'):
            with self.subTest(dimensions=dimensions), self.assertRaises(SVGValidationError):
                validate_svg(svg(dimensions=dimensions))
        with self.assertRaises(SVGValidationError):
            validate_svg('<svg/>'.encode('utf-16'))
    def test_local_xlink_reference_is_supported(self):
        raw = b'<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="20" height="20"><defs><rect id="r" width="20" height="20" fill="blue"/></defs><use xlink:href="#r"/></svg>'
        png, metadata = rasterize_svg(raw)
        image = Image.open(io.BytesIO(png)).convert('RGB')
        self.assertEqual(image.getpixel((10, 10)), (0, 0, 255))
        self.assertEqual(metadata['height'], 20)
    def test_deeply_nested_css_is_rejected_before_tokenizer_recursion(self):
        with self.assertRaises(SVGValidationError):
            validate_svg(svg('<style>rect{fill:' + 'f(' * 2000 + 'red' + ')' * 2000 + '}</style>'))

    def test_duplicate_decorative_ids_render_without_resource_ambiguity(self):
        raw = svg('<g id="decoration"><rect width="20" height="20" fill="red"/></g><g id="decoration"><rect x="20" width="20" height="20" fill="blue"/></g>')
        png, metadata = rasterize_svg(raw)
        image = Image.open(io.BytesIO(png)).convert('RGB')
        self.assertEqual(image.getpixel((5, 5)), (255, 0, 0))
        self.assertEqual(image.getpixel((25, 5)), (0, 0, 255))
        self.assertEqual(metadata['duplicate_id_count'], 1)

    def test_references_to_duplicated_ids_remain_ambiguous_and_rejected(self):
        for reference in ('<use href="#same"/>', '<rect fill="url(#same)"/>',
                          '<style>rect {fill: url(#same)}</style>'):
            with self.subTest(reference=reference), self.assertRaises(SVGValidationError):
                rasterize_svg(svg('<g id="same"/><g id="same"/>' + reference))

    def test_embedded_png_renders_without_any_external_fetch_and_records_hash(self):
        import base64
        import hashlib
        image = Image.new('RGBA', (2, 2), (255, 0, 0, 255))
        buffer = io.BytesIO()
        image.save(buffer, format='PNG')
        payload = buffer.getvalue()
        uri = 'data:image/png;base64,' + base64.b64encode(payload).decode()
        png, metadata = rasterize_svg(svg(f'<image href="{uri}" width="20" height="20"/>'))
        self.assertEqual(Image.open(io.BytesIO(png)).convert('RGB').getpixel((10, 10)), (255, 0, 0))
        self.assertEqual(metadata['embedded_images'][0]['raw_sha256'], 'sha256:' + hashlib.sha256(payload).hexdigest())
        self.assertEqual(metadata['embedded_images'][0]['pixels'], 4)

    def test_embedded_raster_must_be_valid_png_with_bounded_decoded_pixels(self):
        import base64
        buffer = io.BytesIO()
        Image.new('RGB', (100, 100)).save(buffer, format='PNG')
        valid = base64.b64encode(buffer.getvalue()).decode()
        for href in ('data:image/svg+xml;base64,PHN2Zy8+', 'data:image/png;base64,bm90LXBuZw==',
                     'data:image/png;base64,@@@', 'https://example.invalid/logo.png', 'file:///etc/passwd'):
            with self.subTest(href=href), self.assertRaises(SVGValidationError):
                validate_svg(svg(f'<image href="{href}"/>'))
        with self.assertRaises(SVGValidationError):
            validate_svg(svg(f'<image href="data:image/png;base64,{valid}"/>'), max_pixels=5_000)
        with self.assertRaises(SVGValidationError):
            validate_svg(svg(f'<image href="data:image/png;base64,{valid}"/><image href="data:image/png;base64,{valid}"/>'), max_pixels=15_000)

    def test_embedded_png_extreme_dimension_is_rejected_even_below_pixel_budget(self):
        import base64
        buffer = io.BytesIO()
        Image.new('RGB', (40_000, 1)).save(buffer, format='PNG')
        uri = 'data:image/png;base64,' + base64.b64encode(buffer.getvalue()).decode()
        with self.assertRaises(SVGValidationError):
            validate_svg(svg(f'<image href="{uri}"/>'))
