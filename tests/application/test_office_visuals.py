"""Generated nonprivate Office visual provenance and loss rejection."""
import asyncio
import copy
import hashlib
from io import BytesIO
import json
from pathlib import Path
import tempfile
import unittest
import zipfile
from PIL import Image
from ingestion.document_formats import inspect_document
from application.office_visuals import OfficeVisualProcessor
from collection_control.office_visuals import verify_office_visuals, office_inventory
from collection_control.repository import Conflict


def source(hidden=False, external=False):
    w='http://schemas.openxmlformats.org/wordprocessingml/2006/main'
    rel='http://schemas.openxmlformats.org/package/2006/relationships'
    r='http://schemas.openxmlformats.org/officeDocument/2006/relationships'
    image=BytesIO();Image.new('RGB',(32,24),'red').save(image,format='PNG')
    data=BytesIO()
    with zipfile.ZipFile(data,'w') as z:
        z.writestr('word/document.xml',f'<w:document xmlns:w="{w}" xmlns:r="{r}"><w:body><w:p><w:r><w:t>Original complete text</w:t><w:drawing r:embed="im1"/></w:r></w:p></w:body></w:document>')
        z.writestr('word/_rels/document.xml.rels',f'<Relationships xmlns="{rel}"><Relationship Id="im1" Type="{r}/image" Target="'+('https://example.test/image.png" TargetMode="External' if external else 'media/image.png')+'"/></Relationships>')
        z.writestr('word/media/image.png',image.getvalue())
    return data.getvalue()


def native_manifest(raw):
    parsed=inspect_document('source.docx',raw)
    return {'schema_version':'native-source-units.v1','source_kind':'docx','transform':parsed['transform'],'units':parsed['units']}


def pdf_bytes():
    stream=BytesIO();Image.new('RGB',(80,60),'white').save(stream,format='PDF');return stream.getvalue()


class OfficeVisualTests(unittest.IsolatedAsyncioTestCase):
    async def extract(self,folder,raw=None,runner=None):
        raw=raw or source();root=Path(folder);root.joinpath('raw').mkdir(exist_ok=True)
        path=root/'raw'/'original.docx';path.write_bytes(raw)
        manifest=native_manifest(raw)
        runner=runner or (lambda path:[{'page_number':1,'text':'OCR pixels','page_elements_v3':{'detections':[{'label_name':'chart','bbox_xyxy_norm':[0,0,1,1]}]},'chart':[{'text':'Chart value 42','bbox_xyxy_norm':[0,0,1,1]}]}])
        result=await OfficeVisualProcessor(root=root/'stages',raw_root=root/'raw',renderer=lambda raw,name:pdf_bytes(),runner=runner).extract(raw,'source.docx',manifest,raw_uri=path.as_uri())
        return root,raw,manifest,result

    async def test_preserves_structure_and_verifies_all_retained_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            root,raw,manifest,(proof,units)=await self.extract(folder)
            self.assertEqual(manifest['units'][0]['text'],'Original complete text')
            self.assertEqual(len(proof['frames']),2) # rendered page AND embedded image
            self.assertEqual(verify_office_visuals(root/'raw',proof,hashlib.sha256(raw).hexdigest(),manifest),units)
            self.assertTrue(any('Chart value 42' in u['text'] for u in units))
            for unit in units:
                self.assertEqual(unit['anchors']['section'],'office_visual')
                self.assertNotIn('page_number',unit['anchors']);self.assertNotIn('bbox',unit['anchors'])
                self.assertTrue(unit['anchors']['source_scope']['unit_ids'])

    async def test_visual_frame_avoids_duplicate_plain_page_ocr(self):
        with tempfile.TemporaryDirectory() as folder:
            root,raw,manifest,(proof,units)=await self.extract(folder,runner=lambda p:[{'page_number':1,'text':'Original complete text'}])
            self.assertEqual(len(units),1) # only independently processed embedded image OCR
            self.assertEqual(units[0]['text'],'Original complete text')

    async def test_tampered_derivative_and_structural_binding_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root,raw,manifest,(proof,units)=await self.extract(folder)
            bad=copy.deepcopy(manifest);bad['units'][0]['text']='forged'
            with self.assertRaises(Conflict):verify_office_visuals(root/'raw',proof,hashlib.sha256(raw).hexdigest(),bad)
            bad=copy.deepcopy(proof);bad['frames'].pop()
            with self.assertRaises(Conflict):verify_office_visuals(root/'raw',bad,hashlib.sha256(raw).hexdigest(),manifest)
            from urllib.parse import urlparse
            Path(urlparse(proof['rendered_pdf']['uri']).path).write_bytes(b'tampered')
            with self.assertRaises(Conflict):verify_office_visuals(root/'raw',proof,hashlib.sha256(raw).hexdigest(),manifest)

    async def test_historical_native_contract_survives_current_model_rotation(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as folder:
            root,raw,manifest,(proof,units)=await self.extract(folder)
            with patch('application.image_extraction.image_contract',return_value=('sha256:'+'f'*64,'sha256:'+'e'*64)):
                self.assertEqual(verify_office_visuals(root/'raw',proof,hashlib.sha256(raw).hexdigest(),manifest),units)
            tampered=copy.deepcopy(proof)
            tampered['native']['config']['dpi']=600
            with self.assertRaises(Conflict):
                verify_office_visuals(root/'raw',tampered,hashlib.sha256(raw).hexdigest(),manifest)

    async def test_empty_original_presentation_has_no_invented_visual_page(self):
        p='http://schemas.openxmlformats.org/presentationml/2006/main'
        output=BytesIO()
        with zipfile.ZipFile(output,'w') as package:
            package.writestr('ppt/presentation.xml',f'<p:presentation xmlns:p="{p}"/>')
        raw=output.getvalue();parsed=inspect_document('empty.pptx',raw)
        manifest={'schema_version':'native-source-units.v1','source_kind':'pptx','transform':parsed['transform'],'units':parsed['units']}
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);(root/'raw').mkdir();original=root/'raw'/'empty.pptx';original.write_bytes(raw)
            processor=OfficeVisualProcessor(root=root/'stages',renderer=lambda *_:self.fail('empty presentation rendered'),runner=lambda *_:self.fail('empty frame inferred'))
            proof,units=await processor.extract(raw,'empty.pptx',manifest,raw_uri=original.as_uri())
            self.assertEqual(units,[]);self.assertEqual(proof['frames'],[])
            self.assertIsNone(proof['rendered_pdf'])
            self.assertEqual(verify_office_visuals(root/'raw',proof,hashlib.sha256(raw).hexdigest(),manifest),[])

    async def test_external_image_rejected_before_renderer(self):
        raw=source(external=True)
        with self.assertRaisesRegex(ValueError,'[Ee]xternal'):
            office_inventory(raw,'source.docx',native_manifest(raw))

    async def test_split_and_nested_external_fields_fail_before_rendering(self):
        begin='<w:r><w:fldChar w:fldCharType="begin"/></w:r>'
        separate='<w:r><w:fldChar w:fldCharType="separate"/></w:r>'
        end='<w:r><w:fldChar w:fldCharType="end"/></w:r>'
        def instruction(text):return '<w:r><w:instrText>'+text+'</w:instrText></w:r>'
        harmless=begin+instruction('PAGE')+separate+'<w:r><w:t>1</w:t></w:r>'+end
        samples=[]
        for first,second in [('INCLUDE','TEXT'),('INCLUDE','PICTURE'),('DDE','AUTO'),('LI','NK'),('DD','E')]:
            danger=begin+instruction(first)+instruction(second+' &quot;file:///never-fetch-this&quot;')+separate+end
            samples.extend([danger,harmless+danger,begin+instruction('PAGE')+separate+danger+end])
        # A nested field can supply command characters: conservatively ambiguous.
        samples.append(begin+instruction('INCLUDE')+harmless+instruction('TEXT')+separate+end)
        # fldSimple instructions are full attributes, and malformed complex fields fail closed.
        samples.extend(['<w:fldSimple w:instr="INCLUDETEXT &quot;file:///never-fetch-this&quot;"/>',
            instruction('INCLUDE')+instruction('TEXT'),begin+instruction('PAGE'),end,
            begin+instruction('PAGE')+separate+instruction('TEXT')+end])
        for fields in samples:
            with self.subTest(fields=fields),tempfile.TemporaryDirectory() as folder:
                output=BytesIO()
                with zipfile.ZipFile(BytesIO(source())) as original,zipfile.ZipFile(output,'w') as target:
                    for name in original.namelist():
                        data=original.read(name)
                        if name=='word/document.xml':data=data.replace(b'</w:body>',('<w:p>'+fields+'</w:p></w:body>').encode())
                        target.writestr(name,data)
                raw=output.getvalue();root=Path(folder);(root/'raw').mkdir();path=root/'raw'/'original.docx';path.write_bytes(raw)
                processor=OfficeVisualProcessor(root=root/'stages',renderer=lambda *_:self.fail('renderer called for unsafe field'),runner=lambda *_:self.fail('native called for unsafe field'))
                with self.assertRaisesRegex(ValueError,'[Ff]ield'):
                    await processor.extract(raw,'source.docx',native_manifest(raw),raw_uri=path.as_uri())

    async def test_ordinary_page_fields_and_completed_result_nesting_remain_supported(self):
        begin='<w:r><w:fldChar w:fldCharType="begin"/></w:r>'
        separate='<w:r><w:fldChar w:fldCharType="separate"/></w:r>'
        end='<w:r><w:fldChar w:fldCharType="end"/></w:r>'
        page=begin+'<w:r><w:instrText>PA</w:instrText></w:r><w:r><w:instrText>GE</w:instrText></w:r>'+separate+'<w:r><w:t>1</w:t></w:r>'+end
        fields=page+'<w:fldSimple w:instr="NUMPAGES"><w:r><w:t>2</w:t></w:r></w:fldSimple>'+begin+'<w:r><w:instrText>PAGE</w:instrText></w:r>'+separate+page+end
        output=BytesIO()
        with zipfile.ZipFile(BytesIO(source())) as original,zipfile.ZipFile(output,'w') as target:
            for name in original.namelist():
                data=original.read(name)
                if name=='word/document.xml':data=data.replace(b'</w:body>',('<w:p>'+fields+'</w:p></w:body>').encode())
                target.writestr(name,data)
        raw=output.getvalue()
        with tempfile.TemporaryDirectory() as folder:
            _,_,_,(proof,_)=await self.extract(folder,raw=raw)
            self.assertEqual(proof['status'],'complete')

    async def test_native_nested_error_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(ValueError,'[Nn]ative'):
                await self.extract(folder,runner=lambda p:[{'metadata':{'error':{'message':'failed'}}}])


def presentation_source():
    """Hand-authored public OOXML with a native chart, diagram and hidden slide."""
    from tests.test_document_formats import fixtures
    original=fixtures()['fixture.pptx'];output=BytesIO()
    p='http://schemas.openxmlformats.org/presentationml/2006/main'
    a='http://schemas.openxmlformats.org/drawingml/2006/main'
    r='http://schemas.openxmlformats.org/officeDocument/2006/relationships'
    c='http://schemas.openxmlformats.org/drawingml/2006/chart'
    rel='http://schemas.openxmlformats.org/package/2006/relationships'
    shape=f'''<p:sp><p:nvSpPr><p:cNvPr id="2" name="Diagram rectangle"/><p:cNvSpPr/><p:nvPr/></p:nvSpPr><p:spPr><a:xfrm><a:off x="500000" y="500000"/><a:ext cx="3000000" cy="900000"/></a:xfrm><a:prstGeom prst="rect"><a:avLst/></a:prstGeom><a:solidFill><a:srgbClr val="00AACC"/></a:solidFill></p:spPr><p:txBody><a:bodyPr/><a:lstStyle/><a:p><a:r><a:rPr lang="en-US" sz="2800"/><a:t>Diagram Input to Output</a:t></a:r></a:p></p:txBody></p:sp>'''
    chart=f'''<p:graphicFrame><p:nvGraphicFramePr><p:cNvPr id="3" name="Native Chart"/><p:cNvGraphicFramePr/><p:nvPr/></p:nvGraphicFramePr><p:xfrm><a:off x="500000" y="1800000"/><a:ext cx="6000000" cy="4000000"/></p:xfrm><a:graphic><a:graphicData uri="{c}"><c:chart xmlns:c="{c}" r:id="chart1"/></a:graphicData></a:graphic></p:graphicFrame>'''
    tree=f'<p:nvGrpSpPr><p:cNvPr id="1" name=""/><p:cNvGrpSpPr/><p:nvPr/></p:nvGrpSpPr><p:grpSpPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="0" cy="0"/><a:chOff x="0" y="0"/><a:chExt cx="0" cy="0"/></a:xfrm></p:grpSpPr>{shape}{chart}'
    slide=f'<p:sld xmlns:p="{p}" xmlns:a="{a}" xmlns:r="{r}"><p:cSld><p:spTree>{tree}</p:spTree></p:cSld><p:clrMapOvr><a:masterClrMapping/></p:clrMapOvr></p:sld>'
    chart_xml=f'''<c:chartSpace xmlns:c="{c}" xmlns:a="{a}"><c:chart><c:autoTitleDeleted val="1"/><c:plotArea><c:layout/><c:barChart><c:barDir val="col"/><c:grouping val="clustered"/><c:ser><c:idx val="0"/><c:order val="0"/><c:tx><c:v>Revenue</c:v></c:tx><c:cat><c:strLit><c:ptCount val="2"/><c:pt idx="0"><c:v>Alpha</c:v></c:pt><c:pt idx="1"><c:v>Beta</c:v></c:pt></c:strLit></c:cat><c:val><c:numLit><c:formatCode>General</c:formatCode><c:ptCount val="2"/><c:pt idx="0"><c:v>42</c:v></c:pt><c:pt idx="1"><c:v>75</c:v></c:pt></c:numLit></c:val></c:ser><c:axId val="1"/><c:axId val="2"/></c:barChart><c:catAx><c:axId val="1"/><c:scaling><c:orientation val="minMax"/></c:scaling><c:axPos val="b"/><c:crossAx val="2"/><c:crosses val="autoZero"/></c:catAx><c:valAx><c:axId val="2"/><c:scaling><c:orientation val="minMax"/></c:scaling><c:axPos val="l"/><c:crossAx val="1"/><c:crosses val="autoZero"/></c:valAx></c:plotArea><c:plotVisOnly val="1"/></c:chart></c:chartSpace>'''
    with zipfile.ZipFile(BytesIO(original)) as source,zipfile.ZipFile(output,'w') as target:
        for name in source.namelist():
            data=source.read(name)
            if name=='ppt/presentation.xml':data=data.replace(b'</p:presentation>',b'<p:sldSz cx="9144000" cy="6858000"/><p:notesSz cx="6858000" cy="9144000"/></p:presentation>')
            if name=='ppt/slides/slide1.xml':data=slide.encode()
            if name=='ppt/slides/slide2.xml':data=slide.replace('<p:sld xmlns:', '<p:sld show="0" xmlns:').encode()
            if name=='[Content_Types].xml':data=data.replace(b'</Types>',b'<Override PartName="/ppt/charts/chart1.xml" ContentType="application/vnd.openxmlformats-officedocument.drawingml.chart+xml"/></Types>')
            if name=='ppt/slides/_rels/slide2.xml.rels':continue
            target.writestr(name,data)
        target.writestr('ppt/charts/chart1.xml',chart_xml)
        for number in (1,2):
            target.writestr(f'ppt/slides/_rels/slide{number}.xml.rels',f'<Relationships xmlns="{rel}"><Relationship Id="chart1" Type="{r}/chart" Target="../charts/chart1.xml"/></Relationships>')
    return output.getvalue()


class RealRendererTests(unittest.IsolatedAsyncioTestCase):
    async def test_actual_pinned_renderer_retains_hidden_native_charts_and_diagrams(self):
        from collection_control.office_visuals import RENDERER,sha
        binary=Path(RENDERER['binary_path'])
        if not binary.is_file() or sha(binary.read_bytes())!=RENDERER['binary_sha256']:
            self.skipTest('Exact renderer available in pinned application image')
        raw=presentation_source();parsed=inspect_document('source.pptx',raw)
        manifest={'schema_version':'native-source-units.v1','source_kind':'pptx','transform':parsed['transform'],'units':parsed['units']}
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);(root/'raw').mkdir();original=root/'raw'/'source.pptx';original.write_bytes(raw)
            paths=[]
            def native(path):
                paths.append(path)
                return [{'text':'full page OCR is retained','chart':[{'text':'Alpha 42 Beta 75'}]}]
            proof,units=await OfficeVisualProcessor(root=root/'stages',runner=native).extract(raw,'source.pptx',manifest,raw_uri=original.as_uri())
            self.assertEqual(len(proof['frames']),2)
            self.assertTrue(proof['inventory']['slides'][1]['anchors']['hidden'])
            self.assertEqual([u['anchors']['source_scope']['unit_ids'] for u in units],[['slide:1'],['slide:2']])
            import pypdfium2 as pdfium
            from urllib.parse import urlparse
            document=pdfium.PdfDocument(Path(urlparse(proof['rendered_pdf']['uri']).path))
            try:
                self.assertEqual(len(document),2)
                for index in range(2):
                    page=document[index];text=page.get_textpage()
                    try:
                        extracted=' '.join(text.get_text_bounded().split())
                        self.assertIn('Diagram Input to Output',extracted)
                        self.assertIn('Alpha',extracted);self.assertIn('Beta',extracted)
                    finally:text.close();page.close()
            finally:document.close()
            self.assertEqual(verify_office_visuals(root/'raw',proof,sha(raw),manifest),units)


def document_source():
    """Generated DOCX contains an inline native chart and an embedded PNG."""
    from tests.test_document_formats import fixtures
    w='http://schemas.openxmlformats.org/wordprocessingml/2006/main'
    a='http://schemas.openxmlformats.org/drawingml/2006/main'
    r='http://schemas.openxmlformats.org/officeDocument/2006/relationships'
    c='http://schemas.openxmlformats.org/drawingml/2006/chart'
    wp='http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing'
    pic='http://schemas.openxmlformats.org/drawingml/2006/picture'
    rel='http://schemas.openxmlformats.org/package/2006/relationships'
    inline=f'''<w:p><w:r><w:drawing><wp:inline xmlns:wp="{wp}"><wp:extent cx="5486400" cy="3657600"/><wp:docPr id="1" name="Native chart"/><a:graphic xmlns:a="{a}"><a:graphicData uri="{c}"><c:chart xmlns:c="{c}" xmlns:r="{r}" r:id="chart1"/></a:graphicData></a:graphic></wp:inline></w:drawing></w:r></w:p>'''
    image=f'''<w:p><w:r><w:drawing><wp:inline xmlns:wp="{wp}"><wp:extent cx="914400" cy="685800"/><wp:docPr id="2" name="Embedded raster"/><a:graphic xmlns:a="{a}"><a:graphicData uri="{pic}"><pic:pic xmlns:pic="{pic}"><pic:nvPicPr><pic:cNvPr id="0" name="image.png"/><pic:cNvPicPr/></pic:nvPicPr><pic:blipFill><a:blip xmlns:r="{r}" r:embed="image1"/><a:stretch><a:fillRect/></a:stretch></pic:blipFill><pic:spPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="914400" cy="685800"/></a:xfrm><a:prstGeom prst="rect"><a:avLst/></a:prstGeom></pic:spPr></pic:pic></a:graphicData></a:graphic></wp:inline></w:drawing></w:r></w:p>'''
    diagram='<w:p><w:r><w:pict xmlns:v="urn:schemas-microsoft-com:vml"><v:rect style="width:240pt;height:40pt" fillcolor="#00AACC"><v:textbox><w:txbxContent><w:p><w:r><w:t>DOCX Diagram Input to Output</w:t></w:r></w:p></w:txbxContent></v:textbox></v:rect></w:pict></w:r></w:p>'
    output=BytesIO();picture=BytesIO();Image.new('RGB',(32,24),'red').save(picture,format='PNG')
    with zipfile.ZipFile(BytesIO(presentation_source())) as deck:chart=deck.read('ppt/charts/chart1.xml')
    with zipfile.ZipFile(BytesIO(fixtures()['fixture.docx'])) as original,zipfile.ZipFile(output,'w') as target:
        for name in original.namelist():
            data=original.read(name)
            if name=='word/document.xml':data=data.replace(b'</w:body>',(inline+image+diagram+'</w:body>').encode())
            if name=='[Content_Types].xml':data=data.replace(b'</Types>',b'<Default Extension="png" ContentType="image/png"/><Override PartName="/word/charts/chart1.xml" ContentType="application/vnd.openxmlformats-officedocument.drawingml.chart+xml"/></Types>')
            target.writestr(name,data)
        target.writestr('word/charts/chart1.xml',chart)
        target.writestr('word/media/image.png',picture.getvalue())
        target.writestr('word/_rels/document.xml.rels',f'<Relationships xmlns="{rel}"><Relationship Id="chart1" Type="{r}/chart" Target="charts/chart1.xml"/><Relationship Id="image1" Type="{r}/image" Target="media/image.png"/></Relationships>')
    return output.getvalue()


class RealDocumentRendererTests(unittest.IsolatedAsyncioTestCase):
    async def test_actual_docx_chart_diagram_and_embedded_raster(self):
        from collection_control.office_visuals import RENDERER,sha
        binary=Path(RENDERER['binary_path'])
        if not binary.is_file() or sha(binary.read_bytes())!=RENDERER['binary_sha256']:
            self.skipTest('Exact renderer available in pinned application image')
        raw=document_source();manifest=native_manifest(raw)
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);(root/'raw').mkdir();original=root/'raw'/'source.docx';original.write_bytes(raw)
            proof,units=await OfficeVisualProcessor(root=root/'stages',runner=lambda p:[{'text':'embedded OCR','chart':[{'text':'Alpha 42 Beta 75'}]}]).extract(raw,'source.docx',manifest,raw_uri=original.as_uri())
            self.assertTrue(any(f['kind']=='embedded_media' for f in proof['frames']))
            self.assertTrue(all(f['source']['precision']=='document' for f in proof['frames'] if f['kind']=='rendered_page'))
            import pypdfium2 as pdfium
            from urllib.parse import urlparse
            document=pdfium.PdfDocument(Path(urlparse(proof['rendered_pdf']['uri']).path))
            try:
                texts=[]
                for index in range(len(document)):
                    page=document[index];text=page.get_textpage()
                    try:texts.append(text.get_text_bounded())
                    finally:text.close();page.close()
                text=' '.join('\n'.join(texts).split())
                self.assertIn('DOCX_MAIN_SENTINEL',text);self.assertIn('Alpha',text);self.assertIn('Beta',text)
                self.assertIn('DOCX Diagram Input to Output',text)
            finally:document.close()
            self.assertEqual(verify_office_visuals(root/'raw',proof,sha(raw),manifest),units)
