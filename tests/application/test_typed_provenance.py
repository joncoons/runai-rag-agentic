"""Generated typed source units; no parser, model, database or store effects."""
import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from application.provenance import enrich_checkpoint
from collection_control.artifacts import canonical
from collection_control.provenance import checkpoint_records
from collection_control.repository import Conflict


def sha(raw):return hashlib.sha256(raw).hexdigest()


class TypedProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.raw=self.root/'source.txt';self.raw.write_bytes(b'Full retained source')
        self.kwargs=dict(raw_root=self.root,model_digest='e'*64,config_digest='c'*64,ocr_model_digest='a'*64)
        self.contract=dict(dimensions=2048,dtype='float32',normalization='unit',metric='IP',schema_version=1,embedding_checkpoint_sha256='e'*64)
        self.revision=dict(source_id='source',source_revision_id='revision',collection_id='collection',document_id='document',job_id='job',canonical_uri='https://capture.invalid/source',raw_sha256=sha(self.raw.read_bytes()),raw_uri=self.raw.as_uri(),observed_at='2026-11-08T12:00:00Z',ingestion_run_id='run',filename='source.txt',source_kind='txt')
        self.units=dict(schema_version='native-source-units.v1',source_kind='txt',transform={'name':'text_lines','version':'fixture-v1'},units=[{'unit_id':'line:1','anchors':{'line_start':1,'line_end':1},'status':'complete','text':'Full retained source'},{'unit_id':'line:2','anchors':{'line_start':2,'line_end':2},'status':'complete','text':''},{'unit_id':'line:3','anchors':{'line_start':3,'line_end':3},'status':'excluded','reason':'non-text asset'}])
        self.checkpoint=dict(schema_version='1.0',payload_format='es_docs',native_source_units=copy.deepcopy(self.units),documents=[dict(text='Full retained source',vector=[1.0]+[0.0]*2047,metadata={'source':{'native':'preserve'},'content_metadata':{'source_unit_id':'line:1'},'custom':'preserve'})])
        self.revision['source_units']=copy.deepcopy(self.units)

    def enrich(self,checkpoint=None,revision=None):
        return enrich_checkpoint(checkpoint if checkpoint is not None else self.checkpoint,revision if revision is not None else self.revision,**self.kwargs)

    def validate(self,enriched,manifest):
        raw=canonical(manifest);path=self.root/(sha(raw)+'.json');path.write_bytes(raw)
        return checkpoint_records(enriched,SimpleNamespace(document_id='document',model_digest='e'*64,config_digest='c'*64),{'source_uri':self.revision['canonical_uri'],'source_digest':self.revision['raw_sha256']},{'metadata':{'acl':['local-operator'],'ingestion_run_id':'run','source_manifests':{'document':{'uri':path.as_uri(),'sha256':sha(raw)}}}},self.contract,self.root)

    def test_typed_units_preserve_text_vectors_anchors_and_complete_coverage(self):
        before=copy.deepcopy(self.checkpoint);enriched,manifest=self.enrich()
        records,_,_=self.validate(enriched,manifest)
        self.assertEqual(self.checkpoint,before)
        self.assertEqual(enriched['documents'][0]['vector'],before['documents'][0]['vector'])
        self.assertEqual(records[0]['metadata']['provenance']['anchors'],self.units['units'][0]['anchors'])
        self.assertNotIn('page_number',records[0]['metadata']['content_metadata'])
        self.assertEqual(records[0]['metadata']['native_source'],{'native':'preserve'})
        self.assertEqual(records[0]['metadata']['custom'],'preserve')
        self.assertEqual(manifest['expected_units'],['line:1','line:2','line:3'])
        self.assertEqual(manifest['native_source_units'],self.units)
        self.assertEqual(enriched['coverage']['completed_units']['line:2'],[])
        self.assertEqual(enriched['coverage']['completed_units']['line:3'],[])
        transforms=records[0]['metadata']['provenance']['transforms']
        self.assertEqual(transforms[0],{'component':'document_extraction','name':'text_lines','version':'fixture-v1','config_digest':'c'*64})
        self.assertEqual(transforms[-1]['version'],'26.8.2')
        self.assertEqual(self.enrich()[0],enriched)
        encoded={**self.revision,'source_units':json.dumps(self.units)}
        self.assertEqual(self.enrich(revision=encoded)[0],enriched)

    def test_persisted_units_are_authority_for_checkpoint_and_text(self):
        changes=[lambda cp:cp['native_source_units']['units'][0].update(text='invented'),lambda cp:cp['native_source_units']['transform'].update(version='different'),lambda cp:cp['documents'][0].update(text='truncated'),lambda cp:cp.update(documents=[]),lambda cp:cp['documents'].append(copy.deepcopy(cp['documents'][0])),lambda cp:cp['documents'][0]['metadata']['content_metadata'].update(source_unit_id='line:2')]
        for change in changes:
            with self.subTest(change=change):
                cp=copy.deepcopy(self.checkpoint);change(cp)
                with self.assertRaises(Conflict):self.enrich(cp)

    def test_invalid_unit_manifest_and_raw_bytes_fail_closed(self):
        for mutation in (lambda u:u['units'].append(copy.deepcopy(u['units'][0])),lambda u:u['units'][2].pop('reason'),lambda u:u['units'][0].update(status='failed'),lambda u:u['units'][0].update(anchors={'line_start':0,'line_end':1}),lambda u:u['units'][0].update(anchors={'page_number':1}),lambda u:u.update(source_kind='pdf')):
            units=copy.deepcopy(self.units);mutation(units)
            with self.subTest(units=units),self.assertRaises(Conflict):self.enrich({**self.checkpoint,'native_source_units':units},{**self.revision,'source_units':units})
        with self.assertRaises(Conflict):self.enrich(revision={**self.revision,'raw_sha256':'0'*64})

    def test_html_is_one_retained_whole_page_with_web_transform_lineage(self):
        units=dict(schema_version='native-source-units.v1',source_kind='html',transform={'name':'html_parser','version':'html-v1'},units=[{'unit_id':'page:1','anchors':{'section':'page'},'status':'complete','text':'Full retained source'}])
        cp={**self.checkpoint,'native_source_units':units,'documents':copy.deepcopy(self.checkpoint['documents'])};cp['documents'][0]['metadata']['content_metadata']['source_unit_id']='page:1'
        revision={**self.revision,'source_kind':'html','source_units':units}
        enriched,manifest=self.enrich(cp,revision);records,_,_=self.validate(enriched,manifest)
        p=records[0]['metadata']['provenance'];self.assertEqual(p['source_system'],'web_crawl');self.assertEqual(p['modality'],'text')
        self.assertEqual([t['component'] for t in p['transforms']],['html_parser','page_unit','embedding'])
        self.assertEqual(p['anchors'],{'section':'page'})
        bad=copy.deepcopy(units);bad['units'][0]['anchors']={'heading':'fragment'}
        with self.assertRaises(Conflict):self.enrich({**cp,'native_source_units':bad},{**revision,'source_units':bad})

    def test_blank_and_excluded_only_sources_retain_explicit_zero_record_coverage(self):
        units=copy.deepcopy(self.units);units['units']=units['units'][1:]
        enriched,manifest=self.enrich({**self.checkpoint,'documents':[],'native_source_units':units},{**self.revision,'source_units':units})
        self.assertEqual(enriched['coverage']['completed_units'],{'line:2':[],'line:3':[]})
        self.assertEqual(enriched['retained_source_capture']['source_manifest']['sha256'],sha(canonical(manifest)))

    def test_actual_format_anchor_matrix_reaches_strict_admission_without_pdf_pages(self):
        cases=[('docx',{'part':'word/document.xml','paragraph_index':1,'table_index':2,'cell_row':1,'cell_column':3}),('pptx',{'part':'ppt/notesSlides/notesSlide1.xml','slide_number':1,'hidden':False,'part_kind':'notes'}),('markdown',{'line_start':1,'line_end':2,'char_start':0,'char_end':20,'heading':'Intro'}),('csv',{'record_number':1,'line_start':1,'line_end':2}),('xlsx',{'part':'xl/worksheets/sheet1.xml','sheet_id':'7','sheet_name':'Data','sheet_state':'hidden','sheet_index':1,'cell':'C2','row':2,'column':3,'cell_type':'s','row_hidden':False}),('xls',{'sheet_index':1,'sheet_name':'Data','sheet_state':'visible','row':1,'column':2,'cell_type':1})]
        for kind,anchors in cases:
            with self.subTest(kind=kind):
                units={**copy.deepcopy(self.units),'source_kind':kind,'units':[{'unit_id':'retained:1','status':'complete','text':'Full retained source','anchors':anchors}]}
                cp={**self.checkpoint,'native_source_units':units,'documents':copy.deepcopy(self.checkpoint['documents'])};cp['documents'][0]['metadata']['content_metadata']['source_unit_id']='retained:1'
                enriched,manifest=self.enrich(cp,{**self.revision,'source_kind':kind,'source_units':units})
                records,_,_=self.validate(enriched,manifest)
                self.assertEqual(records[0]['metadata']['provenance']['anchors'],anchors)
                self.assertNotIn('page_number',records[0]['metadata']['content_metadata'])

    def test_complete_units_require_explicit_text_and_whitespace_stays_blank(self):
        units=copy.deepcopy(self.units);units['units'][0].pop('text')
        with self.assertRaises(Conflict):self.enrich({**self.checkpoint,'documents':[],'native_source_units':units},{**self.revision,'source_units':units})
        units['units'][0]['text']='  \n'
        enriched,manifest=self.enrich({**self.checkpoint,'documents':[],'native_source_units':units},{**self.revision,'source_units':units})
        self.assertEqual(enriched['coverage']['completed_units']['line:1'],[])
        self.assertEqual(manifest['native_source_units']['units'][0]['text'],'  \n')

    def test_strict_admission_rechecks_typed_record_unit_text_and_anchor_binding(self):
        enriched,manifest=self.enrich()
        for mutation in (lambda cp:cp['documents'][0]['metadata']['provenance']['anchors'].update(line_start=2,line_end=2),lambda cp:cp['documents'][0]['metadata']['content_metadata'].update(source_unit_id='line:2'),lambda cp:cp['native_source_units']['transform'].update(version='forged')):
            cp=copy.deepcopy(enriched);mutation(cp)
            with self.subTest(checkpoint=cp['native_source_units']),self.assertRaises(Conflict):self.validate(cp,manifest)

    def test_html_retained_acquisition_proof_is_verified_and_preserved(self):
        units=dict(schema_version='native-source-units.v1',source_kind='html',transform={'name':'html_parser','version':'html-v1'},units=[{'unit_id':'page:1','anchors':{'section':'page','heading_path':['page']},'status':'complete','text':'Full retained source'}])
        cp={**self.checkpoint,'native_source_units':units,'documents':copy.deepcopy(self.checkpoint['documents'])};cp['documents'][0]['metadata']['content_metadata']['source_unit_id']='page:1'
        evidence={'schema_version':'web-acquisition.v1','requested_uri':'https://capture.invalid/request','final_uri':self.revision['canonical_uri'],'raw_sha256':self.revision['raw_sha256'],'capture_complete':True,'http':{'status':200},'assets':[],'asset_processing':'discovery_only','deletion_authorized':False}
        raw=canonical(evidence);path=self.root/'acquisition.json';path.write_bytes(raw)
        acquisition={'uri':path.as_uri(),'sha256':sha(raw)}
        revision={**self.revision,'source_kind':'html','source_units':units,'acquisition':acquisition}
        enriched,manifest=self.enrich(cp,revision)
        proof={'artifact_uri':path.as_uri(),'sha256':sha(raw)}
        self.assertEqual(enriched['documents'][0]['metadata']['provenance']['acquisition'],proof)
        self.validate(enriched,manifest)
        path.write_bytes(raw+b' ')
        with self.assertRaises(Conflict):self.enrich(cp,revision)
        with self.assertRaises(Conflict):self.validate(enriched,manifest)
        for change in ({'capture_complete':False},{'raw_sha256':'0'*64},{'final_uri':'https://different.invalid/'},{'schema_version':'wrong'}):
            invalid=canonical({**evidence,**change});path.write_bytes(invalid)
            with self.subTest(change=change),self.assertRaises(Conflict):self.enrich(cp,{**revision,'acquisition':{'uri':path.as_uri(),'sha256':sha(invalid)}})

    def test_uploaded_html_accepts_jsonb_null_acquisition_without_inventing_http(self):
        units=dict(schema_version='native-source-units.v1',source_kind='html',transform={'name':'html_parser','version':'html-v1'},units=[{'unit_id':'page:1','anchors':{'section':'page'},'status':'complete','text':'Full retained source'}])
        cp={**self.checkpoint,'native_source_units':units,'documents':copy.deepcopy(self.checkpoint['documents'])};cp['documents'][0]['metadata']['content_metadata']['source_unit_id']='page:1'
        enriched,_=self.enrich(cp,{**self.revision,'source_kind':'html','source_units':json.dumps(units),'acquisition':'null'})
        self.assertNotIn('acquisition',enriched['documents'][0]['metadata']['provenance'])
