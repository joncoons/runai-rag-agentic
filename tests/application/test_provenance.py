"""Pure generated intake fixtures; no services, user PDFs, or model calls."""
import copy
import hashlib
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from collection_control.artifacts import canonical
from collection_control.provenance import checkpoint_records
from collection_control.repository import Conflict
from application.provenance import enrich_checkpoint


def sha(data):
    return hashlib.sha256(data).hexdigest()


class EnrichCheckpointTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        raw = b'%PDF-1.7\n% generated provenance fixture, not parsed\n%%EOF\n'
        self.raw = self.root / 'fixture.pdf'
        self.raw.write_bytes(raw)
        self.revision = dict(source_id='capture-source', source_revision_id='capture-revision',
            previous_source_revision_id='previous-capture', collection_id='collection',
            document_id='document', job_id='job', canonical_uri='https://intake.invalid/fixture.pdf',
            raw_sha256=sha(raw), raw_uri=self.raw.as_uri(),
            observed_at=datetime(2026, 11, 8, tzinfo=timezone.utc),
            ingestion_run_id='intake-run', page_count=3, filename='fixture.pdf')
        self.kwargs = dict(raw_root=self.root, model_digest='e'*64,
            config_digest='c'*64, ocr_model_digest='a'*64)
        self.contract = dict(dimensions=2048, dtype='float32', normalization='unit', metric='IP',
            schema_version=1, embedding_checkpoint_sha256='e'*64)
        self.checkpoint = dict(schema_version='1.0', payload_format='es_docs',
            native_page_completions={'schema_version':'native-page-completions.v1','pages':[{'page_number':n,'status':'complete'} for n in (1,2,3)]},
            documents=[self.doc('Observed source text', 1)],
            caption_jobs=[{'id':'pending-asset', 'status':'queued'}], caption_status={'pending':1})

    def doc(self, text, page, kind='text', **metadata):
        return dict(text=text, vector=[1.0]+[0.0]*2047, metadata=dict(
            source={'source_id':'native-id', 'source_uri':'original.pdf'},
            content_metadata={'type':kind, 'page_number':page}, custom='retained', **metadata))

    def enrich(self, checkpoint=None, revision=None):
        return enrich_checkpoint(checkpoint or self.checkpoint, revision or self.revision, **self.kwargs)

    def validate(self, checkpoint, manifest):
        raw=canonical(manifest); manifest_sha=sha(raw)
        retained=self.root/(manifest_sha+'.json');retained.write_bytes(raw)
        return checkpoint_records(checkpoint, SimpleNamespace(document_id='document',
            model_digest='e'*64, config_digest='c'*64),
            {'source_uri':self.revision['canonical_uri'], 'source_digest':self.revision['raw_sha256']},
            {'metadata':{'acl':['local-operator'], 'ingestion_run_id':'intake-run',
                'source_manifests':{'document':{'uri':retained.as_uri(), 'sha256':manifest_sha}}}},
            self.contract, self.root)

    def test_strict_validator_accepts_new_capture_and_complete_page_coverage(self):
        before=copy.deepcopy(self.checkpoint)
        enriched, manifest=self.enrich()
        records, identity, _=self.validate(enriched, manifest)
        self.assertEqual(identity[1], 'capture-revision')
        self.assertEqual(len(records), 1)
        m=records[0]['metadata'];p=m['provenance']
        self.assertEqual(m['native_source'], before['documents'][0]['metadata']['source'])
        self.assertEqual(m['custom'], 'retained')
        self.assertEqual(m['source_id'], 'capture-source')
        self.assertEqual(p['identity_origin'], 'new_capture')
        self.assertEqual(p['previous_source_revision_id'], 'previous-capture')
        self.assertEqual(m['content_metadata']['extraction_method'], 'text_layer')
        self.assertEqual(enriched['documents'][0]['vector'], before['documents'][0]['vector'])
        self.assertEqual(self.checkpoint, before)
        self.assertEqual(enriched['caption_jobs'], before['caption_jobs'])
        self.assertEqual(enriched['caption_status'], before['caption_status'])
        self.assertEqual(manifest['expected_units'], ['page:1','page:2','page:3'])
        self.assertEqual(enriched['coverage']['completed_units']['page:2'], [])
        self.assertEqual(enriched['coverage']['completed_units']['page:3'], [])
        self.assertEqual(enriched['coverage']['manifest_sha256'], sha(canonical(manifest)))

    def test_ocr_scan_and_regions_have_model_specific_lineage(self):
        docs=[self.doc('Scan',1,needs_ocr_for_text=True,has_text=False)]
        docs += [self.doc(kind,2,kind) for kind in ('table','chart','infographic')]
        checkpoint={**self.checkpoint,'documents':docs}
        enriched,manifest=self.enrich(checkpoint)
        self.validate(enriched,manifest)
        for doc in enriched['documents']:
            self.assertEqual(doc['metadata']['content_metadata']['extraction_method'],'nemotron_ocr')
            transforms=doc['metadata']['provenance']['transforms']
            self.assertEqual(next(t for t in transforms if t['component']=='ocr')['model_digest'],'a'*64)

    def test_chunk_ids_deterministic_distinct_and_revision_bound(self):
        checkpoint={**self.checkpoint,'documents':[self.doc('Same',1),self.doc('Same',1)]}
        first,_=self.enrich(checkpoint);second,_=self.enrich(checkpoint)
        ids=lambda cp:[d['metadata']['source_chunk_id'] for d in cp['documents']]
        self.assertEqual(ids(first),ids(second));self.assertEqual(len(set(ids(first))),2)
        other,_=self.enrich(checkpoint,{**self.revision,'source_revision_id':'next-capture'})
        self.assertTrue(set(ids(first)).isdisjoint(ids(other)))

    def test_authoritative_fields_replace_native_aliases_and_bbox_is_preserved(self):
        native=self.doc('Box',2,source_id='old',source_revision_id='old',origin='synthetic',acl=['old'])
        native['metadata']['content_metadata'].update(bbox_xyxy_norm=[0.1,0.2,0.8,0.9])
        native['metadata']['content_sha256']='0'*64
        enriched,manifest=self.enrich({**self.checkpoint,'documents':[native]})
        records,_,_=self.validate(enriched,manifest)
        self.assertEqual(records[0]['metadata']['provenance']['anchors']['bbox'],[0.1,0.2,0.8,0.9])
        self.assertEqual(records[0]['metadata']['origin'],'grounded')

    def test_empty_checkpoint_retains_complete_empty_pages_without_text(self):
        enriched,manifest=self.enrich({**self.checkpoint,'documents':[]})
        self.assertEqual(enriched['documents'],[])
        self.assertEqual(enriched['coverage']['completed_units'],{'page:1':[],'page:2':[],'page:3':[]})
        self.assertEqual(manifest['coverage'],'complete')
        capture=enriched['retained_source_capture']
        self.assertEqual(capture['source_revision_id'],'capture-revision')
        self.assertEqual(capture['raw_artifact_uri'],self.raw.as_uri())
        self.assertEqual(capture['source_manifest']['sha256'],sha(canonical(manifest)))
        self.assertEqual(capture['source_manifest']['uri'],
            (self.root/(sha(canonical(manifest))+'.json')).as_uri())

    def test_raw_hash_path_timezone_and_anchor_fail_closed(self):
        cases=[{'raw_sha256':'0'*64},{'observed_at':'2026-11-08T12:00:00'},
            {'raw_uri':(self.root.parent/'outside.pdf').as_uri()},{'page_count':0}]
        for change in cases:
            with self.subTest(change=change),self.assertRaises(Conflict):
                self.enrich(revision={**self.revision,**change})
        for page in (None,0,True,4):
            with self.subTest(page=page),self.assertRaises(Conflict):
                self.enrich({**self.checkpoint,'documents':[self.doc('Invalid',page)]})
        native=self.doc('Box',1);native['metadata']['content_metadata']['bbox']=[1,0,0,1]
        with self.assertRaises(Conflict):self.enrich({**self.checkpoint,'documents':[native]})

    def test_generated_captions_cannot_be_relabelled_grounded(self):
        for kind in ('image_caption','infographic_caption'):
            with self.subTest(kind=kind),self.assertRaises(Conflict):
                self.enrich({**self.checkpoint,'documents':[self.doc('Generated caption',1,kind)]})

    def test_exact_observation_string_and_ocr_flags_in_content_metadata(self):
        native=self.doc('Observed OCR',1)
        native['metadata']['content_metadata'].update(needs_ocr_for_text=True,has_text=False)
        revision={**self.revision,'observed_at':'2026-11-08T03:15:22.123-06:00'}
        enriched,manifest=self.enrich({**self.checkpoint,'documents':[native]},revision)
        self.validate(enriched,manifest)
        m=enriched['documents'][0]['metadata']
        self.assertEqual(m['provenance']['observed_at'],revision['observed_at'])
        self.assertEqual(m['content_metadata']['extraction_method'],'nemotron_ocr')

    def test_symlink_raw_artifact_is_refused(self):
        link=self.root/'linked.pdf';link.symlink_to(self.raw)
        with self.assertRaises(Conflict):self.enrich(revision={**self.revision,'raw_uri':link.as_uri()})

    def test_missing_duplicate_failed_or_unobserved_native_pages_fail_closed(self):
        for pages in ([],[{'page_number':1,'status':'complete'}],
                      [{'page_number':n,'status':'complete'} for n in (1,1,3)],
                      [{'page_number':n,'status':'complete'} for n in (1,2,4)],
                      [{'page_number':1,'status':'complete'},{'page_number':2,'status':'failed'},{'page_number':3,'status':'complete'}]):
            with self.subTest(pages=pages),self.assertRaises(Conflict):
                self.enrich({**self.checkpoint,'native_page_completions':{'schema_version':'native-page-completions.v1','pages':pages}})
        checkpoint=copy.deepcopy(self.checkpoint);del checkpoint['native_page_completions']
        with self.assertRaises(Conflict):self.enrich(checkpoint)
