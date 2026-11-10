import copy,hashlib,json,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from application.provenance import enrich_checkpoint
from collection_control.provenance import checkpoint_records
from collection_control.artifacts import canonical
class OfficeAdmissionTests(unittest.IsolatedAsyncioTestCase):
 async def test_original_and_supplemental_visual_units_admit_together(self):
  from tests.application import test_office_visuals as fixtures
  with tempfile.TemporaryDirectory() as folder:
   helper=fixtures.OfficeVisualTests();root,raw,native,(proof,visuals)=await helper.extract(folder);self.assertTrue(visuals)
   uri=(root/'raw/original.docx').as_uri();digest=hashlib.sha256(raw).hexdigest();revision=dict(source_id='source',source_revision_id='revision',collection_id='c',document_id='doc',job_id='job',canonical_uri='upload://c/source.docx',raw_sha256=digest,raw_uri=uri,observed_at='2026-11-08T12:00:00Z',ingestion_run_id='run',filename='source.docx',source_kind='docx',source_units=native,office_visuals_required=True)
   units=[u for u in native['units']+visuals if u['status']=='complete' and u.get('text','').strip()];checkpoint={'schema_version':'1.0','payload_format':'es_docs','native_source_units':copy.deepcopy(native),'office_visuals':proof,'documents':[{'text':u['text'],'vector':[1.0]+[0.0]*2047,'metadata':{'source':{},'content_metadata':{'source_unit_id':u['unit_id']}}} for u in units]}
   enriched,manifest=enrich_checkpoint(checkpoint,revision,raw_root=root/'raw',model_digest='e'*64,config_digest='c'*64,ocr_model_digest='a'*64);data=canonical(manifest);sha=hashlib.sha256(data).hexdigest();path=root/'raw'/(sha+'.json');path.write_bytes(data)
   job={'metadata':{'acl':['local-operator'],'ingestion_run_id':'run','ingestion_options':{'office_visuals_enabled':True},'source_manifests':{'doc':{'uri':path.as_uri(),'sha256':sha}}}};contract={'dimensions':2048,'dtype':'float32','normalization':'unit','metric':'IP','schema_version':1,'embedding_checkpoint_sha256':'e'*64}
   records,_,_=checkpoint_records(enriched,SimpleNamespace(document_id='doc',model_digest='e'*64,config_digest='c'*64),{'source_uri':revision['canonical_uri'],'source_digest':digest},job,contract,root/'raw');self.assertEqual(len(records),len(units));self.assertEqual(manifest['native_source_units'],native)
