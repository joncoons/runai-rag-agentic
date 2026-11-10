import hashlib,tempfile,unittest
from pathlib import Path
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock,patch
from datetime import datetime,timedelta,timezone
from application.web_acquisition import AcquisitionPolicy,PageCapture,CrawlCapture
class RefreshTests(unittest.IsolatedAsyncioTestCase):
 def fixture(self):
  raw=b'<html><body>Public fixture</body></html>';page=PageCapture('https://fixture.invalid/a','https://fixture.invalid/a',raw,'sha256:'+hashlib.sha256(raw).hexdigest(),'Public fixture','',(),(),{'retrieved_at':'one'})
  return CrawlCapture((page,),(),True),AcquisitionPolicy(allowed_hosts=('fixture.invalid',),include_images=False)
 def test_observation_identity_ignores_http_timestamps_but_binds_bytes_and_processing(self):
  from application.source_refresh import capture_identity
  capture,policy=self.fixture();one=capture_identity(capture,(),policy,'engine1');two=capture_identity(replace(capture,pages=(replace(capture.pages[0],evidence={'retrieved_at':'two'}),)),(),policy,'engine1')
  self.assertEqual(one,two);self.assertNotEqual(one,capture_identity(capture,(),policy,'engine2'))
  self.assertNotEqual(one,capture_identity(replace(capture,pages=(replace(capture.pages[0],raw_bytes=b'changed',raw_sha256='sha256:'+hashlib.sha256(b'changed').hexdigest()),)),(),policy,'engine1'))
 def test_actual_native_image_model_and_normalization_contract_change_processing_identity(self):
  from application.source_refresh import processing_identity
  settings=SimpleNamespace(models={'models':{'embed':{'model':'embed'}}},contract={'embedding_checkpoint_sha256':'a'*64})
  with patch('application.image_extraction.image_contract',return_value=('model-one','normalizer-one')):one=processing_identity(settings)
  with patch('application.image_extraction.image_contract',return_value=('model-two','normalizer-one')):two=processing_identity(settings)
  with patch('application.image_extraction.image_contract',return_value=('model-one','normalizer-two')):three=processing_identity(settings)
  self.assertNotEqual(one,two);self.assertNotEqual(one,three)
 def test_observations_reuse_the_qualified_raw_file_suffix_and_keep_full_hash(self):
  from application.source_refresh import retain_observations
  capture,policy=self.fixture()
  with tempfile.TemporaryDirectory() as directory:
   observations,counts=retain_observations(Path(directory),capture,(),None)
   self.assertTrue(observations[0]['raw_uri'].endswith('.html'))
   self.assertEqual(observations[0]['raw_sha256'],hashlib.sha256(capture.pages[0].raw_bytes).hexdigest())
   self.assertEqual(counts['new'],1)
 def test_forged_page_hash_is_rejected(self):
  from application.source_refresh import capture_identity
  capture,policy=self.fixture()
  with self.assertRaises(ValueError):capture_identity(replace(capture,pages=(replace(capture.pages[0],raw_sha256='0'*64),)),(),policy,'engine')
 def test_selected_asset_coverage_cannot_be_dropped_before_unchanged_bypass(self):
  from application.source_refresh import capture_identity
  from application.web_acquisition import Asset
  capture,policy=self.fixture();p=replace(capture.pages[0],assets=(Asset('https://fixture.invalid/a.png','image',capture.pages[0].final_uri),))
  with self.assertRaises(ValueError):capture_identity(replace(capture,pages=(p,)),(),replace(policy,include_images=True),'engine')
 async def test_unchanged_skips_intake_only_with_current_published_baseline(self):
  from application.source_refresh import RefreshWorker,capture_identity
  capture,policy=self.fixture();tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup);ctx=SimpleNamespace(settings=SimpleNamespace(raw=Path(tmp.name)),pool=AsyncMock());registry=AsyncMock()
  run={'run_id':'run','fence':1,'spec':{'seed_urls':['https://fixture.invalid/a'],'policy':{'allowed_hosts':['fixture.invalid'],'include_images':False}},'baseline':{'fingerprint':capture_identity(capture,(),policy,'engine'),'job_id':'job','observations':[]},'deadline':datetime.now(timezone.utc)+timedelta(seconds=100),'lease_until':datetime.now(timezone.utc)+timedelta(seconds=100),'collection_id':'collection'}
  worker=RefreshWorker(ctx,registry=registry)
  with patch('application.source_refresh.processing_identity',return_value='engine'),patch('application.source_refresh.collect',AsyncMock(return_value=capture)),patch('application.source_refresh.acquire_assets',AsyncMock(return_value=())),patch.object(worker,'finish_unchanged',AsyncMock(return_value=True)) as finish,patch('application.source_refresh.submit_web',AsyncMock()) as submit:
   await worker.process(run);submit.assert_not_awaited();finish.assert_awaited_once()
   worker.baseline_current=AsyncMock(return_value=False)
  # A baseline mismatch must enqueue a new group even with identical checksums.
  with patch('application.source_refresh.processing_identity',return_value='engine'),patch('application.source_refresh.collect',AsyncMock(return_value=capture)),patch('application.source_refresh.acquire_assets',AsyncMock(return_value=())),patch.object(worker,'finish_unchanged',AsyncMock(return_value=False)),patch('application.source_refresh.submit_web',AsyncMock(return_value='new-job')) as submit:
   await worker.process(run);submit.assert_awaited_once();self.assertEqual(submit.await_args.kwargs['_refresh']['run_id'],'run')
 async def test_incomplete_scan_never_accepts_or_claims_unchanged(self):
  from application.source_refresh import RefreshWorker
  capture,policy=self.fixture();registry=AsyncMock();ctx=SimpleNamespace(pool=AsyncMock());worker=RefreshWorker(ctx,registry=registry);run={'run_id':'run','fence':1,'spec':{'seed_urls':['https://fixture.invalid/a'],'policy':{'allowed_hosts':['fixture.invalid']}},'deadline':datetime.now(timezone.utc)+timedelta(seconds=100),'lease_until':datetime.now(timezone.utc)+timedelta(seconds=100)}
  with patch('application.source_refresh.collect',AsyncMock(return_value=replace(capture,complete=False))),patch('application.source_refresh.submit_web',AsyncMock()) as submit:
   await worker.process(run);submit.assert_not_awaited();self.assertEqual(registry.finish.await_args.kwargs['status'],'failed')
