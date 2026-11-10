import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock,patch
from application.worker import ApplicationWorker
from collection_control.repository import Conflict
class CaptionParentGateTests(unittest.IsolatedAsyncioTestCase):
 async def test_nonempty_parent_waits_before_loading_any_collection_proof(self):
  e=SimpleNamespace(idempotency_key='caption',job_id='job',document_id='document',artifact_uri='file:///caption')
  from nv_ingest_durable.contracts import Stage
  e.stage=Stage.INDEX
  worker=ApplicationWorker.__new__(ApplicationWorker)
  worker.pool=SimpleNamespace(fetchrow=AsyncMock(side_effect=[
   {'parent_job_id':'job','document_id':'document','collection_id':'collection'},
   {'status':'committed','stage':'caption','artifact_uri':e.artifact_uri},
   {'deleted':False},
   {'web_role':'binary','status':'admitted','records':17}]))
  worker._yield_to_ready_pages=AsyncMock()
  with patch('application.worker.CaptionCollectionRepository') as bridge:
   with self.assertRaisesRegex(Conflict,'parent document publication'):await worker.caption(SimpleNamespace(envelope=e))
   bridge.assert_not_called()

 async def test_nonempty_capture_does_not_enter_empty_source_fallback(self):
  from nv_ingest_durable.contracts import Stage
  e=SimpleNamespace(idempotency_key='caption',job_id='job',document_id='document',artifact_uri='file:///caption',stage=Stage.INDEX)
  worker=ApplicationWorker.__new__(ApplicationWorker)
  worker.pool=SimpleNamespace(fetchrow=AsyncMock(side_effect=[
   {'parent_job_id':'job','document_id':'document','collection_id':'collection'},
   {'status':'committed','stage':'caption','artifact_uri':e.artifact_uri},
   {'deleted':False},{'web_role':'binary','status':'captured','records':17}]))
  worker._yield_to_ready_pages=AsyncMock()
  with patch('application.worker.CaptionCollectionRepository') as bridge:
   with self.assertRaisesRegex(Conflict,'parent document publication'):await worker.caption(SimpleNamespace(envelope=e))
   bridge.assert_not_called()
 async def test_superseded_published_source_is_durably_retired_without_loading_proofs(self):
  from nv_ingest_durable.contracts import Stage
  e=SimpleNamespace(idempotency_key='caption',job_id='job',document_id='document',artifact_uri='file:///caption',stage=Stage.INDEX)
  worker=ApplicationWorker.__new__(ApplicationWorker)
  worker.pool=SimpleNamespace(execute=AsyncMock(),fetchrow=AsyncMock(side_effect=[
   {'parent_job_id':'job','document_id':'document','collection_id':'collection'},
   {'status':'committed','stage':'caption','artifact_uri':e.artifact_uri},
   {'deleted':False},{'web_role':'binary','status':'published','records':17,'source_revision_id':'old','current_revision_id':'new'}]))
  worker._yield_to_ready_pages=AsyncMock()
  with patch('application.worker.CaptionCollectionRepository') as bridge:
   await worker.caption(SimpleNamespace(envelope=e));bridge.assert_not_called()
  worker.pool.execute.assert_awaited_once()
  self.assertIn('SupersededSource',worker.pool.execute.call_args.args[0])
 async def test_caption_batch_advances_across_waiting_dependencies_without_parallel_handlers(self):
  worker=ApplicationWorker.__new__(ApplicationWorker);worker.consume_once=AsyncMock(return_value=0)
  await worker.run_once('caption-admission')
  self.assertEqual(worker.consume_once.call_args.kwargs['batch'],32)

 async def test_deferred_image_waits_before_loading_native_parent_graph(self):
  from nv_ingest_durable.contracts import Stage
  e=SimpleNamespace(idempotency_key='caption',job_id='job',document_id='document',artifact_uri='file:///caption',stage=Stage.INDEX)
  worker=ApplicationWorker.__new__(ApplicationWorker)
  worker.pool=SimpleNamespace(fetchrow=AsyncMock(side_effect=[
   {'parent_job_id':'job','document_id':'document','collection_id':'collection'},
   {'status':'committed','stage':'caption','artifact_uri':e.artifact_uri},
   {'deleted':False},{'web_role':'image','status':'queued','records':0,'web_asset_download_id':'download'}]))
  worker._yield_to_ready_pages=AsyncMock()
  with patch('collection_control.web_captions.WebCaptionRepository') as bridge:
   with self.assertRaisesRegex(Conflict,'native image publication'):await worker.caption(SimpleNamespace(envelope=e))
   bridge.assert_not_called()

 async def test_legacy_image_with_no_current_or_pending_parent_is_retired(self):
  from nv_ingest_durable.contracts import Stage
  e=SimpleNamespace(idempotency_key='caption',job_id='job',document_id='document',artifact_uri='file:///caption',stage=Stage.INDEX)
  worker=ApplicationWorker.__new__(ApplicationWorker)
  worker.pool=SimpleNamespace(execute=AsyncMock(),fetchrow=AsyncMock(side_effect=[
   {'parent_job_id':'job','document_id':'document','collection_id':'collection'},
   {'status':'committed','stage':'caption','artifact_uri':e.artifact_uri},
   {'deleted':False},{'web_role':'image','status':'captured','records':0,'web_group_id':'legacy','web_asset_download_id':None},
   {'has_parents':False,'relevant':False}]))
  worker._yield_to_ready_pages=AsyncMock()
  with patch('collection_control.web_captions.WebCaptionRepository') as bridge:
   await worker.caption(SimpleNamespace(envelope=e));bridge.assert_not_called()
  self.assertIn('SupersededSource',worker.pool.execute.call_args.args[0])
