"""Actual custom compaction/notification contracts with generated inputs."""
import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from nv_ingest_durable.contracts import Stage,StageEnvelope
from nv_ingest_durable.elasticsearch_writer import compact_nrl_rows,EsReadyCheckpointStager


class CompactLineageTests(unittest.TestCase):
    def test_caption_acl_proof_native_source_and_region_survive_compaction(self):
        metadata={'source':{'source_id':'retained-native-id','source_revision_id':'revision-from-intake','source_uri':'upload://collection/generated-fixture.pdf'},'acl':['qualification-operator'],'caption_provenance':{'asset_sha256':'a'*64,'source_context':{'ingestion_run_id':'actual-capture-id'}},'has_text':False,'needs_ocr_for_text':True,'embedding':[1.0]+[0.0]*2047,'content_metadata':{'generated_annotation':True,'type':'image_caption'},'native_extra':{'keep':'unaltered'}}
        row={'text':'Generated caption fixture','metadata':metadata,'_page_number':4,'_bbox_xyxy_norm':[.1,.2,.6,.8]}
        before=copy.deepcopy(row)
        result=compact_nrl_rows([row],'fixture-document')[0]
        self.assertEqual(result['metadata']['caption_provenance'],metadata['caption_provenance'])
        self.assertEqual(result['metadata']['acl'],metadata['acl'])
        self.assertEqual({k:result['metadata']['source'][k] for k in metadata['source']},metadata['source'])
        self.assertEqual(result['metadata']['native_extra'],metadata['native_extra'])
        cm=result['metadata']['content_metadata']
        self.assertEqual((cm['has_text'],cm['needs_ocr_for_text'],cm['page_number'],cm['bbox']),(False,True,4,[.1,.2,.6,.8]))
        self.assertNotIn('embedding',result['metadata'])
        self.assertEqual(row,before)

    def test_flattened_native_source_fallback_is_still_deterministic(self):
        row={'text':'Generated source fixture','text_embeddings_1b_v2':{'embedding':[1.0]+[0.0]*2047},'path':'/generated-fixture.pdf','metadata':{'content_metadata':{'page_number':2}}}
        first=compact_nrl_rows([row],'fixture-document')
        self.assertEqual(first,compact_nrl_rows([row],'fixture-document'))
        self.assertEqual(first[0]['metadata']['source']['source_uri'],'/generated-fixture.pdf')


class EmptySourceNotificationTests(unittest.IsolatedAsyncioTestCase):
    async def stage(self,prefix):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        envelope=StageEnvelope(job_id='generated-job',document_id='generated-image-only-document',stage=Stage.EMBEDDING,artifact_uri='file:///generated-fixture.pdf',idempotency_key='sha256:'+'a'*64,pipeline_version='generated-v1',config_digest='sha256:'+'b'*64,model_digest='sha256:'+'c'*64)
        stager=EsReadyCheckpointStager(Path(self.temp.name),'sha256:'+'d'*64,'sha256:'+'e'*64,2048,index_stream=prefix+'.index')
        with patch.dict(os.environ,{'INGEST_STREAM_PREFIX':prefix}):
            first=await stager.stage(envelope,[])
            second=await stager.stage(envelope,[])
        return envelope,first,second

    async def test_application_empty_checkpoint_notifies_source_admission_deterministically(self):
        envelope,(artifact,downstream),(_,again)=await self.stage('rag.agentic')
        self.assertEqual(len(downstream),1)
        stream,event=downstream[0]
        self.assertEqual(stream,'rag.agentic.index')
        self.assertEqual(event.stage,Stage.INDEX)
        self.assertEqual(event.artifact_uri,artifact.final_uri)
        self.assertEqual(event.document_id,envelope.document_id)
        self.assertEqual(event.idempotency_key,again[0][1].idempotency_key)
        self.assertEqual(event.message_id,again[0][1].message_id)
        self.assertEqual(json.loads(Path(artifact.temporary_uri.removeprefix('file://')).read_text())['documents'],[])
        self.assertEqual(artifact.metadata['writable_documents'],0)

    async def test_other_stream_prefix_keeps_empty_index_notification_disabled(self):
        _,(_,downstream),_=await self.stage('ingest')
        self.assertEqual(downstream,())

class NativeReshapeLineageTests(unittest.TestCase):
    def test_native_ocr_flags_source_acl_and_table_region_survive_reshape_then_compaction(self):
        import pandas as pd
        from nv_ingest_durable.deferred_caption import reshape_source_content
        original={'source':{'source_id':'native-unit','source_revision_id':'captured-revision'},'acl':['qualification'],'has_text':False,'needs_ocr_for_text':True,'retained_extra':{'unit':'original'},'content_metadata':{'bbox':[0.,0.,1.,1.]}}
        frame=pd.DataFrame([{'text':'Observed OCR page text','_page_number':7,'metadata':original,'table':[{'text':'Observed table value','bbox_xyxy_norm':[.2,.3,.8,.9]}],'images':[]}])
        result=reshape_source_content(frame,caption_enabled=False)
        rows=result.to_dict(orient='records')
        for row in rows:row['text_embeddings_1b_v2']={'embedding':[1.0]+[0.0]*2047}
        documents=compact_nrl_rows(rows,'generated-document')
        self.assertEqual({d['text'] for d in documents},{'Observed OCR page text','Observed table value'})
        for doc in documents:
            metadata=doc['metadata'];cm=metadata['content_metadata']
            self.assertEqual(metadata['source']['source_id'],'native-unit')
            self.assertEqual(metadata['source']['source_revision_id'],'captured-revision')
            self.assertEqual(metadata['acl'],['qualification'])
            self.assertEqual((cm['has_text'],cm['needs_ocr_for_text'],cm['page_number']),(False,True,7))
            self.assertEqual(metadata['retained_extra'],original['retained_extra'])
        table=next(d for d in documents if d['text']=='Observed table value')
        self.assertEqual(table['metadata']['content_metadata']['bbox'],[.2,.3,.8,.9])
        self.assertEqual(original['content_metadata'],{'bbox':[0.,0.,1.,1.]})


class ObservedNativePageTests(unittest.IsolatedAsyncioTestCase):
    async def test_actual_native_graph_retains_blank_page_proof_through_foreground_staging(self):
        import importlib.util
        spec=importlib.util.spec_from_file_location('native_page_fixture',Path(__file__).with_name('test_native_content.py'))
        fixtures=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixtures)
        harness=fixtures.NativeContentTests();self.addCleanup(harness.doCleanups);harness.setUp()
        output=harness.run_content([{'page_number':page,'text':text,'metadata':{'error':None},'images':[]} for page,text in [(1,'Observed page one'),(2,''),(3,'')]])
        envelope=StageEnvelope(job_id='generated-job',document_id='generated-doc',stage=Stage.EXTRACTION,artifact_uri='file:///generated.pdf',idempotency_key='sha256:'+'c'*64,pipeline_version='generated-v1',config_digest='sha256:'+'1'*64,model_digest='sha256:'+'1'*64)
        with patch.dict(os.environ,{'INGEST_STREAM_PREFIX':'rag.agentic'}):
            pending,_=await harness.processor.stager.stage(envelope,output)
        checkpoint=json.loads(Path(pending.temporary_uri.removeprefix('file://')).read_text())
        self.assertEqual(len(checkpoint['documents']),1)
        self.assertEqual(checkpoint['native_page_completions'],{'schema_version':'native-page-completions.v1','pages':[{'page_number':page,'status':'complete'} for page in (1,2,3)]})

    async def test_actual_native_graph_preserves_explicit_successful_all_blank_capture(self):
        import pandas as pd
        from nv_ingest_durable.deferred_caption import reshape_source_content,CaptionHandoffStager
        from nv_ingest_durable.stage_processing import FusedCheckpointStager
        result=reshape_source_content(pd.DataFrame([{'page_number':page,'text':'','metadata':{'error':None}} for page in (1,2)]),caption_enabled=False)
        self.assertTrue(result.empty)
        with tempfile.TemporaryDirectory() as root:
            stager=CaptionHandoffStager(FusedCheckpointStager(EsReadyCheckpointStager(Path(root),'sha256:'+'1'*64,'sha256:'+'1'*64,2048,index_stream='rag.agentic.index'),'sha256:'+'1'*64,'sha256:'+'1'*64),root)
            envelope=StageEnvelope(job_id='generated-job',document_id='generated-doc',stage=Stage.EXTRACTION,artifact_uri='file:///generated.pdf',idempotency_key='sha256:'+'c'*64,pipeline_version='generated-v1',config_digest='sha256:'+'1'*64,model_digest='sha256:'+'1'*64)
            with patch.dict(os.environ,{'INGEST_STREAM_PREFIX':'rag.agentic'}):pending,downstream=await stager.stage(envelope,result)
            checkpoint=json.loads(Path(pending.temporary_uri.removeprefix('file://')).read_text())
            self.assertEqual(checkpoint['documents'],[])
            self.assertEqual([p['page_number'] for p in checkpoint['native_page_completions']['pages']],[1,2])
            self.assertEqual(len(downstream),1)

    async def test_native_page_identity_and_failed_rows_are_rejected_before_claiming_completion(self):
        import pandas as pd
        from nv_ingest_durable.deferred_caption import reshape_source_content
        for rows in ([{'text':'No observed page identity'}],
                     [{'page_number':1,'text':'first'},{'page_number':1,'text':'duplicate'}],
                     [{'page_number':1,'text':'failed','metadata':{'error':{'message':'failure'}}}],
                     [{'page_number':0,'text':'bad page'}],
                     [{'page_number':1,'text':'failed','error':'native failure'}]):
            with self.subTest(rows=rows),self.assertRaises(ValueError):reshape_source_content(pd.DataFrame(rows),caption_enabled=False)
