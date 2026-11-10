import hashlib
import json
import math
import tempfile
import unittest
from pathlib import Path

from collection_control.artifacts import admit, canonical, load_artifact, content_digest
from collection_control.writer import GenerationWriter
from collection_control.repository import Conflict


def fixture():
    return {'contract':{'dimensions':2048,'dtype':'float32','normalization':'unit','metric':'IP','embedding_checkpoint_sha256':'e'*64,'schema_version':1},'records':[{'record_id':'source1/page1','text':'Generated qualification example','metadata':{'origin':'synthetic','source_id':'source1','source_revision_id':'rev1','content_sha256':hashlib.sha256(b'Generated qualification example').hexdigest(),'acl':['qualification']} ,'vector':[1.0]+[0.0]*2047}]}


class ArtifactTests(unittest.TestCase):
    def test_admit_and_digest_verify_exact_artifact_bytes(self):
        x=fixture();admit(x)
        with tempfile.TemporaryDirectory() as d:
            raw=canonical(x);sha=hashlib.sha256(raw).hexdigest();p=Path(d)/(sha+'.json');p.write_bytes(raw)
            self.assertEqual(load_artifact(Path(d),sha),x)
            p.write_bytes(raw+b' ')
            with self.assertRaises(Conflict):load_artifact(Path(d),sha)

    def test_reject_wrong_dimension_nan_nonunit_duplicate_and_missing_lineage(self):
        for change in [lambda x:x['records'][0]['vector'].pop(),lambda x:x['records'][0]['vector'].__setitem__(0,float('nan')),lambda x:x['records'][0]['vector'].__setitem__(0,0.5),lambda x:x['records'].append(x['records'][0]),lambda x:x['records'][0]['metadata'].pop('source_revision_id')]:
            x=fixture();change(x)
            with self.assertRaises(Conflict):admit(x)

    def test_symlink_escape_is_not_an_admitted_artifact(self):
        with tempfile.TemporaryDirectory() as d,tempfile.TemporaryDirectory() as external:
            raw=canonical(fixture());sha=hashlib.sha256(raw).hexdigest();other=Path(external)/'data';other.write_bytes(raw)
            (Path(d)/(sha+'.json')).symlink_to(other)
            with self.assertRaises(Conflict):load_artifact(Path(d),sha)


class MemoryRepo:
    def __init__(self,lease):self.lease=lease;self.receipts=[];self.published=False;self.cancelled=False;self.renewals=0
    async def claim(self,*a,**k):return self.lease
    async def receipt(self,*a):self.receipts.append(a)
    async def publish(self,*a):self.published=True
    async def cancel(self,*a):self.cancelled=True
    async def renew(self,*a,**k):self.renewals+=1;return True


class Store:
    def __init__(self,fail=False,bad=False):self.fail=fail;self.bad=bad;self.calls=[]
    async def write_verify(self,handle,artifact):
        self.calls.append(handle)
        if self.fail:raise RuntimeError('injected store failure')
        return ('0'*64 if self.bad else content_digest(artifact['records']),len(artifact['records']))


class WriterTests(unittest.IsolatedAsyncioTestCase):
    async def test_second_store_failure_leaves_publication_closed(self):
        with tempfile.TemporaryDirectory() as d:
            x=fixture();raw=canonical(x);sha=hashlib.sha256(raw).hexdigest();(Path(d)/(sha+'.json')).write_bytes(raw)
            lease={'artifact_sha256':sha,'contract_sha256':hashlib.sha256(canonical(x['contract'])).hexdigest(),'targets':['elasticsearch','milvus'],'handles':{'elasticsearch':'attempt_es','milvus':'attempt_mv'}}
            repo=MemoryRepo(lease);writer=GenerationWriter(repo,Path(d),{'elasticsearch':Store(),'milvus':Store(fail=True)},allow_synthetic=True)
            with self.assertRaises(RuntimeError):await writer.run('operation')
            self.assertEqual(len(repo.receipts),1);self.assertFalse(repo.published)
            repo=MemoryRepo(lease);writer=GenerationWriter(repo,Path(d),{'elasticsearch':Store(),'milvus':Store()},allow_synthetic=True)
            self.assertTrue(await writer.run('operation'));self.assertTrue(repo.published)

    async def test_wrong_store_content_or_contract_never_gets_receipt(self):
        with tempfile.TemporaryDirectory() as d:
            x=fixture();raw=canonical(x);sha=hashlib.sha256(raw).hexdigest();(Path(d)/(sha+'.json')).write_bytes(raw)
            lease={'artifact_sha256':sha,'contract_sha256':hashlib.sha256(canonical(x['contract'])).hexdigest(),'targets':['elasticsearch'],'handles':{'elasticsearch':'attempt_es'}}
            repo=MemoryRepo(lease);store=Store(bad=True)
            with self.assertRaises(Conflict):await GenerationWriter(repo,Path(d),{'elasticsearch':store},allow_synthetic=True).run('operation')
            self.assertFalse(repo.receipts);self.assertFalse(repo.published)
            lease['contract_sha256']='f'*64;store=Store()
            with self.assertRaises(Conflict):await GenerationWriter(MemoryRepo(lease),Path(d),{'elasticsearch':store},allow_synthetic=True).run('operation')
            self.assertFalse(store.calls)

class AdapterBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_adapters_reject_non_attempt_handles_before_network(self):
        from collection_control.stores import ElasticsearchProjection,MilvusProjection
        with self.assertRaises(Conflict):await ElasticsearchProjection(None,'http://unused').write_verify('existing-production-index',fixture())
        with self.assertRaises(Conflict):await MilvusProjection('http://unused').write_verify('existing_production_collection',fixture())

    async def test_grounded_data_is_blocked_until_stage_admission_bridge_exists(self):
        with tempfile.TemporaryDirectory() as d:
            x=fixture();x['records'][0]['metadata']['origin']='grounded'
            raw=canonical(x);sha=hashlib.sha256(raw).hexdigest();(Path(d)/(sha+'.json')).write_bytes(raw)
            lease={'artifact_sha256':sha,'contract_sha256':hashlib.sha256(canonical(x['contract'])).hexdigest(),'targets':['elasticsearch'],'handles':{'elasticsearch':'attempt_es'}}
            repo=MemoryRepo(lease);store=Store()
            with self.assertRaisesRegex(Conflict,'admitted-stage'):
                await GenerationWriter(repo,Path(d),{'elasticsearch':store},allow_synthetic=True).run('operation')
            self.assertFalse(store.calls);self.assertFalse(repo.published)

    async def test_record_identity_utf8_boundary_matches_es(self):
        for name in ['a'*512,'é'*256]:
            x=fixture();x['records'][0]['record_id']=name;admit(x)
        for name in ['a'*513,'é'*257]:
            x=fixture();x['records'][0]['record_id']=name
            with self.assertRaises(Conflict):admit(x)
