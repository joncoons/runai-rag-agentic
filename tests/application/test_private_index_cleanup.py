import unittest
from types import SimpleNamespace
from uuid import uuid4
from application.private_cleanup import purge_indexes
class Response:
 def __init__(self,status,body):self.status=status;self.body=body
 async def __aenter__(self):return self
 async def __aexit__(self,*args):pass
 async def json(self):return self.body
class ScopedIndexCleanup(unittest.IsolatedAsyncioTestCase):
 async def test_cleanup_works_without_cluster_monitor_and_preserves_other_workspace(self):
  operation=uuid4();expired='rag_analysis_'+operation.hex+'_1_es';other='rag_analysis_'+uuid4().hex+'_1_es'
  deleted=[]
  class ES:
   def get(self,url):return Response(403,{}) if '/_cat/' in url else Response(200,{expired:{'aliases':{}},other:{'aliases':{}}})
   def delete(self,url):deleted.append(url.rsplit('/',1)[-1]);return Response(200,{})
  base=SimpleNamespace(es_http=ES(),settings=SimpleNamespace(es_endpoint='http://private-es'))
  await purge_indexes(base,{'operations':[str(operation)]})
  self.assertEqual(deleted,[expired])
