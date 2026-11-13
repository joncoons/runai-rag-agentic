import unittest
from datetime import datetime,timezone,timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock,Mock,patch
from uuid import uuid4
import httpx
from application.api import create_app

class ResearchWebAPITests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.query=SimpleNamespace(_validate=Mock(),catalog=object(),_resolve=AsyncMock(return_value=[]))
  async def created(owner,key,request):
   return {'run_id':uuid4(),'status':'queued','request':request,'checkpoint':{},'report':'','error_type':None,'attempts':0,'expires_at':datetime.now(timezone.utc)+timedelta(days=7)}
  self.repo=SimpleNamespace(create=AsyncMock(side_effect=created),listing=AsyncMock(return_value=[]))
  self.auth=patch('application.research_api.owner_session',new=AsyncMock(return_value={'owner_id':uuid4()}));self.auth.start();self.addCleanup(self.auth.stop)
  self.repository=patch('application.research_api.ResearchJobs',return_value=self.repo);self.repository.start();self.addCleanup(self.repository.stop)
  self.client=httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(context=SimpleNamespace(pool=None),query_service=self.query)),base_url='http://fixture');self.addAsyncCleanup(self.client.aclose)
 async def test_web_only_requires_explicit_strict_optin_and_deployment_enablement(self):
  for flag in (None,False,'true',1):
   body={'question':'Question','collection_ids':[],'request_key':'fixture'}
   if flag is not None:body['include_web']=flag
   response=await self.client.post('/api/research/runs',json=body);self.assertEqual(response.status_code,422,response.text)
  with patch.dict('os.environ',{'RESEARCH_WEB_SEARCH_ENABLED':'false'}):
   response=await self.client.post('/api/research/runs',json={'question':'Question','collection_ids':[],'request_key':'fixture','include_web':True})
  self.assertEqual(response.status_code,503);self.repo.create.assert_not_awaited()
 async def test_web_only_admission_preserves_choice_and_does_not_resolve_local_stores(self):
  with patch.dict('os.environ',{'RESEARCH_WEB_SEARCH_ENABLED':'true'}):
   response=await self.client.post('/api/research/runs',json={'question':'Question','collection_ids':[],'request_key':'fixture','include_web':True})
  self.assertEqual(response.status_code,202,response.text);self.assertTrue(response.json()['include_web']);self.query._resolve.assert_not_awaited()
  self.assertTrue(self.repo.create.await_args.args[2]['include_web'])
 async def test_local_requests_remain_backward_compatible_and_do_not_require_key(self):
  with patch.dict('os.environ',{'RESEARCH_WEB_SEARCH_ENABLED':'false'}):
   response=await self.client.post('/api/research/runs',json={'question':'Question','collection_ids':['one'],'request_key':'fixture'})
  self.assertEqual(response.status_code,202,response.text);self.assertFalse(response.json()['include_web'])
  self.assertNotIn('include_web',self.repo.create.await_args.args[2]);self.query._resolve.assert_awaited_once()
 async def test_capabilities_are_boolean_without_credentials(self):
  with patch.dict('os.environ',{'RESEARCH_WEB_SEARCH_ENABLED':'true','TAVILY_API_KEY':'fixture-not-for-client'}):response=await self.client.get('/api/research/runs')
  self.assertTrue(response.json()['capabilities']['web_search']);self.assertNotIn('fixture-not-for-client',response.text)
