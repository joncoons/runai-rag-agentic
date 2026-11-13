import hashlib,json,unittest
from contextlib import asynccontextmanager
from unittest.mock import patch
from application.web_research import TavilySearch,WebSearchUnavailable

class HTTP:
 def __init__(self,payload,status=200):self.raw=json.dumps(payload).encode();self.status=status;self.calls=[]
 @asynccontextmanager
 async def post(self,url,**kwargs):
  self.calls.append((url,kwargs));yield self
 async def read(self):return self.raw

class TavilyTests(unittest.IsolatedAsyncioTestCase):
 async def test_fixed_endpoint_snippet_provenance_and_no_synthesized_answer(self):
  http=HTTP({'request_id':'provider-id','results':[{'url':'https://public.example/guide','title':'Guide','content':'Full provider excerpt','score':.9}]})
  with patch.dict('os.environ',{'TAVILY_API_KEY':'fixture-key'}):evidence=await TavilySearch(http).search('Question')
  url,options=http.calls[0];self.assertEqual(url,'https://api.tavily.com/search');self.assertFalse(options['allow_redirects'])
  self.assertEqual(options['headers']['Authorization'],'Bearer fixture-key');self.assertNotIn('api_key',options['json'])
  self.assertFalse(options['json']['include_answer']);self.assertFalse(options['json']['include_raw_content'])
  source=evidence['sources'][0];self.assertEqual(source['text'],'Full provider excerpt');self.assertEqual(source['evidence_origin'],'web_search')
  self.assertEqual(source['content_sha256'],hashlib.sha256(source['text'].encode()).hexdigest())
  self.assertEqual(evidence['proof']['response_sha256'],hashlib.sha256(http.raw).hexdigest());self.assertEqual(__import__('base64').b64decode(evidence['proof']['response_bytes_b64']),http.raw);self.assertNotIn('fixture-key',json.dumps(evidence))
 async def test_provider_error_does_not_expose_body_or_key(self):
  http=HTTP({'detail':'fixture-key sensitive provider diagnostic'},429)
  with patch.dict('os.environ',{'TAVILY_API_KEY':'fixture-key'}):
   with self.assertRaises(WebSearchUnavailable) as caught:await TavilySearch(http).search('Question')
  self.assertNotIn('fixture-key',str(caught.exception));self.assertNotIn('sensitive',str(caught.exception))
 async def test_invalid_source_url_is_not_a_citable_evidence_item(self):
  http=HTTP({'results':[{'url':'javascript:alert(1)','title':'Invalid','content':'Unsafe link'}]})
  with patch.dict('os.environ',{'TAVILY_API_KEY':'fixture-key'}):result=await TavilySearch(http).search('Question')
  self.assertEqual(result['sources'],[]);self.assertTrue(result['warnings'])


class WebGroundingTests(unittest.TestCase):
 def test_web_only_research_uses_supplied_evidence_not_model_only_instructions(self):
  from application.query import RAGQuery
  q=object.__new__(RAGQuery)
  messages=q._answer_messages({'query':'Question','collection_ids':[],'mode':'deep_research','evidence_only':True,'sources':[]})
  self.assertNotIn('answer from model knowledge',messages[0].content)
  self.assertIn('Use only supplied evidence',messages[0].content)
