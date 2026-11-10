import unittest
from dataclasses import replace
from application.web_acquisition import AcquisitionPolicy,acquire_page,collect,AcquisitionError
from application.api import WebIngestion
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from test_web_acquisition import Session,Response

class CrawlScopeControls(unittest.IsolatedAsyncioTestCase):
 def policy(self,**kwargs):return AcquisitionPolicy(allowed_hosts=('docs.example','assets.example'),page_prefixes=('https://docs.example/accounts',),**kwargs)
 def test_api_does_not_impose_small_page_or_depth_ceiling(self):
  request=WebIngestion(seed_urls=['https://docs.example/accounts/'],allowed_hosts=['docs.example'],max_depth=9,max_pages=4000)
  self.assertEqual(request.max_pages,4000)
  default=WebIngestion(seed_urls=['https://docs.example/accounts/'],allowed_hosts=['docs.example'])
  self.assertIsNone(default.max_pages)
 async def test_prefix_gates_html_but_not_linked_assets(self):
  raw=b'<main>Body<a href="/accounts/card">A</a><a href="/accounts-other">Other</a><a href="/cards/">Cards</a><a href="https://assets.example/terms.pdf">PDF</a><img src="/icons/test.png"></main>'
  page=await acquire_page('https://docs.example/accounts/',self.policy(),session=Session({'https://docs.example/accounts/':Response(raw)}))
  self.assertEqual(page.links,('https://docs.example/accounts/card',))
  self.assertEqual({a.url for a in page.assets},{'https://assets.example/terms.pdf','https://docs.example/icons/test.png'})
 async def test_prefix_escape_redirect_is_rejected_before_second_request(self):
  session=Session({'https://docs.example/accounts/':Response(status=302,headers={'Location':'/cards/'})})
  with self.assertRaises(AcquisitionError):await acquire_page('https://docs.example/accounts/',self.policy(),session=session)
  self.assertEqual(session.calls,['https://docs.example/accounts/'])
 async def test_encoded_traversal_is_not_treated_as_section_html(self):
  raw=b'<main>Body<a href="/accounts/%2e%2e/cards">Escape</a><a href="/accounts/deep">Good</a></main>'
  page=await acquire_page('https://docs.example/accounts/',self.policy(),session=Session({'https://docs.example/accounts/':Response(raw)}))
  self.assertEqual(page.links,('https://docs.example/accounts/deep',))
 async def test_unlimited_policy_handles_more_than_twenty_five_pages(self):
  base='https://docs.example/accounts/';raw=('<main>Root'+''.join('<a href="p'+str(i)+'">P</a>' for i in range(40))+'</main>').encode();routes={base:Response(raw)};routes.update({base+'p'+str(i):Response(b'<main>Page</main>') for i in range(40)})
  capture=await collect([base],self.policy(max_depth=1,max_pages=None),session=Session(routes))
  self.assertTrue(capture.complete);self.assertEqual(len(capture.pages),41)
 async def test_optional_https_normalization_keeps_original_body(self):
  raw=b'<main>Body<a href="http://docs.example/shared.pdf">Terms</a></main>'
  original=await acquire_page('https://docs.example/accounts/',self.policy(prefer_https=True),session=Session({'https://docs.example/accounts/':Response(raw)}))
  self.assertEqual(original.raw_bytes,raw);self.assertEqual(original.assets[0].url,'https://docs.example/shared.pdf')
 async def test_redirect_aliases_admit_one_canonical_page(self):
  base='https://docs.example/accounts/'
  routes={base:Response(b'<main>Root<a href="alias">Alias</a><a href="canonical">Canonical</a></main>'),base+'alias':Response(status=302,headers={'Location':'canonical'}),base+'canonical':Response(b'<main>Same page</main>')}
  capture=await collect([base],self.policy(max_depth=1),session=Session(routes))
  self.assertTrue(capture.complete);self.assertEqual(len(capture.pages),2)

 async def test_unlimited_depth_follows_chain_beyond_five_without_leaving_scope(self):
  base='https://docs.example/accounts/'
  routes={base:Response(b'<main>Seed<a href="p1">Next</a></main>')}
  for i in range(1,13):
   links=('<a href="p'+str(i+1)+'">Next</a>') if i<12 else '<a href="p1">Cycle</a><a href="/cards/">Outside</a>'
   routes[base+'p'+str(i)]=Response(('<main>Page'+str(i)+links+'</main>').encode())
  request=WebIngestion(seed_urls=[base],allowed_hosts=['docs.example'],max_depth=None)
  capture=await collect([base],self.policy(max_depth=request.max_depth),session=Session(routes))
  self.assertTrue(capture.complete);self.assertEqual(len(capture.pages),13)
  self.assertTrue(all(p.final_uri.startswith(base) for p in capture.pages))
