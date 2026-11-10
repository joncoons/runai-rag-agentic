import unittest
from unittest.mock import AsyncMock,patch
from application import web_acquisition as web
from tests.test_web_acquisition import Session,Response

class BrowserAcquisitionTests(unittest.IsolatedAsyncioTestCase):
 async def test_sparse_html_calls_renderer_and_preserves_http_evidence(self):
  raw=b'<html><body><main></main><script src="app.js"></script></body></html>'
  rendered=b'<html><title>Rendered</title><main><h1>Accounts</h1><p>Loaded product details.</p><a href="next">Next</a></main></html>'
  from application.browser_acquisition import BrowserResult
  renderer=AsyncMock(return_value=BrowserResult(rendered,'https://docs.example/',{'wait_timeout_seconds':20}))
  page=await web.acquire_page('https://docs.example/',web.AcquisitionPolicy(('docs.example',)),session=Session({'https://docs.example/':Response(raw)}),renderer=renderer)
  renderer.assert_awaited_once()
  self.assertEqual(page.raw_bytes,rendered)
  self.assertIn('Loaded product',page.text)
  self.assertEqual(page.evidence['rendering']['http_html_sha256'],'sha256:'+__import__('hashlib').sha256(raw).hexdigest())
  self.assertEqual(page.links,('https://docs.example/next',))

 async def test_static_content_does_not_call_browser(self):
  renderer=AsyncMock()
  raw=('<main>'+('Product facts. '*40)+'</main>').encode()
  await web.acquire_page('https://docs.example/',web.AcquisitionPolicy(('docs.example',)),session=Session({'https://docs.example/':Response(raw)}),renderer=renderer)
  renderer.assert_not_awaited()

 async def test_renderer_cannot_change_scope_or_leave_empty_main(self):
  from application.browser_acquisition import BrowserResult
  raw=b'<main></main><script src="app.js"></script>'
  for result in [BrowserResult(b'<main>outside</main>','https://elsewhere.example/',{}),BrowserResult(raw,'https://docs.example/',{})]:
   with self.assertRaises(web.AcquisitionError):
    await web.acquire_page('https://docs.example/',web.AcquisitionPolicy(('docs.example',)),session=Session({'https://docs.example/':Response(raw)}),renderer=AsyncMock(return_value=result))

 async def test_empty_main_is_rendered_even_when_surrounding_navigation_is_long(self):
  from application.browser_acquisition import BrowserResult
  raw=('<html><body><nav>'+('<span>Navigation</span>'*70)+'</nav><main></main><script src="app.js"></script></body></html>').encode()
  renderer=AsyncMock(return_value=BrowserResult(b'<main>Actual rendered application</main>','https://docs.example/',{}))
  page=await web.acquire_page('https://docs.example/',web.AcquisitionPolicy(('docs.example',)),session=Session({'https://docs.example/':Response(raw)}),renderer=renderer)
  renderer.assert_awaited_once();self.assertIn('Actual rendered',page.text)

 async def test_cancellation_waits_for_browser_thread_before_cleanup(self):
  import asyncio,threading
  from application.browser_acquisition import BrowserRenderer,BrowserResult
  renderer=BrowserRenderer(web.AcquisitionPolicy(('docs.example',)))
  entered=threading.Event();release=threading.Event()
  def render(url):
   entered.set();release.wait(timeout=3)
   return BrowserResult(b'<main>ready</main>',url,{})
  renderer._render=render
  task=asyncio.create_task(renderer('https://docs.example/'))
  try:
   await asyncio.to_thread(entered.wait,1);task.cancel();await asyncio.sleep(.02)
   self.assertFalse(task.done(),'Cancellation must not release browser ownership while its thread still uses the driver')
  finally:
   release.set()
   with self.assertRaises(asyncio.CancelledError):await task

 async def test_capture_labels_the_new_disclosure_representation(self):
  raw=b'<main>Product<section id="disclaimer">Terms apply.</section></main>'
  page=await web.acquire_page('https://docs.example/',web.AcquisitionPolicy(('docs.example',)),session=Session({'https://docs.example/':Response(raw)}))
  self.assertEqual(page.evidence['normalization'],'html-disclosures-v4')
