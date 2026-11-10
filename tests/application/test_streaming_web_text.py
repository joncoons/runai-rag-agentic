"""Crawler page acknowledgments precede later HTML/asset work."""
import asyncio
import sys
from pathlib import Path
import unittest
from application.web_acquisition import AcquisitionPolicy, collect
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from test_web_acquisition import Session,Response

class StreamingWebTextTests(unittest.IsolatedAsyncioTestCase):
    async def test_first_page_is_durably_admitted_while_later_page_is_blocked(self):
        base='https://docs.example/accounts/';admitted=[];first=asyncio.Event();release=asyncio.Event()
        class SlowResponse(Response):
            async def __aenter__(self):await release.wait();return self
        session=Session({base:Response(b'<main>Root<a href="later">Next</a><img src="/slow.png"><a href="/slow.pdf">PDF</a></main>'),base+'later':SlowResponse(b'<main>Later</main>')})
        async def sink(pages):
            admitted.extend(p.final_uri for p in pages)
            if base in admitted:first.set()
        task=asyncio.create_task(collect([base],AcquisitionPolicy(allowed_hosts=('docs.example',),page_prefixes=(base,),max_depth=None),session=session,page_sink=sink))
        try:
            await asyncio.wait_for(first.wait(),2)
            self.assertFalse(task.done());self.assertEqual(admitted,[base])
            release.set();capture=await asyncio.wait_for(task,3)
            self.assertTrue(capture.complete);self.assertEqual(len(admitted),2)
            self.assertEqual(session.calls,[base,base+'later'])
        finally:
            release.set()
            if not task.done():task.cancel()
