"""A stalled caption result must not occupy the page admission process."""
import asyncio
import unittest
from application.worker import ApplicationWorker

class AdmissionIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def test_blocked_caption_does_not_block_page_or_writer(self):
        started=asyncio.Event();release=asyncio.Event();pages=[];published=[]
        caption=object.__new__(ApplicationWorker);page=object.__new__(ApplicationWorker)
        async def caption_handler(message):started.set();await release.wait()
        async def source_handler(message):pages.append(message)
        async def consume_caption(stream,handler,**kwargs):
            self.assertEqual(stream,'rag.agentic.caption.results');await handler('caption')
        async def consume_page(stream,handler):
            self.assertEqual(stream,'rag.agentic.index');await handler('page')
        async def publish():published.extend(pages)
        caption.caption=caption_handler;caption.consume_once=consume_caption
        page.source=source_handler;page.consume_once=consume_page;page.publish_once=publish
        task=asyncio.create_task(caption.run_once('caption-admission'))
        try:
            await asyncio.wait_for(started.wait(),1)
            await asyncio.wait_for(page.run_once('admission'),1)
            await asyncio.wait_for(page.run_once('writer'),1)
            self.assertEqual(published,['page']);self.assertFalse(task.done())
        finally:
            release.set();await task

    async def test_admission_never_consumes_caption_stream(self):
        worker=object.__new__(ApplicationWorker);streams=[]
        async def consume(stream,handler):streams.append(stream)
        worker.consume_once=consume
        await worker.run_once('admission')
        self.assertEqual(streams,['rag.agentic.index'])
