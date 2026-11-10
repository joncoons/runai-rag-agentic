"""SSE transport with one outstanding event, heartbeat and disconnect cleanup."""
import asyncio,json,logging
from contextlib import aclosing
from fastapi import Request
from fastapi.responses import StreamingResponse
from collection_control.repository import Conflict
LOG=logging.getLogger(__name__)

def install_stream_routes(app,service_for):
    from application.api import SearchRequest
    @app.post('/api/search/stream')
    async def stream_search(body:SearchRequest,request:Request):
        service=service_for(request)
        async def events():
            task=None
            try:
                async with aclosing(service.stream_search(body.query,body.collection_ids,backend=body.backend,mode=body.mode)) as source:
                    try:
                        while True:
                            task=asyncio.create_task(anext(source))
                            while not (await asyncio.wait({task},timeout=15))[0]:
                                yield ': keep-alive\n\n'
                            try:event=task.result()
                            except StopAsyncIteration:return
                            task=None
                            if event['event']=='complete':
                                # Model grounding retains excerpts. Browser references need links only.
                                event={**event,'data':{**event['data'],'sources':[{k:v for k,v in s.items() if k!='text'} for s in event['data']['sources']]}}
                            yield 'event: '+event['event']+'\ndata: '+json.dumps(event['data'],ensure_ascii=False)+'\n\n'
                    finally:
                        if task is not None:
                            task.cancel();await asyncio.gather(task,return_exceptions=True);task=None
            except Conflict as error:
                yield 'event: error\ndata: '+json.dumps({'detail':str(error)})+'\n\n'
            except asyncio.CancelledError:raise
            except Exception as error:
                LOG.error('search_stream_failed error_type=%s',type(error).__name__)
                yield 'event: error\ndata: '+json.dumps({'detail':'Search interrupted. Please retry.'})+'\n\n'
            finally:
                if task is not None:
                    task.cancel();await asyncio.gather(task,return_exceptions=True)
        return StreamingResponse(events(),media_type='text/event-stream',headers={'Cache-Control':'no-store, no-transform','X-Accel-Buffering':'no'})
