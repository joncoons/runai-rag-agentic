"""Native model SSE and simple answer events; no background queue."""
import asyncio,codecs,json,re,time
from contextlib import aclosing
import aiohttp
from collection_control.repository import Conflict

async def sse_data(chunks):
    decoder=codecs.getincrementaldecoder('utf-8')();pending=''
    try:
        async for chunk in chunks:
            pending+=decoder.decode(chunk)
            while (match:=re.search(r'\r?\n\r?\n',pending)):
                frame,pending=pending[:match.start()],pending[match.end():]
                lines=[line[5:].removeprefix(' ') for line in frame.splitlines() if line.startswith('data:')]
                if lines:yield '\n'.join(lines)
        pending+=decoder.decode(b'',final=True)
    except UnicodeDecodeError:raise Conflict('Invalid UTF-8 model stream') from None
    if pending.strip():raise Conflict('Incomplete model stream frame')

async def chat_stream(query,messages,*,max_tokens=2048):
    from .query import _endpoint
    roles={'system':'system','human':'user'}
    payload={'model':query.settings.llm_model,'messages':[{'role':roles[m.type],'content':m.content} for m in messages],
             'temperature':0,'max_tokens':max_tokens,'stream':True,'chat_template_kwargs':{'enable_thinking':False}}
    finished=False;content_seen=False
    try:
        async with query.http.post(_endpoint(query.settings.llm_endpoint,'/chat/completions'),json=payload,
                                   timeout=aiohttp.ClientTimeout(total=180)) as response:
            if response.status>=300:raise Conflict('Answer model stream rejected (HTTP '+str(response.status)+')')
            async for data in sse_data(response.content.iter_any()):
                if data=='[DONE]':
                    if not finished or not content_seen:raise Conflict('Answer model returned no complete final text')
                    return
                try:
                    event=json.loads(data)
                    observer=getattr(response,'record_model_event',None)
                    if observer:observer(event)
                    if 'error' in event:raise ValueError()
                    choices=event['choices']
                    if choices==[]:continue
                    if len(choices)!=1:raise ValueError()
                    choice=choices[0];delta=choice.get('delta',{});text=delta.get('content')
                    if delta.get('tool_calls') or delta.get('function_call'):raise ValueError()
                    if text is not None and not isinstance(text,str):raise ValueError()
                    if text:
                        if finished:raise ValueError()
                        content_seen=content_seen or bool(text.strip());yield text
                    reason=choice.get('finish_reason')
                    if reason is not None:
                        if reason=='length':raise Conflict('Answer model reached the output limit; incomplete text was not returned')
                        if reason!='stop':raise Conflict('Answer model did not finish normally')
                        finished=True
                except (ValueError,KeyError,TypeError,IndexError):raise Conflict('Invalid answer model stream') from None
            raise Conflict('Answer model stream ended before completion')
    except (aiohttp.ClientError,asyncio.TimeoutError):raise Conflict('Answer model stream interrupted; retry the query') from None

class CitationFilter:
    def __init__(self,sources):self.allowed={s['citation_id'] for s in sources};self.pending='';self.invalid=False
    def feed(self,text,*,final=False):
        text=self.pending+text;self.pending=''
        if not final and (match:=re.search(r'\[(?:S\d*)?$',text)):
            text,self.pending=text[:match.start()],text[match.start():]
        def citation(match):
            if match.group(1) in self.allowed:return match.group(0)
            self.invalid=True;return ''
        return re.sub(r'\[(S\d+)\]',citation,text)

async def search_events(query,question,collection_ids,backend='elasticsearch',mode='search',*,allow_remote=False):
    if type(allow_remote) is not bool or (allow_remote and mode!='search'):raise Conflict('Remote permission is valid only for ordinary Search')
    began=time.monotonic();elapsed=lambda:round((time.monotonic()-began)*1000,2)
    query._validate(question,collection_ids,backend,mode)
    state={'query':question,'collection_ids':list(collection_ids),'backend':backend,'mode':mode,'queries':[question]}
    yield {'event':'status','data':{'stage':'planning' if mode=='deep_research' else 'retrieval','elapsed_ms':elapsed()}}
    if mode=='deep_research':state.update(await query._plan(state))
    state.update(await query._evidence(state))
    model_start=elapsed();first_model=None;first_answer=None;answer='';filter=CitationFilter(state['sources'])
    yield {'event':'status','data':{'stage':'generating','elapsed_ms':elapsed()}}
    messages=query._answer_messages(state);client=query
    if mode=='search':
        from .search_routing import answer_client
        client=answer_client(query,messages,allow_remote=allow_remote)
    async with aclosing(client._chat_stream(messages)) as pieces:
        async for chunk in pieces:
            if first_model is None:first_model=elapsed()
            text=filter.feed(chunk)
            if not text:continue
            answer+=text
            if first_answer is None:first_answer=elapsed()
            yield {'event':'delta','data':{'text':text,'elapsed_ms':elapsed()}}
    tail=filter.feed('',final=True)
    if tail:
        answer+=tail
        if first_answer is None:first_answer=elapsed()
        yield {'event':'delta','data':{'text':tail,'elapsed_ms':elapsed()}}
    routing=getattr(client,'search_routing',None)
    warnings=list(state.get('warnings',[]))
    if routing and routing.get('remote_context_excluded'):warnings.append('The complete input exceeds the hosted context eligibility budget; this answer used a local model without truncation.')
    if filter.invalid:warnings.append('Unverified model citation IDs were removed from the answer.')
    await query._checkpoint('answer',{'answer':answer,'sources':state['sources'],'mode':mode})
    yield {'event':'complete','data':{'answer':answer,'sources':state['sources'],'warnings':warnings,**({'routing':routing} if routing else {}),'timings':{
        'generation_start_ms':model_start,'first_model_content_ms':first_model,'first_answer_text_ms':first_answer,'complete_ms':elapsed()}}}
