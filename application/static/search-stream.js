'use strict';
async function consumeSearchStream(response,onEvent){
 if(!response.ok){let data={};try{data=await response.json();}catch{}throw Error(typeof data.detail==='string'?data.detail:'Search failed ('+response.status+').');}
 if(!response.body||!(response.headers.get('content-type')||'').includes('text/event-stream'))throw Error('Search streaming is unavailable. Please retry.');
 const reader=response.body.getReader(),decoder=new TextDecoder('utf-8',{fatal:true});let pending='';
 try{while(true){const {done,value}=await reader.read();pending+=done?decoder.decode():decoder.decode(value,{stream:true});let match;
  while((match=/\r?\n\r?\n/.exec(pending))){const frame=pending.slice(0,match.index);pending=pending.slice(match.index+match[0].length);const lines=frame.split(/\r?\n/),kind=lines.find(l=>l.startsWith('event:'))?.slice(6).trim()||'message';const dataLines=lines.filter(l=>l.startsWith('data:')).map(l=>l.slice(5).replace(/^ /,''));if(!dataLines.length)continue;const data=JSON.parse(dataLines.join('\n'));
   if(kind==='error')throw Error(typeof data.detail==='string'?data.detail:'Search interrupted. Please retry.');
   if(kind==='delta'&&typeof data.text!=='string')throw Error('Invalid search stream. Please retry.');
   if(kind==='complete'&&(typeof data.answer!=='string'||!Array.isArray(data.sources)||!Array.isArray(data.warnings)))throw Error('Invalid completed search response.');
   if(['status','delta','complete'].includes(kind))onEvent(kind,data);
   if(kind==='complete')return data;
  }
  if(done)throw Error('Search ended before completion. Please retry.');
 }}finally{try{await reader.cancel();}finally{reader.releaseLock();}}
}
async function streamSearch(body,onEvent,signal){
 const response=await fetch('/api/search/stream',{method:'POST',headers:{'Content-Type':'application/json','Accept':'text/event-stream'},body:JSON.stringify(body),signal});
 return consumeSearchStream(response,onEvent);
}
