"""External search boundary for explicitly opted-in, non-private research jobs."""
import base64,hashlib,json,os
from datetime import datetime,timezone
from urllib.parse import urlsplit,urlunsplit
import aiohttp
from collection_control.artifacts import canonical

class WebSearchUnavailable(RuntimeError):pass

def enabled():return os.environ.get('RESEARCH_WEB_SEARCH_ENABLED','').strip().lower()=='true'

class TavilySearch:
 def __init__(self,http):self.http=http
 async def search(self,question):
  key=os.environ.get('TAVILY_API_KEY','').strip()
  if not key:raise WebSearchUnavailable('Web search is not configured')
  try:
   async with self.http.post('https://api.tavily.com/search',
     headers={'Authorization':'Bearer '+key},allow_redirects=False,timeout=aiohttp.ClientTimeout(total=30),
     json={'query':question,'search_depth':'basic','max_results':5,'topic':'general',
           'include_answer':False,'include_raw_content':False,'include_images':False,'auto_parameters':False}) as response:
    if response.status!=200:raise WebSearchUnavailable('Web search provider returned HTTP '+str(response.status))
    raw=await response.read()
   data=json.loads(raw)
   if not isinstance(data,dict) or not isinstance(data.get('results'),list):raise ValueError()
  except WebSearchUnavailable:raise
  except (aiohttp.ClientError,TimeoutError,ValueError,UnicodeError):
   raise WebSearchUnavailable('Web search provider did not return usable evidence') from None
  observed=datetime.now(timezone.utc).isoformat();response_sha=hashlib.sha256(raw).hexdigest();query_sha=hashlib.sha256(question.encode()).hexdigest()
  proof={'provider':'tavily','response_sha256':response_sha,'retrieved_at':observed,'query_sha256':query_sha,'response_bytes_b64':base64.b64encode(raw).decode('ascii')}
  if isinstance(data.get('request_id'),str):proof['request_id']=data['request_id']
  sources=[];seen=set();skipped=0
  for rank,item in enumerate(data['results'],1):
   if not isinstance(item,dict):skipped+=1;continue
   uri=item.get('url');text=item.get('content');title=item.get('title')
   try:
    parts=urlsplit(uri) if isinstance(uri,str) else None
    if not parts or parts.scheme not in ('http','https') or not parts.hostname or parts.username or parts.password or not isinstance(text,str) or not text.strip():raise ValueError()
    uri=urlunsplit(parts._replace(fragment=''))
   except ValueError:skipped+=1;continue
   content_sha=hashlib.sha256(text.encode()).hexdigest();identity=hashlib.sha256(canonical({'uri':uri,'content_sha256':content_sha})).hexdigest()
   if identity in seen:continue
   seen.add(identity)
   source={'collection_id':'web:tavily','record_id':'web:'+identity,'uri':uri,'title':title if isinstance(title,str) and title.strip() else uri,
           'text':text,'generated_annotation':False,'evidence_origin':'web_search','provider':'tavily',
           'content_sha256':content_sha,'response_sha256':response_sha,'retrieved_at':observed,'query_sha256':query_sha,'result_rank':rank}
   if proof.get('request_id'):source['provider_request_id']=proof['request_id']
   if isinstance(item.get('published_date'),str):source['published_at']=item['published_date']
   sources.append(source)
  warnings=['Some web search results lacked usable excerpt text or a safe source URL.'] if skipped else []
  if not sources:warnings.append('Web search returned no usable source excerpts for a planned query.')
  return {'sources':sources,'warnings':warnings,'proof':proof}
