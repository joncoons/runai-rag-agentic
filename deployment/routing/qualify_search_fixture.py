#!/usr/bin/env python3
"""Exercise the real Switchyard three-model policy against synthetic CPU endpoints."""
import argparse,json,socket,subprocess,tempfile,threading,time,urllib.request,uuid
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from render import render

class Fixture(BaseHTTPRequestHandler):
 calls=[]
 def log_message(self,*args):pass
 def do_POST(self):
  body=json.loads(self.rfile.read(int(self.headers['Content-Length'])));judge=bool(body.get('response_format'));text=json.dumps(body.get('messages',[]))
  if judge:
   enum=body['response_format']['json_schema']['schema']['properties']['target']['enum']
   target='large' if 'LARGE_FIXTURE' in text and 'large' in enum else 'capable' if any(x in text for x in ('MEDIUM_FIXTURE','LARGE_FIXTURE')) else 'efficient'
   content=json.dumps({'target':target})
  else:content='Synthetic grounded answer'
  Fixture.calls.append({'model':body['model'],'judge':judge,'long_input_complete':'LONG_START' in text and 'LONG_END' in text and text.count('x')>=180000})
  value={'id':'fixture','object':'chat.completion','created':0,'model':body['model'],'choices':[{'index':0,'message':{'role':'assistant','content':content},'finish_reason':'stop'}],'usage':{'prompt_tokens':10,'completion_tokens':5,'total_tokens':15}}
  raw=json.dumps(value).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--image',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
 server=ThreadingHTTPServer(('127.0.0.1',0),Fixture);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
 with socket.socket() as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
 upstream=f'http://127.0.0.1:{server.server_port}/v1';base=f'http://127.0.0.1:{port}/v1';name='rag-search-fixture-'+uuid.uuid4().hex[:10];results=[]
 try:
  with tempfile.TemporaryDirectory(prefix='rag-search-fixture-') as temp:
   files=render(efficient_endpoint=upstream,capable_endpoint=upstream,efficient_model='model/small',capable_model='model/medium',router_endpoint=base,hosted_endpoint='https://synthetic-provider.test/v1',hosted_model='model/large')
   # Only the isolated synthetic fixture substitutes loopback for TLS hosting.
   files['routes.toml']=files['routes.toml'].replace('https://synthetic-provider.test/v1',upstream)
   (Path(temp)/'routes.toml').write_text(files['routes.toml'])
   command=['docker','run','--rm','--runtime','runc','--network','none','-e','NVIDIA_REMOTE_API_KEY=synthetic-fixture-only','-v',temp+':/config:ro',a.image,'--config','/config/routes.toml','--dry-run']
   check=subprocess.run(command,capture_output=True,text=True)
   if check.returncode:raise RuntimeError('Router configuration rejected: '+check.stderr[-2000:])
   subprocess.run(['docker','run','-d','--name',name,'--runtime','runc','--network','host','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--tmpfs','/tmp:rw,size=16m','-e','NVIDIA_REMOTE_API_KEY=synthetic-fixture-only','-v',temp+':/config:ro',a.image,'--config','/config/routes.toml','--host','127.0.0.1','--port',str(port)],check=True,capture_output=True)
   for _ in range(50):
    try:
     with urllib.request.urlopen(base+'/models',timeout=1):break
    except Exception:time.sleep(.1)
   else:raise RuntimeError('Fixture router not ready')
   for route,marker,expected in [('search-local','LONG_START'+'x'*180000+'LONG_END','small'),('search-local','SMALL_FIXTURE','small'),('search-local','MEDIUM_FIXTURE','medium'),('search-local','LARGE_FIXTURE','medium'),('search-hosted','LARGE_FIXTURE','large'),('search-hosted','SMALL_FIXTURE','small')]:
    count=sum(c['judge'] for c in Fixture.calls);start=time.monotonic()
    request=urllib.request.Request(base+'/chat/completions',data=json.dumps({'model':route,'messages':[{'role':'user','content':marker}],'max_tokens':128}).encode(),headers={'Content-Type':'application/json','x-switchyard-session-id':uuid.uuid4().hex})
    with urllib.request.urlopen(request,timeout=10) as response:
     result=json.load(response);selected=response.headers.get('x-model-router-selected-model')
    assert selected=='model/'+expected,(route,selected)
    assert result['model']==selected
    assert sum(c['judge'] for c in Fixture.calls)==count+1
    if marker.startswith('LONG_START'):assert all(c['long_input_complete'] for c in Fixture.calls[-2:]),'Router truncated complete input'
    results.append({'route':route,'selected_model':selected,'elapsed_ms':round((time.monotonic()-start)*1000,2)})
   proof={'status':'passed','qualification':'synthetic-policy-only','checks':['native-configuration','three-target-selection','local-remote-exclusion','one-classification-per-query','independent-query-affinity','complete-long-context-forwarding'],'requests':results,'production_enabled':False}
   a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(proof,indent=2)+'\n');print(json.dumps(proof))
 finally:
  subprocess.run(['docker','rm','-f',name],capture_output=True);server.shutdown();server.server_close();thread.join(timeout=2)
if __name__=='__main__':main()
