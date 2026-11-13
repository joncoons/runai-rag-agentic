#!/usr/bin/env python3
"""Exercise the real Switchyard binary with synthetic, CPU-only model endpoints."""
import argparse,json,socket,subprocess,tempfile,threading,time,urllib.request,uuid
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from render import render

class Fixture(BaseHTTPRequestHandler):
 calls=[]
 def log_message(self,*args):pass
 def do_POST(self):
  body=json.loads(self.rfile.read(int(self.headers['Content-Length'])));structured=bool(body.get('response_format'));Fixture.calls.append({'model':body['model'],'judge':structured})
  if structured:
   schema=body['response_format'].get('json_schema',{}).get('schema',{});props=schema.get('properties',{})
   complex_task='COMPLEX_FIXTURE' in json.dumps(body.get('messages',[]))
   result={'target':'capable' if complex_task else 'efficient'} if 'target' in props else {'p_solve':0.1 if complex_task else .99,'capability_boundary':'unsupported' if complex_task else 'supported','primary_rule':'LIM-2' if complex_task else 'SUP-1','crux':'fixture validation'}
   Fixture.calls[-1].update(schema_properties=list(props),complex_fixture=complex_task,verdict=result);text=json.dumps(result)
  else:text='Fixture answer'
  response={'id':'fixture','object':'chat.completion','created':0,'model':body['model'],'choices':[{'index':0,'message':{'role':'assistant','content':text},'finish_reason':'stop'}],'usage':{'prompt_tokens':10,'completion_tokens':5,'total_tokens':15}}
  data=json.dumps(response).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--image',required=True);p.add_argument('--output',type=Path,required=True);args=p.parse_args()
 fixture=ThreadingHTTPServer(('127.0.0.1',0),Fixture);thread=threading.Thread(target=fixture.serve_forever,daemon=True);thread.start()
 with socket.socket() as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
 name='rag-routing-fixture-'+uuid.uuid4().hex[:10];base=f'http://127.0.0.1:{port}';upstream=f'http://127.0.0.1:{fixture.server_port}/v1';results=[]
 try:
  with tempfile.TemporaryDirectory(prefix='rag-routing-fixture-') as temp:
   root=Path(temp)
   for key,value in render(efficient_endpoint=upstream,capable_endpoint=upstream,efficient_model='model/efficient',capable_model='model/capable',router_endpoint=base+'/v1').items():(root/key).write_text(value)
   subprocess.run(['docker','run','--rm','--runtime','runc','--network','none','-v',temp+':/config:ro',args.image,'--config','/config/routes.toml','--dry-run'],check=True,capture_output=True)
   subprocess.run(['docker','run','-d','--name',name,'--runtime','runc','--network','host','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--tmpfs','/tmp:rw,size=16m','-v',temp+':/config:ro',args.image,'--config','/config/routes.toml','--host','127.0.0.1','--port',str(port)],check=True,capture_output=True)
   for _ in range(50):
    try:
     with urllib.request.urlopen(base+'/v1/models',timeout=1):break
    except Exception:time.sleep(.1)
   else:raise RuntimeError('Fixture router did not become ready')
   def call(route,session,messages):
    payload={'model':route,'messages':messages,'max_tokens':128,'stream':False}
    request=urllib.request.Request(base+'/v1/chat/completions',data=json.dumps(payload).encode(),headers={'Content-Type':'application/json','x-switchyard-session-id':session})
    start=time.monotonic()
    with urllib.request.urlopen(request,timeout=10) as response:
     json.load(response);selected=response.headers.get('x-model-router-selected-model')
    results.append({'route':route,'selected_model':selected,'elapsed_ms':round((time.monotonic()-start)*1000,2)})
    return selected
   simple=[{'role':'user','content':'SIMPLE_FIXTURE'}];complex_task=[{'role':'user','content':'COMPLEX_FIXTURE'}]
   for route in ('research-plan','research-document-selection','research-synthesis'):
    count=sum(c['judge'] for c in Fixture.calls)
    assert call(route,'simple-'+route,simple)=='model/efficient'
    assert call(route,'simple-'+route,simple)=='model/efficient'
    assert sum(c['judge'] for c in Fixture.calls)==count+1,'Research phase was classified more than once'
    assert call(route,'complex-'+route,complex_task)=='model/capable'
   assert call('coding-router','coding-simple',simple)=='model/efficient'
   assert call('coding-router','coding-complex',complex_task)=='model/capable',str(Fixture.calls[-4:])
   history=list(simple)
   for i in range(3):
    history += [{'role':'assistant','tool_calls':[{'id':f'call_{i}','type':'function','function':{'name':'execute','arguments':'{"command":"pytest"}'}}]},
                {'role':'tool','tool_call_id':f'call_{i}','content':'Traceback (most recent call last):\nRuntimeError: test failed\nexit code: 1'}]
   assert call('coding-router','coding-simple',history)=='model/capable','Composite did not recover from repeated tool errors'
   assert call('coding-router','coding-new-job',simple)=='model/efficient','Coding state leaked to a new job'
   assert call('research-capable','fixed',simple)=='model/capable'
   record={'status':'passed','qualification':'synthetic-router-behavior-only','image':args.image,'checks':['native-v0.3-config','research-phase-affinity','independent-complex-tasks','coding-composite-recovery','coding-job-isolation','fixed-capable-alias'],'requests':results,'fixture_judge_calls':sum(c['judge'] for c in Fixture.calls),'model_quality_qualified':False,'production_enabled':False}
   args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(record,indent=2)+'\n');print(json.dumps({k:v for k,v in record.items() if k!='requests'}))
 finally:
  subprocess.run(['docker','rm','-f',name],capture_output=True);fixture.shutdown();fixture.server_close();thread.join(timeout=2)
if __name__=='__main__':main()
