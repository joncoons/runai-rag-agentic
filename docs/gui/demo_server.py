"""Loopback-only fictional GUI preview; no databases, crawling or model calls.

Serves the real application static files with synthetic API responses. Mutating
collection/source operations are deliberately unavailable. Never deploy this
preview as the application API or expose it through an ingress.
"""
import argparse,json
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote,urlsplit

ROOT=Path(__file__).resolve().parents[2]
COLLECTION='atlas-reference'
SCHEDULE={'enabled':True,'timezone':'Etc/UTC','start':'02:00','end':'06:00'}
COLLECTIONS=[
 {'id':COLLECTION,'name':'Atlas Reference','status':'ready','elasticsearch_status':'ready','also_write_milvus':True,'milvus_status':'ready','documents':36,'published_documents':36,'generation':3,'caption_pending':2,'caption_failed':0,'processing':{'captions_pending':1,'captions_awaiting_publication':1,'captions_generated':11,'publication_pending':True,'publication_pending_operations':1},'image_origins':{'webpage':{'unique_images':6,'associations':8,'pending':2,'completed':6,'queued':1,'generating':0,'generated':7,'publishing':1},'pdf':{'unique_images':3,'associations':4,'pending':0,'completed':4,'queued':0,'generating':0,'generated':4,'publishing':0}}},
 {'id':'service-bulletins','name':'Service Bulletins','status':'ready','elasticsearch_status':'ready','also_write_milvus':False,'milvus_status':'not_enabled','documents':12,'published_documents':12,'generation':1,'caption_pending':0,'caption_failed':0}]
SOURCES=[
 {'source_id':'atlas-website','kind':'website','name':'Atlas documentation website','version':2,'seed_urls':['https://docs.example.com/atlas/'],'policy':{'allowed_hosts':['docs.example.com','documents.example.com'],'page_prefixes':['https://docs.example.com/atlas/'],'max_depth':None,'max_pages':None,'include_images':True,'prefer_https':True,'included_extensions':['.pdf','.docx','.pptx','.csv','.xls','.xlsx','.md','.txt'],'ignored_extensions':['.gif','.avif']},'schedule':SCHEDULE,'paused':False,'captions_enabled':True,'next_fire':'2026-12-02T02:00:00Z'},
 {'source_id':'atlas-directory','kind':'location','name':'Atlas service guides','version':1,'location':{'uri':'location://atlas-documents/service-guides/','recursive':True,'max_depth':4,'max_files':128},'schedule':SCHEDULE,'paused':False,'captions_enabled':True,'next_fire':'2026-12-02T02:00:00Z'},
 {'source_id':'atlas-document-url','kind':'location','name':'Atlas release notes PDF','version':1,'location':{'uri':'https://documents.example.com/atlas/release-notes.pdf','allowed_hosts':['documents.example.com']},'schedule':{**SCHEDULE,'enabled':False},'paused':False,'captions_enabled':False}]
RUNS={'atlas-website':[{'run_id':'website-check-illustration','state':'published','nominal_fire':'2026-12-01T02:00:00Z','counts':{'pages_captured':18,'pages_published':18,'assets_discovered':20,'assets_downloaded':14,'captions_generated':11,'captions_published':10}}], 'atlas-directory':[{'run_id':'directory-check-illustration','state':'unchanged','nominal_fire':'2026-12-01T02:00:00Z','counts':{'new':0,'changed':0,'unchanged':8,'missing':0,'failed':0}}], 'atlas-document-url':[{'run_id':'document-check-illustration','state':'published','nominal_fire':'2026-12-01T03:00:00Z','counts':{'new':1,'changed':0,'unchanged':0,'missing':0,'failed':0}}]}
JOB={'id':'atlas-demo-ingestion','collection_id':COLLECTION,'relevant':True,'status':'processing','stage':'Page text published; linked assets and optional captions continue independently','documents':[{'name':'https://docs.example.com/atlas/commissioning/','status':'completed','records':1},{'name':'Atlas-Operator-Guide.pdf','status':'completed','records':8}],'assets':[{'url':'https://documents.example.com/atlas/acceptance-form.pdf','kind':'binary','status':'completed'},{'url':'https://docs.example.com/atlas/images/control-panel.png','kind':'image','status':'processing'}],'captions':{'pending':1,'completed':1,'failed':0,'generated':1,'generating':0,'publishing':0},'images':[{'name':'Atlas control panel','status':'queued'},{'name':'Atlas inspection diagram','status':'completed'}],'operations':[{'target':'elasticsearch','state':'verified'},{'target':'milvus','state':'verified'}],'errors':[]}
ANSWER={'answer':'The Atlas Operator Guide contains the commissioning checklist. The acceptance form is linked from the commissioning page, and the Service Notes describe recent revisions. [S1] [S2]\n\nUse the guide for the procedure and the latest notes to check which revision applies.','sources':[{'citation_id':'S1','title':'Atlas commissioning checklist','uri':'https://docs.example.com/atlas/commissioning/'},{'citation_id':'S2','title':'Atlas Service Notes','uri':'https://documents.example.com/atlas/service-notes.pdf','page':2}],'warnings':[]}


RESEARCH={'id':'atlas-research','status':'completed','question':'Compare the Atlas commissioning requirements across the selected guides.','backend':'elasticsearch','collection_ids':[COLLECTION],'search_budget':6,'queries':['Find commissioning requirements.','Compare the guide with the service notes.'],'completed_queries':['Find commissioning requirements.','Compare the guide with the service notes.'],'sources':ANSWER['sources'],'warnings':['Illustrative local research report; no internet access.'],'report':'The Operator Guide defines the commissioning sequence; the Service Notes describe changes to the acceptance checks. [S1] [S2]\n\nConfirm the document revision before applying either checklist.','error_type':None,'attempts':1,'event_cursor':12,'expires_at':'2026-12-08T02:00:00Z'}
DOCUMENT={'id':'atlas-private-guide','filename':'Atlas-Operator-Guide.pdf','status':'published','ready':True,'records':8,'expires_at':'2026-12-02T02:00:00Z','captions':{'generated':3,'published':3,'pending':0},'promotions':[], 'run':{**RESEARCH,'id':'atlas-private-answer','question':'Which checks are required before commissioning?','search_budget':1,'queries':['Which checks are required before commissioning?'],'completed_queries':['Which checks are required before commissioning?'],'sources':[{'citation_id':'S1','title':'Atlas-Operator-Guide.pdf','uri':'https://documents.example.com/atlas/operator-guide.pdf','page':3}],'report':'Complete the inspection checklist, confirm the equipment configuration and record the acceptance result before commissioning. [S1]'}}

class Handler(BaseHTTPRequestHandler):
    authenticated=False
    def send(self,value,status=200):
        raw=json.dumps(value).encode();self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
    def do_GET(self):
        path=unquote(urlsplit(self.path).path)
        if path=='/favicon.ico':self.send_response(204);self.end_headers();return
        if path=='/api/status':return self.send({'status':'demo_preview'})
        if path=='/api/collections':return self.send({'collections':COLLECTIONS})
        if path=='/api/research/runs':return self.send({'runs':[RESEARCH],'capabilities':{'web_search':True}})
        if path=='/api/research/runs/atlas-research':return self.send(RESEARCH)
        if path=='/api/analysis/workspaces':return self.send({'workspaces':[DOCUMENT]})
        if path=='/api/analysis/workspaces/atlas-private-guide':return self.send(DOCUMENT)
        if path=='/api/session/collection-destinations':return self.send({'collections':[{'id':COLLECTION,'name':'Atlas Reference'}]})
        if path=='/api/location-roots':return self.send({'roots':[{'alias':'atlas-documents','uri':'location://atlas-documents/'}]})
        if path=='/api/session':return self.send({'authenticated':Handler.authenticated,'username':'demo-admin' if Handler.authenticated else None,'csrf_token':'fictional-preview-only'})
        if path=='/api/admin/locations':return self.send({'locations':[{'alias':'atlas-documents','path':'/source-roots/approved/atlas','enabled':True,'version':1}],'mounted_directories':['/source-roots/approved']})
        if path==f'/api/jobs/{JOB["id"]}':return self.send(JOB)
        if path.startswith('/api/collections/'):
            parts=path.split('/')
            if path.endswith('/sources'):
                return self.send({'sources':SOURCES if COLLECTION in parts else [],'uploads':[{'filename':'Atlas-Operator-Guide.pdf','status':'published'},{'filename':'Safety-Checklist.pdf','status':'published'}] if COLLECTION in parts else [],'one_time_sources':[]})
            if path.endswith('/runs'):return self.send({'runs':RUNS.get(parts[-2],[])})
        if path=='/' or path.startswith('/static/'):
            static=ROOT/'application/static';file=static/('index.html' if path=='/' else path[len('/static/'):])
            if not file.resolve().is_relative_to(static.resolve()) or not file.is_file():return self.send({'detail':'Not found'},404)
            raw=file.read_bytes();self.send_response(200);self.send_header('Content-Type',{'.html':'text/html','.css':'text/css','.js':'application/javascript'}.get(file.suffix,'application/octet-stream'));self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw);return
        self.send({'detail':'Unknown fictional preview endpoint'},404)
    def do_POST(self):
        path=urlsplit(self.path).path
        length=int(self.headers.get('Content-Length','0'));raw=self.rfile.read(length)
        body=json.loads(raw) if raw and path=='/api/search/stream' else {}
        if path=='/api/session/login':Handler.authenticated=True;return self.send({'authenticated':True,'username':'demo-admin','csrf_token':'fictional-preview-only'})
        if path=='/api/session/logout':Handler.authenticated=False;return self.send({'authenticated':False,'csrf_token':'fictional-preview-only'})
        if path=='/api/search/stream':
            self.send_response(200);self.send_header('Content-Type','text/event-stream');self.end_headers()
            answer=ANSWER if body.get('collection_ids') else {'answer':'This workspace can organize product guides, connect approved sources and help you find information in the collections you choose.','sources':[],'warnings':[]}
            answer={**answer,'routing':{'tier':'small','model':'nvidia/nemotron-3.5-lightning','response_header_ms':120,'illustrative':True}}
            for event,value in [('status',{'stage':'generating'}),('delta',{'text':answer['answer']}),('complete',answer)]:self.wfile.write(('event: '+event+'\ndata: '+json.dumps(value)+'\n\n').encode());self.wfile.flush()
            return
        self.send({'detail':'Visual preview only; this action does not create or change data.'},405)
    def do_PATCH(self):self.send({'detail':'Visual preview only; changes are not persisted.'},405)
    def do_DELETE(self):self.send({'detail':'Visual preview only; deletion is unavailable.'},405)

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--port',type=int,default=8099);a=p.parse_args()
    print(f'Fictional GUI preview: http://127.0.0.1:{a.port} (no live backend)',flush=True)
    ThreadingHTTPServer(('127.0.0.1',a.port),Handler).serve_forever()

if __name__=='__main__':main()
