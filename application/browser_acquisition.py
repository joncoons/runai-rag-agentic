"""Original crawler browser fallback behind the application's network boundary."""
import asyncio
from dataclasses import dataclass
import select
import socket
import socketserver
import threading
import time
from urllib.parse import urlsplit

@dataclass(frozen=True)
class BrowserResult:
    raw_bytes: bytes
    final_uri: str
    evidence: dict


def is_js_sparse(raw):
    # Keep the original local-origin detection, rather than inventing a JS test.
    from ingestion.queued_crawler_runtime.crawl import SimpleWebCrawler
    probe=object.__new__(SimpleWebCrawler);probe._selenium_content_threshold=300
    text=raw.decode('utf-8',errors='replace')
    if probe._is_js_sparse(text):return True
    from bs4 import BeautifulSoup
    soup=BeautifulSoup(raw,'html.parser');root=soup.select_one('main,article,[role=main]')
    if root is not None and soup.find('script') is not None:
        for node in root.find_all(['script','style','noscript','template']):node.decompose()
        return not root.get_text(' ',strip=True)
    return False


class _Proxy(socketserver.ThreadingTCPServer):
    allow_reuse_address=True
    daemon_threads=True

class _Forward(socketserver.StreamRequestHandler):
    rbufsize=0
    def handle(self):
        from .web_acquisition import validate_url,AcquisitionError,_public_address
        remote=None
        try:
            line=self.rfile.readline(65537).decode('latin1').strip()
            method,target,protocol=line.split(' ',2)
            headers=[]
            while True:
                value=self.rfile.readline(65537)
                if value in (b'\r\n',b'\n',b''):break
                headers.append(value)
            url=('https://'+target+'/') if method=='CONNECT' else target
            url=validate_url(url,self.server.policy)
            parsed=urlsplit(url);port=parsed.port or (443 if parsed.scheme=='https' else 80)
            answers=socket.getaddrinfo(parsed.hostname,port,type=socket.SOCK_STREAM)
            if not answers:raise AcquisitionError('Browser destination has no addresses')
            private=parsed.hostname.lower() in {h.lower() for h in self.server.policy.private_hosts}
            for family,kind,proto,_,address in answers:
                if not private:_public_address(address[0])
            family,kind,proto,_,address=answers[0]
            remote=socket.socket(family,kind,proto);remote.settimeout(self.server.policy.timeout_seconds);remote.connect(address)
            if method=='CONNECT':
                self.wfile.write(b'HTTP/1.1 200 Connection Established\r\n\r\n');self.wfile.flush()
            else:
                path=parsed.path or '/'
                if parsed.query:path+='?'+parsed.query
                remote.sendall((method+' '+path+' '+protocol+'\r\n').encode('latin1')+b''.join(h for h in headers if not h.lower().startswith(b'proxy-'))+b'\r\n')
            peers=(self.connection,remote)
            while True:
                ready,_,_=select.select(peers,[],[],self.server.policy.timeout_seconds)
                if not ready:break
                for source in ready:
                    data=source.recv(65536)
                    if not data:return
                    (remote if source is self.connection else self.connection).sendall(data)
        except (OSError,ValueError):
            try:self.wfile.write(b'HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n')
            except OSError:pass
        finally:
            if remote is not None:remote.close()


class BrowserRenderer:
    """One serialized original browser per crawl, not a browser per static page."""
    def __init__(self,policy):
        self.policy=policy;self.lock=asyncio.Lock();self.crawler=None;self.proxy=None;self.thread=None

    def _open(self,url):
        from ingestion.queued_crawler_runtime.crawl import SimpleWebCrawler
        self.proxy=_Proxy(('127.0.0.1',0),_Forward);self.proxy.policy=self.policy
        self.thread=threading.Thread(target=self.proxy.serve_forever,daemon=True);self.thread.start()
        crawler=SimpleWebCrawler(url,use_selenium=True,selenium_wait_timeout=20)
        crawler._browser_proxy='http://127.0.0.1:'+str(self.proxy.server_address[1])
        crawler._selenium_driver=crawler._create_selenium_driver()
        self.crawler=crawler
        if crawler._selenium_driver is None:
            raise RuntimeError('Original Selenium driver could not start')

    def _render(self,url):
        from .web_acquisition import AcquisitionError,validate_url,html_in_scope
        if self.crawler is None:self._open(url)
        crawler=self.crawler
        if not crawler._render_with_selenium(url):raise AcquisitionError('Selenium rendering failed')
        driver=crawler._selenium_driver
        # Preserve the original network wait and add a content readiness gate:
        # network-idle alone can precede a framework's DOM update.
        deadline=time.monotonic()+20
        while True:
            ready=driver.execute_script("const e=document.querySelector('main,article,[role=main]')||document.body;return e && e.innerText.trim().length>0")
            if ready:break
            if time.monotonic()>=deadline:raise AcquisitionError('Rendered page has no meaningful main content after wait')
            time.sleep(.25)
        final=validate_url(driver.current_url,self.policy)
        if not html_in_scope(final,self.policy):raise AcquisitionError('Browser navigation left HTML section scope')
        raw=driver.page_source.encode('utf-8')
        if len(raw)>self.policy.max_response_bytes:raise AcquisitionError('Rendered DOM exceeds acquisition byte bound')
        return BrowserResult(raw,final,{'engine':'original-crawler-selenium','wait_timeout_seconds':20,'readiness':'original-fetch-xhr-idle-plus-nonempty-main','representation':'rendered-dom'})

    async def __call__(self,url):
        async with self.lock:
            worker=asyncio.create_task(asyncio.to_thread(self._render,url))
            try:
                return await asyncio.shield(worker)
            except asyncio.CancelledError:
                # asyncio cannot stop Selenium's thread. Retain browser ownership
                # until it exits, before quit/another get can use the driver.
                try:await worker
                finally:raise
            except Exception as error:
                from .web_acquisition import AcquisitionError
                if isinstance(error,AcquisitionError):raise
                raise AcquisitionError('Browser rendering failed: '+type(error).__name__) from error

    async def close(self):
        async with self.lock:
            def close():
                try:
                    if self.crawler and self.crawler._selenium_driver:self.crawler._selenium_driver.quit()
                finally:
                    if self.crawler and getattr(self.crawler,'_browser_profile_dir',None):
                        import shutil
                        shutil.rmtree(self.crawler._browser_profile_dir,ignore_errors=True)
                    if self.proxy:self.proxy.shutdown();self.proxy.server_close()
            await asyncio.to_thread(close)
