import asyncio
import ssl
import aiohttp
import asyncpg
from .promotion_control import PromotionControl as ProvenanceRepository
from collection_control.incremental import IncrementalElasticsearch as ElasticsearchProjection,IncrementalMilvus as MilvusProjection
from collection_control.writer import GenerationWriter
from .settings import Settings

class Context:
    def __init__(self,settings):self.settings=settings
    async def open(self):
        for directory in [self.settings.root,self.settings.raw,self.settings.stages,self.settings.snapshots]:directory.mkdir(parents=True,exist_ok=True)
        self.pool=await asyncpg.create_pool(self.settings.dsn,min_size=1,max_size=8)
        self.http=aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=180))
        ssl_context=ssl.create_default_context(cafile=self.settings.es_ca) if self.settings.es_ca else None
        self.es_http=aiohttp.ClientSession(auth=aiohttp.BasicAuth(self.settings.es_user,self.settings.es_password),connector=aiohttp.TCPConnector(ssl=ssl_context),timeout=aiohttp.ClientTimeout(total=180))
        self.control=ProvenanceRepository(self.pool,handle_prefix='rag_agentic',incremental=True)
        self.stores={'elasticsearch':ElasticsearchProjection(self.es_http,self.settings.es_endpoint),'milvus':MilvusProjection(self.settings.milvus_uri,self.settings.milvus_token)}
        for store in self.stores.values():store.pool=self.pool
        self.writer=GenerationWriter(self.control,self.settings.snapshots,self.stores)
        return self
    async def close(self):
        private=getattr(self,'_private_context',None)
        if private is not None:await private.close()
        await self.http.close();await self.es_http.close();await self.pool.close()
