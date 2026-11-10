"""Asynchronous per-image caption/embed worker using existing durable stages."""
from .streams import stream_name
import asyncio
import base64
from dataclasses import replace
from datetime import timedelta
from io import BytesIO
import json
import hashlib
import os
from pathlib import Path
import signal
import socket

from .contracts import Stage,StageResult,WorkerOutcome
from .artifacts import FileArtifactStore,JsonResultStager
from .postgres import PostgresManifestRepository
from .redis_streams import RedisStreamsTransport
from .worker import DurableStageWorker,OutboxRelay
from .service import StageWorkerService
from .caption_repository import CaptionManifestRepository,PermanentImageError
from .deferred_caption import verified_file,canonical,digest


class CaptionProcessor:
    def __init__(self,root,caption_params,embedding,model_digest,source_context=None):
        self.root=Path(root);self.params=caption_params;self.embedding=embedding;self.model_digest=model_digest;self.source_context=source_context
        self.stager=JsonResultStager(self.root)
        self.embedding_contract={'dimensions':2048,'dtype':'float32','normalization':'unit','metric':'IP','schema_version':1,'embedding_checkpoint_sha256':os.environ.get('EMBED_MODEL_DIGEST','').removeprefix('sha256:')}

    async def process(self,envelope):
        return await self._process(envelope)

    async def _process(self,envelope):
        import pandas as pd
        from PIL import Image
        from nemo_retriever.operators.extract.caption.caption import CaptionCPUActor
        from .elasticsearch_writer import compact_checkpoint,validate_documents
        # The retained manifest is content-addressed by its filename.
        manifest_sha=Path(envelope.artifact_uri).name.split('.')[0]
        manifest=json.loads(await asyncio.to_thread(verified_file,self.root,envelope.artifact_uri,manifest_sha))
        if manifest['model_name']!=self.params.model_name or manifest['document_id']!=envelope.document_id or manifest['parent_job_id']!=envelope.job_id or manifest['model_digest']!=envelope.model_digest or envelope.model_digest!=self.model_digest:
            raise PermanentImageError('Caption job/model identity mismatch')
        if 'sha256:'+digest(canonical(manifest['caption_config']))!=envelope.config_digest:
            raise PermanentImageError('Retained caption configuration digest differs')
        if manifest.get('asset_validation_error'):
            raise PermanentImageError('Image encoding or crop is invalid')
        try:
            raw=await asyncio.to_thread(verified_file,self.root,manifest['asset_uri'],manifest['asset_sha256'])
            with Image.open(BytesIO(raw)) as image:image.verify()
        except Exception as exc:
            raise PermanentImageError('Retained image is corrupt or unsupported') from exc
        source_scope=await self.source_context(envelope) if self.source_context else {}
        item={'image_b64':base64.b64encode(raw).decode(),'text':''}
        actor=CaptionCPUActor(self.params.model_copy(update={**manifest['caption_config'],'caption_infographics':False,'batch_size':1}))
        response=await asyncio.to_thread(actor.process,pd.DataFrame([{'images':[item]}]))
        caption=response.iloc[0]['images'][0].get('text')
        if not isinstance(caption,str) or not caption.strip():raise RuntimeError('Caption endpoint returned an empty caption')
        metadata={'source':{'source_id':envelope.idempotency_key},'content_metadata':{'type':'image_caption','page_number':manifest['page_number'],'bbox':manifest['bbox'],'generated_annotation':True},'acl':source_scope.get('acl',[]),'caption_provenance':{'source_context':source_scope,'parent_job_id':envelope.job_id,'document_id':envelope.document_id,'parent_stage_key':manifest['parent_stage_key'],'asset_sha256':manifest['asset_sha256'],'asset_uri':manifest['asset_uri'],'source_metadata':manifest['source_metadata'],'model_digest':envelope.model_digest,'config_digest':envelope.config_digest}}
        result=await asyncio.to_thread(self.embedding.process,pd.DataFrame([{'text':caption,'metadata':metadata,'_content_type':'image_caption'}]))
        rows=result.to_dict(orient='records')
        checkpoint=compact_checkpoint(rows,envelope.document_id);validate_documents(checkpoint['documents'],expected_dimensions=2048)
        checkpoint['embedding_contract']=self.embedding_contract
        checkpoint['embedding_config_digest']=os.environ.get('EMBED_CONFIG_DIGEST','')
        checkpoint['caption_text_sha256']=hashlib.sha256(checkpoint['documents'][0]['text'].encode()).hexdigest()
        checkpoint['caption_asset_sha256']=manifest['asset_sha256'];checkpoint['publication_admitted']=False
        artifact,_=await self.stager.stage(envelope,checkpoint)
        ready=replace(envelope,stage=Stage.INDEX,artifact_uri=artifact.final_uri)
        return StageResult(artifact=artifact,downstream=((stream_name('caption.results'),ready),))


class CaptionStageWorker:
    """Acknowledge only after a durable retry or terminal disposition exists."""
    def __init__(self,worker,repository):self.worker=worker;self.repository=repository;self.transport=worker.transport
    async def handle(self,message):
        if await self.repository.terminal(message):
            await self.transport.acknowledge(message);return WorkerOutcome.DEADLETTERED
        try:outcome=await self.worker.handle(message)
        except PermanentImageError as error:
            if await self.repository.deadletter(message,error=error,target_stream=stream_name('caption.deadletter')):
                await self.transport.acknowledge(message);return WorkerOutcome.DEADLETTERED
            return WorkerOutcome.LEASE_BUSY
        if outcome==WorkerOutcome.FAILED:
            await self.repository.terminal(message)
            # record_failure atomically persisted either the next outbox event
            # or terminal policy. The consumed notification is now redundant.
            await self.transport.acknowledge(message)
        return outcome


async def run():
    import asyncpg
    from .pipeline_worker import _model_params,_digest,HealthServer
    from .embedding import RemoteEmbedding
    root=Path(os.environ['ARTIFACT_ROOT']);root.mkdir(parents=True,exist_ok=True)
    pool=await asyncpg.create_pool(os.environ['POSTGRES_DSN'],min_size=1,max_size=4)
    transport=RedisStreamsTransport.from_url(os.environ['REDIS_URL'])
    repository=CaptionManifestRepository(PostgresManifestRepository(pool),root)
    await pool.fetchval('SELECT count(*) FROM caption_control.assets')
    params,embed=_model_params()
    embedding=RemoteEmbedding(embed,checkpoint=os.environ['EMBED_CHECKPOINT_DIR'],expected_files=json.loads(Path(os.environ.get('EMBED_INPUT_CONTRACT', str(Path(__file__).with_name('embedding-input-contract.json')))).read_text()))
    processor=CaptionProcessor(root,params,embedding,_digest("CAPTION_MODEL_DIGEST"),repository.source_context)
    consumer=socket.gethostname()+'-caption'
    native=DurableStageWorker(owner=consumer,manifests=repository,artifacts=FileArtifactStore(root),processor=processor,transport=transport,lease_ttl=timedelta(seconds=180),heartbeat_interval=20,max_attempts=4,deadletter_stream=stream_name('caption.deadletter'))
    worker=CaptionStageWorker(native,repository)
    service=StageWorkerService(stream=stream_name('caption.pending'),group='caption-v1',consumer=consumer,worker=worker,max_concurrency=1,read_count=1,block_ms=500,reclaim_idle_ms=30000)
    relay=OutboxRelay(repository,transport);stop=asyncio.Event()
    loop=asyncio.get_running_loop()
    for event in (signal.SIGINT,signal.SIGTERM):loop.add_signal_handler(event,stop.set)
    health=HealthServer(pool,transport.client,int(os.environ.get('CAPTION_HEALTH_PORT','8084')))
    async def relay_loop():
        while not stop.is_set():
            await relay.publish_once()
            try:await asyncio.wait_for(stop.wait(),timeout=.5)
            except TimeoutError:pass
    try:
        await health.start()
        await asyncio.gather(service.run(stop),relay_loop())
    finally:
        stop.set();await health.stop();await transport.client.aclose();await pool.close()


def main():asyncio.run(run())
if __name__=='__main__':main()
