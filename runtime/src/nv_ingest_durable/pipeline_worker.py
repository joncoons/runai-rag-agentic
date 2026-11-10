"""Runnable fused NeMo Retriever 26.8.2 stage workers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import timedelta
import logging
import json
from functools import partial
import os
from pathlib import Path
import signal
import socket
from typing import Any
from urllib.parse import unquote, urlparse
from uuid import uuid4

from .artifacts import FileArtifactStore
from .streams import stream_name
from .elasticsearch_writer import EsReadyCheckpointStager
from .graph_adapter import NemoGraphStageProcessor, RegistryGraphRunner
from .nemo_blueprints import (
    BLUEPRINT_NAME,
    PdfExtractionBlueprintConfig,
    register_pdf_extraction_blueprint,
)
from .postgres import PostgresManifestRepository
from .redis_streams import RedisStreamsTransport
from .service import StageWorkerService
from .stage_processing import FusedCheckpointStager
from .worker import DurableStageWorker

logger = logging.getLogger(__name__)
ROLES = {"fused"}


@dataclass(frozen=True, slots=True)
class PipelineWorkerSettings:
    role: str
    postgres_dsn: str
    redis_url: str
    artifact_root: Path
    stream: str
    consumer_group: str
    consumer: str
    concurrency: int = 4
    read_count: int = 4
    block_ms: int = 5000
    reclaim_idle_ms: int = 900000
    lease_seconds: int = 600
    heartbeat_seconds: float = 30.0
    max_attempts: int = 5
    health_port: int = 8083
    caption_enabled: bool = True
    microbatch_size: int = 1
    microbatch_wait_ms: int = 0

    @classmethod
    def from_env(cls) -> "PipelineWorkerSettings":
        env = os.environ
        role = env.get("STAGE_WORKER_ROLE", "fused").lower()
        if role != "fused":
            raise ValueError(f"unknown worker role: {role}")
        stream, group = "ingest.extraction.pending", "fused-v1"
        return cls(
            role=role,
            postgres_dsn=env["POSTGRES_DSN"],
            redis_url=env["REDIS_URL"],
            artifact_root=Path(env["ARTIFACT_ROOT"]),
            stream=env.get("STAGE_WORKER_STREAM", stream),
            consumer_group=env.get("STAGE_WORKER_GROUP", group),
            consumer=env.get("STAGE_WORKER_CONSUMER")
            or f"{socket.gethostname()}-{uuid4().hex[:8]}",
            concurrency=int(env.get("STAGE_WORKER_CONCURRENCY", "4")),
            read_count=int(env.get("STAGE_WORKER_READ_COUNT", "4")),
            block_ms=int(env.get("STAGE_WORKER_BLOCK_MS", "5000")),
            reclaim_idle_ms=int(env.get("STAGE_WORKER_RECLAIM_IDLE_MS", "900000")),
            lease_seconds=int(env.get("STAGE_WORKER_LEASE_SECONDS", "600")),
            heartbeat_seconds=float(env.get("STAGE_WORKER_HEARTBEAT_SECONDS", "30")),
            max_attempts=int(env.get("STAGE_WORKER_MAX_ATTEMPTS", "5")),
            health_port=int(env.get("STAGE_WORKER_HEALTH_PORT", "8083")),
            caption_enabled=env.get("CAPTION_ENABLED", "true").lower()
            in {"1", "true", "yes", "on"},
            microbatch_size=int(env.get("STAGE_WORKER_MICROBATCH_SIZE", "1")),
            microbatch_wait_ms=int(env.get("STAGE_WORKER_MICROBATCH_WAIT_MS", "0")),
        )

    def __post_init__(self) -> None:
        if self.role not in ROLES:
            raise ValueError(f"unknown worker role: {self.role}")
        if (
            self.concurrency < 1
            or self.read_count < 1
            or self.read_count > self.concurrency
        ):
            raise ValueError(
                "read_count must be positive and no greater than concurrency"
            )
        if not 1 <= self.microbatch_size <= self.concurrency:
            raise ValueError("microbatch_size must be within concurrency")
        if self.microbatch_size > 1 and self.microbatch_wait_ms < 1:
            raise ValueError("microbatch_wait_ms must be positive for batching")
        if self.heartbeat_seconds * 3 > self.lease_seconds:
            raise ValueError("heartbeat must be no more than one third of lease")


def _digest(name: str) -> str:
    value = os.environ.get(name, "")
    if not value.startswith("sha256:"):
        raise ValueError(f"{name} must be a sha256 digest")
    return value


def _local_input(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise ValueError("extraction input requires a file:// artifact")
    return str(Path(unquote(parsed.path)))


async def _configuration(_envelope):
    return {}


def _extract_config() -> PdfExtractionBlueprintConfig:
    env = os.environ
    truthy = {"1", "true", "yes", "on"}
    return PdfExtractionBlueprintConfig(
        page_elements_url=env["PAGE_ELEMENTS_URL"],
        ocr_url=env["OCR_URL"],
        table_structure_url=env["TABLE_STRUCTURE_URL"],
        extract_images=True,
        extract_tables=env.get("EXTRACT_TABLES", "true").lower() in truthy,
        extract_charts=env.get("EXTRACT_CHARTS", "true").lower() in truthy,
        extract_infographics=env.get("EXTRACT_INFOGRAPHICS", "true").lower() in truthy,
        dpi=int(env.get("EXTRACTION_DPI", "200")),
        inference_batch_size=int(env.get("EXTRACTION_BATCH_SIZE", "8")),
    )


def _model_params():
    from nemo_retriever.common.params import CaptionParams, EmbedParams

    caption = CaptionParams(
        endpoint_url=os.environ["CAPTION_URL"],
        caption_infographics=os.environ.get("CAPTION_INFOGRAPHICS", "true").lower() in {"1", "true", "yes", "on"},
        model_name=os.environ["CAPTION_MODEL"],
        batch_size=int(os.environ.get("CAPTION_BATCH_SIZE", "8")),
        temperature=float(os.environ.get("CAPTION_TEMPERATURE", "0")),
        max_tokens=int(os.environ.get("CAPTION_MAX_TOKENS", "512")),
        extra_body=json.loads(os.environ.get("CAPTION_EXTRA_BODY", "{}")),
    )
    embedding = EmbedParams(
        embed_invoke_url=os.environ["EMBED_URL"],
        model_name=os.environ["EMBED_MODEL"],
        embed_model_name=os.environ["EMBED_MODEL"],
        inference_batch_size=int(os.environ.get("EMBED_BATCH_SIZE", "32")),
        dimensions=int(os.environ.get("EMBED_DIMENSIONS", "2048")),
    )
    return caption, embedding


def build_processor(settings: PipelineWorkerSettings, *, pool=None):
    root = settings.artifact_root
    embed_digest = _digest("EMBED_CONFIG_DIGEST")
    embed_model_digest = _digest("EMBED_MODEL_DIGEST")
    index_digest = _digest("INDEX_CONFIG_DIGEST")
    index_model_digest = _digest("INDEX_MODEL_DIGEST")

    from nemo_retriever.graph import InprocessExecutor
    from nemo_retriever.graph.graph_pipeline_registry import (
        GraphPipelineRegistry,
    )
    import nemo_retriever.graph.ingestor_runtime as runtime

    registry = GraphPipelineRegistry()
    caption_params, embed_params = _model_params()
    from .embedding import RemoteEmbedding
    from .deferred_caption import DeferredCaptionAssets,CaptionHandoffStager,reshape_source_content
    embedding = RemoteEmbedding(
        embed_params,
        checkpoint=os.environ["EMBED_CHECKPOINT_DIR"],
        expected_files=json.loads(Path(os.environ.get("EMBED_INPUT_CONTRACT", str(Path(__file__).with_name("embedding-input-contract.json")))).read_text()),
    )
    deferred = DeferredCaptionAssets(root, caption_params, _digest("CAPTION_MODEL_DIGEST"))
    input_resolver = _local_input

    extract_config = _extract_config()
    register_pdf_extraction_blueprint(registry, extract_config)

    graph_name = "durable-pdf-fused-26.8.2"

    def append_source_stages(graph):
        from .admission import require_successful_upstream
        graph = graph >> runtime.UDFOperator(
            require_successful_upstream, name="RequireSuccessfulExtraction",
        )
        from nemo_retriever.common.params import DedupParams
        order = ("dedup",) if settings.caption_enabled else ()
        graph = runtime._append_ordered_transform_stages(
            graph,
            dedup_params=DedupParams() if settings.caption_enabled else None,
            caption_params=None,
            store_params=None,
            embed_params=None,
            stage_order=order,
            supports_dedup=True,
            reshape_content_before_embed=True,
        )

        if settings.caption_enabled:
            graph = graph >> runtime.UDFOperator(deferred.process, name="RetainAndQueueCaptionAssets")
        graph = graph >> runtime.UDFOperator(
            partial(reshape_source_content, caption_enabled=settings.caption_enabled),
            name="ExplodeSourceContentToRows", preserve_pandas_output=True,
        )
        return graph >> runtime.UDFOperator(embedding.process, name="PinnedRemoteEmbedding")

    @registry.register(graph_name, version="26.8.2", overwrite=True)
    def fused_graph():
        return append_source_stages(registry.build(BLUEPRINT_NAME))

    tail_name = "durable-pdf-source-stages-26.8.2"

    @registry.register(tail_name, version="26.8.2", overwrite=True)
    def source_graph():
        return append_source_stages(runtime.Graph())

    stager = FusedCheckpointStager(
        EsReadyCheckpointStager(
            root,
            index_digest,
            index_model_digest,
            int(os.environ.get("EMBED_DIMENSIONS", "2048")),
            index_stream=os.environ.get("INDEX_RESULT_STREAM",stream_name("index")),
        ),
        embed_digest,
        embed_model_digest,
    )

    stager = CaptionHandoffStager(stager, root)

    runner = RegistryGraphRunner(
        registry,
        lambda graph: InprocessExecutor(graph, show_progress=False),
        input_resolver=input_resolver,
    )
    configuration_loader = _configuration
    if pool is not None:
        from application.pdf_extraction_cache import PdfExtractionCache, PdfReuseRunner, extraction_contract
        cache = PdfExtractionCache(pool, root)
        runner = PdfReuseRunner(runner, cache=cache,
            tail_factory=lambda: InprocessExecutor(registry.build(tail_name), show_progress=False),
            contract=extraction_contract(extract_config), raw_root=root.parent / "raw")

        async def configuration_loader(envelope):
            row = await pool.fetchrow("SELECT collection_id,raw_sha256,raw_uri,source_kind,page_count,document_id FROM application.source_revisions WHERE document_id=$1 AND job_id=$2",envelope.document_id,__import__("uuid").UUID(envelope.job_id))
            if row is None or row["source_kind"] != "pdf":
                raise ValueError("PDF extraction reuse requires a persisted PDF capture")
            return dict(row)

    return NemoGraphStageProcessor(
        graph_name,
        runner,
        configuration_loader,
        stager,
        batch_enabled=False,
    )


class HealthServer:
    def __init__(self, pool: Any, redis: Any, port: int) -> None:
        self.pool = pool
        self.redis = redis
        self.port = port
        self.runner: Any = None

    async def start(self) -> None:
        from aiohttp import web

        async def live(_request):
            return web.json_response({"status": "ok"})

        async def ready(_request):
            try:
                await self.pool.execute("SELECT 1")
                await self.redis.ping()
            except Exception:
                return web.json_response({"status": "not ready"}, status=503)
            return web.json_response({"status": "ready"})

        app = web.Application()
        app.router.add_get("/health/live", live)
        app.router.add_get("/health/ready", ready)
        self.runner = web.AppRunner(app, access_log=None)
        await self.runner.setup()
        await web.TCPSite(self.runner, "0.0.0.0", self.port).start()

    async def stop(self) -> None:
        if self.runner is not None:
            await self.runner.cleanup()


async def run_pipeline_worker() -> None:
    import asyncpg

    settings = PipelineWorkerSettings.from_env()
    settings.artifact_root.mkdir(parents=True, exist_ok=True)
    pool = await asyncpg.create_pool(
        settings.postgres_dsn,
        min_size=1,
        max_size=settings.concurrency,
    )
    transport = RedisStreamsTransport.from_url(settings.redis_url)
    health = HealthServer(pool, transport.client, settings.health_port)
    from .caption_repository import CaptionManifestRepository
    await pool.fetchval("SELECT count(*) FROM caption_control.assets")
    from .caption_policy import CaptionPolicyProcessor
    reuse = os.environ.get("PDF_EXTRACTION_REUSE", "false").lower() == "true"
    if reuse and os.environ.get("INGEST_STREAM_PREFIX") != "rag.agentic":
        raise ValueError("PDF extraction reuse is scoped to the agentic application")
    reuse_pool = pool if reuse else None
    processor = CaptionPolicyProcessor(pool, build_processor(settings,pool=reuse_pool), build_processor(replace(settings, caption_enabled=False),pool=reuse_pool), feature_allowed=settings.caption_enabled)
    worker = DurableStageWorker(
        owner=settings.consumer,
        manifests=CaptionManifestRepository(PostgresManifestRepository(pool), settings.artifact_root),
        artifacts=FileArtifactStore(settings.artifact_root),
        processor=processor,
        transport=transport,
        lease_ttl=timedelta(seconds=settings.lease_seconds),
        heartbeat_interval=settings.heartbeat_seconds,
        max_attempts=settings.max_attempts,
        deadletter_stream=stream_name("extraction.deadletter"),
    )
    service = StageWorkerService(
        stream=settings.stream,
        group=settings.consumer_group,
        consumer=settings.consumer,
        worker=worker,
        max_concurrency=settings.concurrency,
        read_count=settings.read_count,
        block_ms=settings.block_ms,
        reclaim_idle_ms=settings.reclaim_idle_ms,
        microbatch_size=settings.microbatch_size,
        microbatch_wait_ms=settings.microbatch_wait_ms,
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for event in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(event, stop.set)
    try:
        await health.start()
        logger.info(
            "%s worker ready stream=%s group=%s",
            settings.role,
            settings.stream,
            settings.consumer_group,
        )
        await service.run(stop)
    finally:
        await health.stop()
        await transport.client.aclose()
        await pool.close()


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run_pipeline_worker())


if __name__ == "__main__":
    main()
