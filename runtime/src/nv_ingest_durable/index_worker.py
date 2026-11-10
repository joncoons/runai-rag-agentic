"""Runnable optimized Elasticsearch INDEX worker."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
import logging
import os
from pathlib import Path
import re
import signal
import socket
from typing import Any
from uuid import uuid4

from .artifacts import FileArtifactStore
from .elasticsearch_writer import (
    ElasticsearchBulkClient,
    ElasticsearchMicrobatcher,
    JsonIndexReceiptStager,
    LocalEsReadyArtifactLoader,
    OptimizedElasticsearchStageProcessor,
    OptimizedWriterSettings,
)
from .observability import IndexWorkerMetrics
from .postgres import PostgresManifestRepository
from .redis_streams import RedisStreamsTransport
from .service import StageWorkerService
from .worker import DurableStageWorker


logger = logging.getLogger(__name__)
_INDEX_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,254}$")


@dataclass(frozen=True, slots=True)
class IndexWorkerSettings:
    postgres_dsn: str
    redis_url: str
    artifact_root: Path
    index: str
    stream: str = "ingest.index"
    consumer_group: str = "index-writers-v1"
    consumer: str = ""
    max_concurrency: int = 16
    read_count: int = 16
    block_ms: int = 5000
    reclaim_idle_ms: int = 900_000
    reclaim_count: int = 20
    lease_seconds: int = 600
    heartbeat_seconds: float = 30.0
    max_attempts: int = 5
    health_host: str = "0.0.0.0"
    health_port: int = 8084

    @classmethod
    def from_env(cls) -> "IndexWorkerSettings":
        env = os.environ
        consumer = env.get("INDEX_WORKER_CONSUMER") or (
            f"{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}"
        )
        return cls(
            postgres_dsn=env["POSTGRES_DSN"],
            redis_url=env["REDIS_URL"],
            artifact_root=Path(env["ARTIFACT_ROOT"]),
            index=env["APP_VECTORSTORE_INDEX"],
            stream=env.get("INDEX_WORKER_STREAM", "ingest.index"),
            consumer_group=env.get(
                "INDEX_WORKER_CONSUMER_GROUP", "index-writers-v1"
            ),
            consumer=consumer,
            max_concurrency=int(env.get("INDEX_WORKER_CONCURRENCY", "16")),
            read_count=int(env.get("INDEX_WORKER_READ_COUNT", "16")),
            block_ms=int(env.get("INDEX_WORKER_BLOCK_MS", "5000")),
            reclaim_idle_ms=int(
                env.get("INDEX_WORKER_RECLAIM_IDLE_MS", "900000")
            ),
            reclaim_count=int(env.get("INDEX_WORKER_RECLAIM_COUNT", "20")),
            lease_seconds=int(env.get("INDEX_WORKER_LEASE_SECONDS", "600")),
            heartbeat_seconds=float(
                env.get("INDEX_WORKER_HEARTBEAT_SECONDS", "30")
            ),
            max_attempts=int(env.get("INDEX_WORKER_MAX_ATTEMPTS", "5")),
            health_host=env.get("INDEX_WORKER_HEALTH_HOST", "0.0.0.0"),
            health_port=int(env.get("INDEX_WORKER_HEALTH_PORT", "8084")),
        )

    def __post_init__(self) -> None:
        if not self.postgres_dsn or not self.redis_url or not self.index:
            raise ValueError("PostgreSQL, Redis, and Elasticsearch index are required")
        if not _INDEX_RE.fullmatch(self.index):
            raise ValueError("invalid Elasticsearch index name")
        if self.max_concurrency < 1 or self.read_count < 1:
            raise ValueError("worker concurrency and read count must be positive")
        if self.read_count > self.max_concurrency:
            raise ValueError("read_count must not exceed max_concurrency")
        if self.heartbeat_seconds <= 0 or self.lease_seconds <= 0:
            raise ValueError("lease and heartbeat must be positive")
        if self.heartbeat_seconds * 3 > self.lease_seconds:
            raise ValueError("heartbeat must be no more than one third of lease")
        if self.reclaim_idle_ms <= self.heartbeat_seconds * 3000:
            raise ValueError("reclaim idle time must exceed three heartbeats")


class WorkerHealthServer:
    def __init__(
        self,
        pool: Any,
        redis_client: Any,
        *,
        host: str,
        port: int,
        metrics: IndexWorkerMetrics,
        stream: str,
        group: str,
    ) -> None:
        self.pool = pool
        self.redis_client = redis_client
        self.host = host
        self.port = port
        self.metrics = metrics
        self.stream = stream
        self.group = group
        self._runner: Any = None

    async def start(self) -> None:
        from aiohttp import web

        async def live(_request):
            return web.json_response({"status": "ok"})

        async def ready(_request):
            try:
                await self.pool.execute("SELECT 1")
                await self.redis_client.ping()
            except Exception:
                return web.json_response({"status": "not ready"}, status=503)
            return web.json_response({"status": "ready"})

        async def metrics(_request):
            await self.metrics.refresh_durable_state(
                self.pool,
                self.redis_client,
                stream=self.stream,
                group=self.group,
            )
            return web.Response(
                body=self.metrics.render(),
                headers={"Content-Type": "text/plain; version=0.0.4"},
            )

        app = web.Application()
        app.router.add_get("/health/live", live)
        app.router.add_get("/health/ready", ready)
        app.router.add_get("/metrics", metrics)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        await web.TCPSite(self._runner, self.host, self.port).start()

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None


async def run_index_worker() -> None:
    import asyncpg

    runtime = IndexWorkerSettings.from_env()
    writer_settings = OptimizedWriterSettings.from_env()
    artifact_root = runtime.artifact_root.resolve()
    artifact_root.mkdir(parents=True, exist_ok=True)

    pool = await asyncpg.create_pool(
        runtime.postgres_dsn,
        min_size=1,
        max_size=runtime.max_concurrency,
    )
    transport = RedisStreamsTransport.from_url(runtime.redis_url)
    metrics = IndexWorkerMetrics()
    bulk_client = ElasticsearchBulkClient(writer_settings, metrics=metrics)
    batcher = ElasticsearchMicrobatcher(bulk_client)
    health = WorkerHealthServer(
        pool,
        transport.client,
        host=runtime.health_host,
        port=runtime.health_port,
        metrics=metrics,
        stream=runtime.stream,
        group=runtime.consumer_group,
    )
    manifests = PostgresManifestRepository(pool)
    processor = OptimizedElasticsearchStageProcessor(
        index=runtime.index,
        batcher=batcher,
        loader=LocalEsReadyArtifactLoader(artifact_root),
        receipt_stager=JsonIndexReceiptStager(artifact_root),
    )
    worker = DurableStageWorker(
        owner=runtime.consumer,
        manifests=manifests,
        artifacts=FileArtifactStore(artifact_root),
        processor=processor,
        transport=transport,
        lease_ttl=timedelta(seconds=runtime.lease_seconds),
        heartbeat_interval=runtime.heartbeat_seconds,
        max_attempts=runtime.max_attempts,
    )
    service = StageWorkerService(
        stream=runtime.stream,
        group=runtime.consumer_group,
        consumer=runtime.consumer,
        worker=worker,
        max_concurrency=runtime.max_concurrency,
        read_count=runtime.read_count,
        block_ms=runtime.block_ms,
        reclaim_idle_ms=runtime.reclaim_idle_ms,
        reclaim_count=runtime.reclaim_count,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for event in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(event, stop.set)

    try:
        await bulk_client.start()
        await bulk_client.verify_index(runtime.index)
        batcher.start()
        await health.start()
        logger.info(
            "index worker ready consumer=%s stream=%s group=%s index=%s",
            runtime.consumer,
            runtime.stream,
            runtime.consumer_group,
            runtime.index,
        )
        await service.run(stop)
    finally:
        await health.stop()
        await batcher.stop()
        await bulk_client.close()
        await transport.client.aclose()
        await pool.close()


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run_index_worker())
