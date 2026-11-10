"""Prometheus metrics for the durable Elasticsearch INDEX worker."""

from __future__ import annotations

import time
from typing import Any


class IndexWorkerMetrics:
    def __init__(self, registry: Any = None) -> None:
        from prometheus_client import (
            CollectorRegistry,
            Counter,
            Gauge,
            Histogram,
            REGISTRY,
        )

        target = registry or REGISTRY
        self.registry = target
        self.bulk_requests = Counter(
            "nv_ingest_index_bulk_requests_total",
            "Elasticsearch bulk transport requests.",
            ("outcome",),
            registry=target,
        )
        self.bulk_documents = Counter(
            "nv_ingest_index_bulk_documents_total",
            "Documents returned by Elasticsearch bulk transport.",
            ("outcome",),
            registry=target,
        )
        self.bulk_bytes = Histogram(
            "nv_ingest_index_bulk_request_bytes",
            "Exact serialized NDJSON bytes per bulk request.",
            buckets=(10_000, 100_000, 500_000, 1_000_000, 2_500_000, 5_000_000),
            registry=target,
        )
        self.bulk_seconds = Histogram(
            "nv_ingest_index_bulk_request_seconds",
            "End-to-end bulk request latency.",
            registry=target,
        )
        self.retries = Counter(
            "nv_ingest_index_retries_total",
            "Bulk retry events.",
            ("scope",),
            registry=target,
        )
        self.microbatch_requests = Histogram(
            "nv_ingest_index_microbatch_source_requests",
            "Source INDEX envelopes coalesced into a microbatch.",
            buckets=(1, 2, 4, 8, 16, 32, 64, 100),
            registry=target,
        )
        self.microbatch_documents = Histogram(
            "nv_ingest_index_microbatch_documents",
            "ES documents in a microbatch.",
            buckets=(1, 10, 25, 50, 75, 100, 200, 500),
            registry=target,
        )
        self.microbatch_seconds = Histogram(
            "nv_ingest_index_microbatch_seconds",
            "Microbatch flush latency including ES persistence.",
            registry=target,
        )
        self.stream_lag = Gauge(
            "nv_ingest_index_stream_lag",
            "Redis consumer-group lag for the INDEX stream.",
            registry=target,
        )
        self.stream_pending = Gauge(
            "nv_ingest_index_stream_pending",
            "Pending entries in the INDEX consumer group.",
            registry=target,
        )
        self.oldest_pending_seconds = Gauge(
            "nv_ingest_index_oldest_pending_seconds",
            "Idle age of the oldest pending INDEX entry.",
            registry=target,
        )
        self.unpublished_outbox = Gauge(
            "nv_ingest_index_unpublished_outbox",
            "Unpublished INDEX outbox rows.",
            registry=target,
        )
        self.oldest_outbox_seconds = Gauge(
            "nv_ingest_index_oldest_outbox_seconds",
            "Age of the oldest unpublished INDEX outbox row.",
            registry=target,
        )
        self.unreplayed_deadletters = Gauge(
            "nv_ingest_index_unreplayed_deadletters",
            "Unreplayed INDEX dead letters.",
            registry=target,
        )

    def observe_bulk(
        self, *, bytes_sent: int, seconds: float, succeeded: int, failed: int
    ) -> None:
        outcome = "complete" if failed == 0 else "partial"
        self.bulk_requests.labels(outcome).inc()
        self.bulk_bytes.observe(bytes_sent)
        self.bulk_seconds.observe(seconds)
        if succeeded:
            self.bulk_documents.labels("succeeded").inc(succeeded)
        if failed:
            self.bulk_documents.labels("failed").inc(failed)

    def observe_microbatch(
        self, *, source_requests: int, documents: int, seconds: float
    ) -> None:
        self.microbatch_requests.observe(source_requests)
        self.microbatch_documents.observe(documents)
        self.microbatch_seconds.observe(seconds)

    async def refresh_durable_state(
        self, pool: Any, redis_client: Any, *, stream: str, group: str
    ) -> None:
        groups = await redis_client.xinfo_groups(stream)
        selected = next(
            (
                item
                for item in groups
                if _text(item.get(b"name", item.get("name", ""))) == group
            ),
            {},
        )
        lag = selected.get(b"lag", selected.get("lag", 0)) or 0
        pending = selected.get(b"pending", selected.get("pending", 0)) or 0
        self.stream_lag.set(float(lag))
        self.stream_pending.set(float(pending))

        oldest = await redis_client.xpending_range(
            stream, group, min="-", max="+", count=1
        )
        idle_ms = 0
        if oldest:
            item = oldest[0]
            idle_ms = item.get(
                b"time_since_delivered",
                item.get("time_since_delivered", 0),
            )
        self.oldest_pending_seconds.set(float(idle_ms or 0) / 1000.0)

        row = await pool.fetchrow(
            """
            SELECT count(*) AS count,
                   EXTRACT(EPOCH FROM now() - min(created_at)) AS oldest
              FROM outbox_events
             WHERE target_stream = $1 AND published_at IS NULL
            """,
            stream,
        )
        self.unpublished_outbox.set(float(row["count"] or 0))
        self.oldest_outbox_seconds.set(float(row["oldest"] or 0))
        deadletters = await pool.fetchval(
            """
            SELECT count(*) FROM deadletters
             WHERE replayed_at IS NULL AND envelope->>'stage' = 'index'
            """
        )
        self.unreplayed_deadletters.set(float(deadletters or 0))

    def render(self) -> bytes:
        from prometheus_client import generate_latest

        return generate_latest(self.registry)


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


class NullIndexWorkerMetrics:
    def observe_bulk(self, **_kwargs: Any) -> None:
        pass

    def observe_microbatch(self, **_kwargs: Any) -> None:
        pass

    async def refresh_durable_state(self, *_args: Any, **_kwargs: Any) -> None:
        pass

    def render(self) -> bytes:
        return b""
