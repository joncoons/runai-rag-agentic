"""Optimized, idempotent Elasticsearch bulk transport for the durable INDEX stage."""

from __future__ import annotations

import asyncio
import base64
import copy
from collections import Counter
from dataclasses import dataclass, field, replace
import hashlib
import json
import os
import random
import ssl
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Protocol
from urllib.parse import unquote, urlparse
from uuid import uuid5, NAMESPACE_URL

from .artifacts import JsonResultStager
from .observability import NullIndexWorkerMetrics
from .contracts import (
    PendingArtifact,
    Stage,
    StageEnvelope,
    StageResult,
    derive_idempotency_key,
)


TRANSIENT_STATUSES = frozenset({408, 429, 500, 502, 503, 504})


class ElasticsearchPayloadError(ValueError):
    """A payload cannot satisfy the qualified writer contract."""


class ElasticsearchWriteIncomplete(RuntimeError):
    """At least one deterministic ES identity did not become durable."""

    def __init__(self, report: "BulkWriteReport") -> None:
        super().__init__(
            f"Elasticsearch write incomplete: "
            f"{len(report.succeeded_ids)}/{len(report.expected_ids)} durable"
        )
        self.report = report


@dataclass(frozen=True, slots=True)
class OptimizedWriterSettings:
    endpoint: str
    expected_dimensions: int = 2048
    bulk_max_documents: int = 100
    bulk_max_bytes: int = 5_000_000
    bulk_max_concurrency: int = 2
    bulk_max_retries: int = 3
    retry_base_seconds: float = 0.5
    retry_max_seconds: float = 8.0
    request_timeout_seconds: float = 120.0
    microbatch_max_documents: int = 100
    microbatch_max_bytes: int = 5_000_000
    microbatch_flush_seconds: float = 0.025
    microbatch_queue_size: int = 1024
    username: str = ""
    password: str = ""
    verify_tls: bool = True
    ca_file: str = ""

    @classmethod
    def from_env(cls) -> "OptimizedWriterSettings":
        env = os.environ
        truthy = {"1", "true", "yes", "on"}
        return cls(
            endpoint=env["APP_VECTORSTORE_URL"],
            expected_dimensions=int(env.get("NRL_ES_EXPECTED_EMBEDDING_DIMENSIONS", "2048")),
            bulk_max_documents=int(env.get("APP_VECTORSTORE_BULK_BATCH_SIZE", "100")),
            bulk_max_bytes=int(env.get("APP_VECTORSTORE_BULK_MAX_BYTES", "5000000")),
            bulk_max_concurrency=int(env.get("APP_VECTORSTORE_BULK_MAX_CONCURRENCY", "2")),
            bulk_max_retries=int(env.get("APP_VECTORSTORE_BULK_MAX_RETRIES", "3")),
            retry_base_seconds=float(env.get("APP_VECTORSTORE_BULK_RETRY_BASE_SECONDS", "0.5")),
            retry_max_seconds=float(env.get("APP_VECTORSTORE_BULK_RETRY_MAX_SECONDS", "8.0")),
            microbatch_max_documents=int(env.get("NRL_ES_MICROBATCH_MAX_DOCS", "100")),
            microbatch_max_bytes=int(env.get("NRL_ES_MICROBATCH_MAX_BYTES", "5000000")),
            microbatch_flush_seconds=float(env.get("NRL_ES_MICROBATCH_FLUSH_SECONDS", "0.025")),
            microbatch_queue_size=int(env.get("NRL_ES_MICROBATCH_QUEUE_SIZE", "1024")),
            username=env.get("APP_VECTORSTORE_USERNAME", ""),
            password=env.get("APP_VECTORSTORE_PASSWORD", ""),
            verify_tls=env.get("APP_VECTORSTORE_VERIFY_CERTS", "true").lower() in truthy,
            ca_file=env.get("APP_VECTORSTORE_CA_CERTS", ""),
        )

    def __post_init__(self) -> None:
        if not self.endpoint.strip():
            raise ValueError("endpoint is required")
        integer_limits = (
            self.expected_dimensions,
            self.bulk_max_documents,
            self.bulk_max_bytes,
            self.bulk_max_concurrency,
            self.microbatch_max_documents,
            self.microbatch_max_bytes,
            self.microbatch_queue_size,
        )
        if any(value < 1 for value in integer_limits):
            raise ValueError("writer limits must be positive")
        if self.bulk_max_retries < 0:
            raise ValueError("bulk_max_retries must be non-negative")
        if self.microbatch_flush_seconds < 0:
            raise ValueError("microbatch_flush_seconds must be non-negative")


@dataclass(frozen=True, slots=True)
class BulkWriteReport:
    expected_ids: tuple[str, ...] = ()
    succeeded_ids: tuple[str, ...] = ()
    failed_items: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    duplicate_ids: tuple[str, ...] = ()
    schema_version: str = "1.0"

    @property
    def written_count(self) -> int:
        return len(self.succeeded_ids)

    @property
    def complete(self) -> bool:
        return (
            not self.failed_items
            and not self.duplicate_ids
            and set(self.expected_ids) == set(self.succeeded_ids)
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "expected_ids": list(self.expected_ids),
            "succeeded_ids": list(self.succeeded_ids),
            "failed_items": {
                key: dict(value) for key, value in self.failed_items.items()
            },
            "duplicate_ids": list(self.duplicate_ids),
            "written_count": self.written_count,
            "complete": self.complete,
        }


@dataclass(frozen=True, slots=True)
class SerializedBulkBatch:
    documents: tuple[Mapping[str, Any], ...]
    body: bytes

    @property
    def size_bytes(self) -> int:
        return len(self.body)


def elasticsearch_document_id(document: Mapping[str, Any]) -> str:
    source = document.get("metadata", {})
    source_id = (
        source.get("source", {}).get("source_id")
        if isinstance(source, Mapping)
        else None
    )
    if source_id:
        return str(source_id)
    encoded = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:32]


def _json_line(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8") + b"\n"


def _serialized_pair(index: str, document: Mapping[str, Any]) -> bytes:
    action = {
        "index": {
            "_index": index,
            "_id": elasticsearch_document_id(document),
        }
    }
    return _json_line(action) + _json_line(document)


def iter_bulk_batches(
    documents: list[Mapping[str, Any]],
    index: str,
    *,
    max_documents: int,
    max_bytes: int,
):
    """Yield exact serialized NDJSON batches within both hard ceilings."""

    batch: list[Mapping[str, Any]] = []
    parts: list[bytes] = []
    size = 0
    for document in documents:
        part = _serialized_pair(index, document)
        if len(part) > max_bytes:
            raise ElasticsearchPayloadError(
                f"single Elasticsearch document exceeds {max_bytes} serialized bytes"
            )
        if batch and (
            len(batch) >= max_documents or size + len(part) > max_bytes
        ):
            yield SerializedBulkBatch(tuple(batch), b"".join(parts))
            batch, parts, size = [], [], 0
        batch.append(document)
        parts.append(part)
        size += len(part)
    if batch:
        yield SerializedBulkBatch(tuple(batch), b"".join(parts))


def _embedding(
    row: Mapping[str, Any], metadata: Mapping[str, Any]
) -> list[float] | None:
    candidate = metadata.get("embedding")
    if candidate is None:
        payload = row.get("text_embeddings_1b_v2")
        candidate = payload.get("embedding") if isinstance(payload, Mapping) else None
    if not isinstance(candidate, list) or not candidate:
        return None
    return [float(value) for value in candidate]


def compact_nrl_rows(
    rows: list[Mapping[str, Any]], document_id: str
) -> list[dict[str, Any]]:
    """Convert full 26.5 rows into compact ES-ready durable records."""

    documents: list[dict[str, Any]] = []
    for position, row in enumerate(rows):
        metadata = dict(row.get("metadata") or {})
        vector = _embedding(row, metadata)
        text = row.get("text") or row.get("content") or metadata.get("content")
        if vector is None or not text:
            continue
        content = dict(metadata.get("content_metadata") or {})
        page = content.get("page_number", row.get("_page_number", row.get("page_number", metadata.get("page_number"))))
        if page is not None:
            content.setdefault("page_number", page)
        content_type = row.get("_content_type") or row.get("content_type")
        if content_type:
            content.setdefault("type", content_type)
        for field in ("has_text", "needs_ocr_for_text"):
            if field in metadata:
                content.setdefault(field, metadata[field])
            elif field in row:
                content.setdefault(field, row[field])
        bbox = row.get("_bbox_xyxy_norm")
        if isinstance(bbox, (list, tuple)):
            content["bbox"] = list(bbox)
        retained = copy.deepcopy(metadata)
        retained.pop("embedding", None)
        source = dict(retained.get("source") or {})
        source_path = str(
            metadata.get("source_path")
            or source.get("source_uri")
            or row.get("path")
            or row.get("source_id")
            or document_id
        )
        identity_material = {
            "document_id": document_id,
            "position": position,
            "path": source_path,
            "page": page,
            "text": str(text),
        }
        identity = hashlib.sha256(
            json.dumps(
                identity_material, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        ).hexdigest()[:32]
        documents.append(
            {
                "text": str(text),
                "vector": vector,
                "metadata": {
                    **retained,
                    "source": {
                        "source_id": identity,
                        "source_name": Path(source_path).name,
                        "source_uri": source_path,
                        **source,
                    },
                    "content_metadata": content,
                },
            }
        )
    return documents


def compact_checkpoint(
    rows: list[Mapping[str, Any]], document_id: str
) -> dict[str, Any]:
    documents = compact_nrl_rows(rows, document_id)
    return {
        "schema_version": "1.0",
        "payload_format": "es_docs",
        "document_id": document_id,
        "documents": documents,
    }


def validate_documents(
    documents: list[Mapping[str, Any]], *, expected_dimensions: int
) -> None:
    if not documents:
        raise ElasticsearchPayloadError("no ES-ready documents")
    for document in documents:
        vector = document.get("vector")
        if not isinstance(vector, list):
            raise ElasticsearchPayloadError("one or more documents lacked an embedding")
        if len(vector) != expected_dimensions:
            raise ElasticsearchPayloadError(
                f"embedding dimension {len(vector)} did not match "
                f"expected {expected_dimensions}"
            )


def _retry_after(value: str | None) -> float | None:
    try:
        return max(0.0, float(value)) if value else None
    except ValueError:
        return None


class ElasticsearchBulkClient:
    """Persistent ES client with bounded concurrency and selective item retries."""

    def __init__(
        self,
        settings: OptimizedWriterSettings,
        *,
        session: Any = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        metrics: Any = None,
    ) -> None:
        self.settings = settings
        self._session = session
        self._owns_session = session is None
        self._sleep = sleep
        self.metrics = metrics or NullIndexWorkerMetrics()
        self._semaphore = asyncio.Semaphore(settings.bulk_max_concurrency)

    async def start(self) -> None:
        if self._session is not None:
            return
        import aiohttp

        ssl_value: ssl.SSLContext | bool
        if not self.settings.verify_tls:
            ssl_value = False
        elif self.settings.ca_file:
            ssl_value = ssl.create_default_context(cafile=self.settings.ca_file)
        else:
            ssl_value = ssl.create_default_context()
        connector = aiohttp.TCPConnector(
            ssl=ssl_value,
            limit=max(8, self.settings.bulk_max_concurrency * 4),
            limit_per_host=max(4, self.settings.bulk_max_concurrency * 2),
            ttl_dns_cache=300,
        )
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(
                total=self.settings.request_timeout_seconds
            ),
        )

    async def close(self) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
        self._session = None

    async def verify_index(self, index: str) -> None:
        """Require a pre-created dense-vector index with the qualified dimension."""

        await self.start()
        index_url = f"{self.settings.endpoint.rstrip('/')}/{index}"
        async with self._session.head(
            index_url, headers=self._headers()
        ) as response:
            if int(response.status) == 404:
                raise ElasticsearchPayloadError(
                    f"Elasticsearch index does not exist: {index}"
                )
            if int(response.status) >= 400:
                raise RuntimeError(
                    f"Elasticsearch index check failed with HTTP {response.status}"
                )

        async with self._session.get(
            f"{index_url}/_mapping", headers=self._headers()
        ) as response:
            if int(response.status) >= 400:
                raise RuntimeError(
                    f"Elasticsearch mapping check failed with HTTP {response.status}"
                )
            mapping = await response.json()

        index_mapping = mapping.get(index) if isinstance(mapping, Mapping) else None
        if not isinstance(index_mapping, Mapping) and isinstance(mapping, Mapping):
            index_mapping = next(
                (value for value in mapping.values() if isinstance(value, Mapping)),
                None,
            )
        mappings = (
            index_mapping.get("mappings")
            if isinstance(index_mapping, Mapping)
            else None
        )
        properties = (
            mappings.get("properties") if isinstance(mappings, Mapping) else None
        )
        vector = properties.get("vector") if isinstance(properties, Mapping) else None
        dimensions = vector.get("dims") if isinstance(vector, Mapping) else None
        vector_type = vector.get("type") if isinstance(vector, Mapping) else None
        if (
            vector_type != "dense_vector"
            or dimensions != self.settings.expected_dimensions
        ):
            raise ElasticsearchPayloadError(
                f"index {index!r} vector mapping must be dense_vector with "
                f"{self.settings.expected_dimensions} dimensions"
            )

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/x-ndjson"}
        if self.settings.username:
            value = base64.b64encode(
                f"{self.settings.username}:{self.settings.password}".encode()
            ).decode()
            headers["Authorization"] = f"Basic {value}"
        return headers

    def _delay(self, attempt: int, retry_after: float | None) -> float:
        if retry_after is not None:
            return retry_after
        base = max(0.0, self.settings.retry_base_seconds)
        cap = max(base, self.settings.retry_max_seconds)
        delay = min(cap, base * (2**attempt))
        return delay + random.uniform(0.0, min(1.0, delay * 0.2))

    async def _post_batch(
        self, index: str, batch: SerializedBulkBatch
    ) -> BulkWriteReport:
        await self.start()
        started = time.monotonic()
        expected = [elasticsearch_document_id(doc) for doc in batch.documents]
        pending = list(batch.documents)
        succeeded: set[str] = set()
        failed: dict[str, Mapping[str, Any]] = {}

        for attempt in range(self.settings.bulk_max_retries + 1):
            if not pending:
                break
            body = (
                batch.body
                if attempt == 0 and len(pending) == len(batch.documents)
                else b"".join(_serialized_pair(index, doc) for doc in pending)
            )
            try:
                async with self._semaphore:
                    async with self._session.post(
                        f"{self.settings.endpoint.rstrip('/')}/_bulk",
                        data=body,
                        headers=self._headers(),
                    ) as response:
                        status = int(response.status)
                        retry_after = _retry_after(
                            response.headers.get("Retry-After")
                        )
                        if status in TRANSIENT_STATUSES:
                            if attempt < self.settings.bulk_max_retries:
                                await self._sleep(
                                    self._delay(attempt, retry_after)
                                )
                                continue
                            for document in pending:
                                failed[elasticsearch_document_id(document)] = {
                                    "status": status,
                                    "reason": "bulk_http_retry_exhausted",
                                    "retryable": True,
                                }
                            pending = []
                            break
                        if status >= 400:
                            for document in pending:
                                failed[elasticsearch_document_id(document)] = {
                                    "status": status,
                                    "reason": "bulk_http_permanent",
                                    "retryable": False,
                                }
                            pending = []
                            break
                        result = await response.json()
            except Exception as error:
                if attempt < self.settings.bulk_max_retries:
                    await self._sleep(self._delay(attempt, None))
                    continue
                for document in pending:
                    failed[elasticsearch_document_id(document)] = {
                        "status": None,
                        "reason": type(error).__name__,
                        "retryable": True,
                    }
                pending = []
                break

            if not result.get("errors"):
                succeeded.update(elasticsearch_document_id(doc) for doc in pending)
                pending = []
                break

            response_items = list(result.get("items") or [])
            retry_documents: list[Mapping[str, Any]] = []
            for position, document in enumerate(pending):
                document_id = elasticsearch_document_id(document)
                item = (
                    response_items[position]
                    if position < len(response_items)
                    and isinstance(response_items[position], Mapping)
                    else {}
                )
                operation = next(iter(item.values()), {}) if item else {}
                item_status = operation.get("status") if isinstance(operation, Mapping) else None
                try:
                    item_status = int(item_status)
                except (TypeError, ValueError):
                    item_status = None
                error = operation.get("error") if isinstance(operation, Mapping) else None
                if not error and item_status is not None and 200 <= item_status < 300:
                    succeeded.add(document_id)
                    failed.pop(document_id, None)
                    continue
                transient = item_status in TRANSIENT_STATUSES or item_status is None
                if transient and attempt < self.settings.bulk_max_retries:
                    retry_documents.append(document)
                    continue
                failed[document_id] = {
                    "status": item_status,
                    "reason": (
                        "bulk_item_failed" if item else "bulk_item_result_missing"
                    ),
                    "error_type": (
                        error.get("type") if isinstance(error, Mapping) else None
                    ),
                    "retryable": transient,
                }
            if retry_documents:
                pending = retry_documents
                await self._sleep(self._delay(attempt, None))
            else:
                pending = []
                break

        counts = Counter(expected)
        report = BulkWriteReport(
            expected_ids=tuple(sorted(set(expected))),
            succeeded_ids=tuple(sorted(succeeded)),
            failed_items=failed,
            duplicate_ids=tuple(
                sorted(key for key, count in counts.items() if count > 1)
            ),
        )
        self.metrics.observe_bulk(
            bytes_sent=batch.size_bytes,
            seconds=time.monotonic() - started,
            succeeded=len(report.succeeded_ids),
            failed=len(report.failed_items),
        )
        return report

    async def write(
        self, index: str, documents: list[Mapping[str, Any]]
    ) -> BulkWriteReport:
        validate_documents(
            documents, expected_dimensions=self.settings.expected_dimensions
        )
        succeeded: set[str] = set()
        failed: dict[str, Mapping[str, Any]] = {}
        for batch in iter_bulk_batches(
            documents,
            index,
            max_documents=self.settings.bulk_max_documents,
            max_bytes=self.settings.bulk_max_bytes,
        ):
            report = await self._post_batch(index, batch)
            succeeded.update(report.succeeded_ids)
            failed.update(report.failed_items)

        expected = [elasticsearch_document_id(doc) for doc in documents]
        counts = Counter(expected)
        return BulkWriteReport(
            expected_ids=tuple(sorted(set(expected))),
            succeeded_ids=tuple(sorted(succeeded)),
            failed_items=failed,
            duplicate_ids=tuple(
                sorted(key for key, count in counts.items() if count > 1)
            ),
        )


class BulkClient(Protocol):
    settings: OptimizedWriterSettings

    async def write(
        self, index: str, documents: list[Mapping[str, Any]]
    ) -> BulkWriteReport: ...


@dataclass(slots=True)
class _QueuedWrite:
    index: str
    documents: list[Mapping[str, Any]]
    estimated_bytes: int
    future: asyncio.Future[BulkWriteReport]


class ElasticsearchMicrobatcher:
    """Coalesce ready documents while preserving one report per source request."""

    def __init__(self, client: BulkClient) -> None:
        self.client = client
        self.settings = client.settings
        self.metrics = getattr(client, "metrics", NullIndexWorkerMetrics())
        self._queue: asyncio.Queue[_QueuedWrite] = asyncio.Queue(
            maxsize=self.settings.microbatch_queue_size
        )
        self._tasks: list[asyncio.Task[None]] = []

    def start(self) -> None:
        if not self._tasks:
            self._tasks = [
                asyncio.create_task(self._run())
                for _ in range(self.settings.bulk_max_concurrency)
            ]

    async def submit(
        self, index: str, documents: list[Mapping[str, Any]]
    ) -> BulkWriteReport:
        validate_documents(
            documents, expected_dimensions=self.settings.expected_dimensions
        )
        self.start()
        future = asyncio.get_running_loop().create_future()
        estimated = sum(
            len(_serialized_pair(index, document)) for document in documents
        )
        await self._queue.put(
            _QueuedWrite(index, documents, estimated, future)
        )
        return await future

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks.clear()
        while not self._queue.empty():
            item = self._queue.get_nowait()
            if not item.future.done():
                item.future.set_exception(RuntimeError("microbatcher stopped"))

    async def _run(self) -> None:
        carry: _QueuedWrite | None = None
        while True:
            first = carry if carry is not None else await self._queue.get()
            carry = None
            items = [first]
            document_count = len(first.documents)
            byte_count = first.estimated_bytes
            deadline = (
                asyncio.get_running_loop().time()
                + self.settings.microbatch_flush_seconds
            )
            while (
                document_count < self.settings.microbatch_max_documents
                and byte_count < self.settings.microbatch_max_bytes
            ):
                timeout = deadline - asyncio.get_running_loop().time()
                if timeout <= 0:
                    break
                try:
                    candidate = await asyncio.wait_for(self._queue.get(), timeout)
                except TimeoutError:
                    break
                fits = (
                    candidate.index == first.index
                    and document_count + len(candidate.documents)
                    <= self.settings.microbatch_max_documents
                    and byte_count + candidate.estimated_bytes
                    <= self.settings.microbatch_max_bytes
                )
                if not fits:
                    carry = candidate
                    break
                items.append(candidate)
                document_count += len(candidate.documents)
                byte_count += candidate.estimated_bytes

            try:
                flush_started = time.monotonic()
                report = await self.client.write(
                    first.index,
                    [
                        document
                        for item in items
                        for document in item.documents
                    ],
                )
                self.metrics.observe_microbatch(
                    source_requests=len(items),
                    documents=sum(len(item.documents) for item in items),
                    seconds=time.monotonic() - flush_started,
                )
                succeeded = set(report.succeeded_ids)
                duplicates = set(report.duplicate_ids)
                for item in items:
                    ids = [
                        elasticsearch_document_id(document)
                        for document in item.documents
                    ]
                    counts = Counter(ids)
                    expected = set(ids)
                    item_report = BulkWriteReport(
                        expected_ids=tuple(sorted(expected)),
                        succeeded_ids=tuple(sorted(expected & succeeded)),
                        failed_items={
                            key: value
                            for key, value in report.failed_items.items()
                            if key in expected
                        },
                        duplicate_ids=tuple(
                            sorted(
                                {key for key, count in counts.items() if count > 1}
                                | (expected & duplicates)
                            )
                        ),
                    )
                    if not item.future.done():
                        item.future.set_result(item_report)
            except asyncio.CancelledError:
                for item in items:
                    if not item.future.done():
                        item.future.set_exception(
                            RuntimeError("microbatch cancelled")
                        )
                raise
            except Exception as error:
                for item in items:
                    if not item.future.done():
                        item.future.set_exception(error)


@dataclass(slots=True)
class EsReadyCheckpointStager:
    """Commit compact embedding output and schedule the durable INDEX stage."""

    root: Path
    index_config_digest: str
    index_model_digest: str
    expected_dimensions: int = 2048
    index_stream: str = "ingest.index"
    _delegate: JsonResultStager = field(init=False)

    def __post_init__(self) -> None:
        self._delegate = JsonResultStager(self.root)

    @staticmethod
    def _rows(graph_result: Any) -> list[Mapping[str, Any]]:
        value = (
            graph_result.get("result_data")
            if isinstance(graph_result, Mapping)
            else graph_result
        )
        if value is None:
            return []
        if not isinstance(value, list) or any(
            not isinstance(row, Mapping) for row in value
        ):
            raise ElasticsearchPayloadError(
                "embedding result must be a row list or result_data row list"
            )
        return value

    async def stage(
        self, envelope: StageEnvelope, graph_result: Any
    ) -> tuple[PendingArtifact, tuple[tuple[str, StageEnvelope], ...]]:
        if envelope.stage is not Stage.EMBEDDING:
            raise ValueError("ES-ready checkpoint stager requires EMBEDDING stage")
        checkpoint = compact_checkpoint(
            self._rows(graph_result), envelope.document_id
        )
        documents = checkpoint["documents"]
        if documents:
            validate_documents(
                documents, expected_dimensions=self.expected_dimensions
            )
        artifact, _ = await self._delegate.stage(envelope, checkpoint)
        metadata = {
            **artifact.metadata,
            "payload_format": "es_docs",
            "writable_documents": len(documents),
        }
        downstream: tuple[tuple[str, StageEnvelope], ...] = ()
        if documents or os.environ.get("INGEST_STREAM_PREFIX") == "rag.agentic":
            # The app source-admission consumer needs complete extraction
            # evidence even for image-only PDFs with no source vectors.
            index_key = derive_idempotency_key(
                source_content_hash=envelope.document_id,
                logical_unit_id=f"{envelope.document_id}:index",
                stage=Stage.INDEX,
                pipeline_version=envelope.pipeline_version,
                stage_config={
                    "config_digest": self.index_config_digest,
                    "payload_format": "es_docs",
                    "schema_version": checkpoint["schema_version"],
                },
                model_digest=self.index_model_digest,
            )
            downstream = (
                (
                    self.index_stream,
                    StageEnvelope(
                        job_id=envelope.job_id,
                        document_id=envelope.document_id,
                        message_id=str(uuid5(NAMESPACE_URL,index_key)),
                        created_at=envelope.created_at,
                        stage=Stage.INDEX,
                        artifact_uri=artifact.final_uri,
                        idempotency_key=index_key,
                        pipeline_version=envelope.pipeline_version,
                        config_digest=self.index_config_digest,
                        model_digest=self.index_model_digest,
                        priority=envelope.priority,
                        trace_id=envelope.trace_id,
                    ),
                ),
            )
        else:
            metadata["index_skipped"] = "no_writable_rows"
        return replace(artifact, metadata=metadata), downstream


class EsReadyArtifactLoader(Protocol):
    async def load(self, artifact_uri: str) -> list[Mapping[str, Any]]: ...


class IndexReceiptStager(Protocol):
    async def stage(
        self, envelope: StageEnvelope, report: BulkWriteReport
    ) -> PendingArtifact: ...


@dataclass(slots=True)
class LocalEsReadyArtifactLoader:
    root: Path | None = None

    def __post_init__(self) -> None:
        if self.root is not None:
            self.root = self.root.resolve()

    async def load(self, artifact_uri: str) -> list[Mapping[str, Any]]:
        parsed = urlparse(artifact_uri)
        if parsed.scheme != "file":
            raise ValueError(
                "local ES-ready loader requires file://; inject an object-store loader"
            )
        path = Path(unquote(parsed.path)).resolve()
        if self.root is not None and not path.is_relative_to(self.root):
            raise ElasticsearchPayloadError(
                f"ES-ready artifact escapes configured root: {path}"
            )
        value = await asyncio.to_thread(
            lambda: json.loads(path.read_text(encoding="utf-8"))
        )
        if not isinstance(value, Mapping) or value.get("payload_format") != "es_docs":
            raise ElasticsearchPayloadError("artifact is not an es_docs checkpoint")
        documents = value.get("documents")
        if not isinstance(documents, list):
            raise ElasticsearchPayloadError("es_docs checkpoint lacks documents")
        return documents


@dataclass(slots=True)
class JsonIndexReceiptStager:
    root: Path
    _delegate: JsonResultStager = field(init=False)

    def __post_init__(self) -> None:
        self._delegate = JsonResultStager(self.root)

    async def stage(
        self, envelope: StageEnvelope, report: BulkWriteReport
    ) -> PendingArtifact:
        artifact, _ = await self._delegate.stage(envelope, report.to_dict())
        return replace(
            artifact,
            metadata={
                **artifact.metadata,
                "complete": report.complete,
                "written_count": report.written_count,
            },
        )


@dataclass(slots=True)
class OptimizedElasticsearchStageProcessor:
    index: str
    batcher: ElasticsearchMicrobatcher
    loader: EsReadyArtifactLoader
    receipt_stager: IndexReceiptStager

    async def process(self, envelope: StageEnvelope) -> StageResult:
        if envelope.stage is not Stage.INDEX:
            raise ValueError("optimized Elasticsearch processor requires INDEX stage")
        documents = await self.loader.load(envelope.artifact_uri)
        report = await self.batcher.submit(self.index, documents)
        if not report.complete:
            raise ElasticsearchWriteIncomplete(report)
        receipt = await self.receipt_stager.stage(envelope, report)
        return StageResult(artifact=receipt)
