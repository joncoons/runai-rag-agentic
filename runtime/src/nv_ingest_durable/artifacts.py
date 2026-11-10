"""Content-verified local artifact adapter and JSON graph-result stager."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import unquote, urlparse
from uuid import uuid4

from .contracts import CommittedArtifact, PendingArtifact, StageEnvelope


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return f"sha256:{digest.hexdigest()}"


@dataclass(slots=True)
class FileArtifactStore:
    root: Path
    io_concurrency: int = 2
    _io_slots: asyncio.Semaphore = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.io_concurrency < 1:
            raise ValueError("io_concurrency must be positive")
        self.root = self.root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._io_slots = asyncio.Semaphore(self.io_concurrency)

    def _path(self, uri: str) -> Path:
        parsed = urlparse(uri)
        if parsed.scheme != "file":
            raise ValueError(f"FileArtifactStore requires file:// URIs: {uri}")
        path = Path(unquote(parsed.path)).resolve()
        if not path.is_relative_to(self.root):
            raise ValueError(f"artifact escapes configured root: {path}")
        return path

    async def finalize(self, artifact: PendingArtifact) -> CommittedArtifact:
        async with self._io_slots:
            return await asyncio.to_thread(self._finalize, artifact)

    def _finalize(self, artifact: PendingArtifact) -> CommittedArtifact:
        temporary = self._path(artifact.temporary_uri)
        final = self._path(artifact.final_uri)
        if not temporary.is_file() and not final.is_file():
            raise FileNotFoundError(temporary)

        if final.is_file():
            actual_digest = _sha256(final)
            actual_size = final.stat().st_size
            if actual_digest != artifact.digest or actual_size != artifact.size_bytes:
                raise ValueError("existing artifact does not match committed identity")
            if temporary.is_file() and temporary != final:
                temporary.unlink()
        else:
            if _sha256(temporary) != artifact.digest:
                raise ValueError("temporary artifact digest mismatch")
            if temporary.stat().st_size != artifact.size_bytes:
                raise ValueError("temporary artifact size mismatch")
            final.parent.mkdir(parents=True, exist_ok=True)
            os.replace(temporary, final)
            with final.open("rb") as committed:
                os.fsync(committed.fileno())
            directory_fd = os.open(final.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)

        return CommittedArtifact(
            uri=final.as_uri(),
            digest=artifact.digest,
            size_bytes=artifact.size_bytes,
            metadata=artifact.metadata,
        )


Encoder = Callable[[Any], Any]


@dataclass(slots=True)
class JsonResultStager:
    root: Path
    encoder: Encoder = lambda value: value
    io_concurrency: int = 2
    _io_slots: asyncio.Semaphore = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.io_concurrency < 1:
            raise ValueError("io_concurrency must be positive")
        self.root = self.root.resolve()
        (self.root / ".pending").mkdir(parents=True, exist_ok=True)
        self._io_slots = asyncio.Semaphore(self.io_concurrency)

    async def stage(self, envelope: StageEnvelope, graph_result: Any):
        async with self._io_slots:
            return await asyncio.to_thread(self._stage, envelope, graph_result)

    def _stage(self, envelope: StageEnvelope, graph_result: Any):
        payload = json.dumps(
            self.encoder(graph_result),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        digest = f"sha256:{hashlib.sha256(payload).hexdigest()}"
        temporary = self.root / ".pending" / f"{uuid4()}.json"
        identity = envelope.idempotency_key.removeprefix("sha256:")
        final = self.root / envelope.stage.value / f"{identity}.json"
        temporary.write_bytes(payload)
        return (
            PendingArtifact(
                temporary_uri=temporary.as_uri(),
                final_uri=final.as_uri(),
                digest=digest,
                size_bytes=len(payload),
                metadata={"content_type": "application/json"},
            ),
            (),
        )

@dataclass(slots=True)
class ParquetRecordsStager:
    """Stage heterogeneous records as compressed Parquet binary rows."""

    root: Path
    io_concurrency: int = 2
    compression: str = "zstd"
    _io_slots: asyncio.Semaphore = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.io_concurrency < 1:
            raise ValueError("io_concurrency must be positive")
        self.root = self.root.resolve()
        (self.root / ".pending").mkdir(parents=True, exist_ok=True)
        self._io_slots = asyncio.Semaphore(self.io_concurrency)

    async def stage(self, envelope: StageEnvelope, records: list[Mapping[str, Any]]):
        async with self._io_slots:
            return await asyncio.to_thread(self._stage, envelope, records)

    def _stage(self, envelope: StageEnvelope, records: list[Mapping[str, Any]]):
        import pyarrow as pa
        import pyarrow.parquet as pq

        encoded = [
            json.dumps(
                record,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            for record in records
        ]
        table = pa.table(
            {"record_json": pa.array(encoded, type=pa.large_binary())}
        )
        temporary = self.root / ".pending" / f"{uuid4()}.parquet"
        identity = envelope.idempotency_key.removeprefix("sha256:")
        final = self.root / envelope.stage.value / f"{identity}.parquet"
        pq.write_table(
            table,
            temporary,
            compression=self.compression,
            use_dictionary=False,
            write_statistics=False,
        )
        size = temporary.stat().st_size
        return (
            PendingArtifact(
                temporary_uri=temporary.as_uri(),
                final_uri=final.as_uri(),
                digest=_sha256(temporary),
                size_bytes=size,
                metadata={
                    "content_type": "application/vnd.apache.parquet",
                    "payload_format": "record-json-parquet-v1",
                    "row_count": len(records),
                },
            ),
            (),
        )


def read_parquet_records(uri: str) -> list[Mapping[str, Any]]:
    """Read a record-json Parquet handoff from a local durable URI."""

    import pyarrow.parquet as pq

    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise ValueError("Parquet handoff requires a file:// URI")
    path = Path(unquote(parsed.path))
    table = pq.read_table(path, columns=["record_json"])
    values = table.column("record_json").to_pylist()
    records = [json.loads(bytes(value)) for value in values]
    if any(not isinstance(record, dict) for record in records):
        raise ValueError("Parquet handoff contains a non-object record")
    return records
