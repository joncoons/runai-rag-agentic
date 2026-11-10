"""Versioned transport and persistence contracts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Mapping
from uuid import uuid4


class Stage(StrEnum):
    EXTRACTION = "extraction"
    PARSE = "parse"
    CAPTION = "caption"
    EMBEDDING = "embedding"
    INDEX = "index"


class WorkerOutcome(StrEnum):
    COMMITTED = "committed"
    ALREADY_COMMITTED = "already_committed"
    LEASE_BUSY = "lease_busy"
    FAILED = "failed"
    DEADLETTERED = "deadlettered"


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def derive_idempotency_key(
    *,
    source_content_hash: str,
    logical_unit_id: str,
    stage: Stage | str,
    pipeline_version: str,
    stage_config: Mapping[str, Any],
    model_digest: str,
) -> str:
    material = {
        "source_content_hash": source_content_hash,
        "logical_unit_id": logical_unit_id,
        "stage": str(stage),
        "pipeline_version": pipeline_version,
        "stage_config": stage_config,
        "model_digest": model_digest,
    }
    encoded = _canonical_json(material).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _decode_fields(fields: Mapping[Any, Any]) -> dict[str, str]:
    def decode(value: Any) -> str:
        return value.decode("utf-8") if isinstance(value, bytes) else str(value)

    return {decode(key): decode(value) for key, value in fields.items()}


@dataclass(frozen=True, slots=True)
class StageEnvelope:
    job_id: str
    document_id: str
    stage: Stage
    artifact_uri: str
    idempotency_key: str
    pipeline_version: str
    config_digest: str
    model_digest: str
    schema_version: str = "1"
    message_id: str = field(default_factory=lambda: str(uuid4()))
    attempt: int = 0
    not_before: str = ""
    priority: int = 0
    trace_id: str = ""
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def __post_init__(self) -> None:
        if self.attempt < 0:
            raise ValueError("attempt must be non-negative")
        if not self.artifact_uri:
            raise ValueError("artifact_uri is required")
        if not self.idempotency_key.startswith("sha256:"):
            raise ValueError("idempotency_key must be a sha256 key")

    def to_dict(self) -> dict[str, Any]:
        values = asdict(self)
        values["stage"] = self.stage.value
        return values

    def to_stream_fields(self) -> dict[str, str]:
        return {key: str(value) for key, value in self.to_dict().items()}

    @classmethod
    def from_stream_fields(cls, fields: Mapping[Any, Any]) -> "StageEnvelope":
        values = _decode_fields(fields)
        return cls(
            job_id=values["job_id"],
            document_id=values["document_id"],
            stage=Stage(values["stage"]),
            artifact_uri=values["artifact_uri"],
            idempotency_key=values["idempotency_key"],
            pipeline_version=values["pipeline_version"],
            config_digest=values["config_digest"],
            model_digest=values["model_digest"],
            schema_version=values.get("schema_version", "1"),
            message_id=values["message_id"],
            attempt=int(values.get("attempt", 0)),
            not_before=values.get("not_before", ""),
            priority=int(values.get("priority", 0)),
            trace_id=values.get("trace_id", ""),
            created_at=values["created_at"],
        )


@dataclass(frozen=True, slots=True)
class ClaimedMessage:
    stream: str
    group: str
    redis_id: str
    envelope: StageEnvelope


@dataclass(frozen=True, slots=True)
class PendingArtifact:
    temporary_uri: str
    final_uri: str
    digest: str
    size_bytes: int
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CommittedArtifact:
    uri: str
    digest: str
    size_bytes: int
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class StageResult:
    artifact: PendingArtifact
    downstream: tuple[tuple[str, StageEnvelope], ...] = ()


@dataclass(frozen=True, slots=True)
class OutboxEvent:
    event_id: str
    stream: str
    envelope: StageEnvelope
