"""Idempotent corpus submission and initial-stage scheduling."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Protocol
from uuid import NAMESPACE_URL, uuid4, uuid5

from .contracts import CommittedArtifact, Stage, StageEnvelope, derive_idempotency_key


INITIAL_STAGE = Stage.EXTRACTION
INITIAL_STREAM = "ingest.extraction.pending"


@dataclass(frozen=True, slots=True)
class SourceDocument:
    path: Path
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SubmissionSpec:
    corpus_namespace: str
    submission_key: str
    pipeline_version: str
    config_digest: str
    model_digest: str = "sha256:none"
    job_id: str = field(default_factory=lambda: str(uuid4()))
    initial_stage: Stage = INITIAL_STAGE
    target_stream: str = INITIAL_STREAM


@dataclass(frozen=True, slots=True)
class PreparedDocument:
    document_id: str
    source: CommittedArtifact
    envelope: StageEnvelope


@dataclass(frozen=True, slots=True)
class RegistrationOutcome:
    job_id: str
    created: bool
    scheduled_documents: int


@dataclass(frozen=True, slots=True)
class SubmissionResult:
    job_id: str
    created: bool
    scheduled_documents: int
    duplicate_inputs: int


class SourceStore(Protocol):
    async def import_path(
        self, path: Path, *, metadata: Mapping[str, Any]
    ) -> CommittedArtifact: ...


class SubmissionRepository(Protocol):
    async def register_submission(
        self, spec: SubmissionSpec, documents: tuple[PreparedDocument, ...]
    ) -> RegistrationOutcome: ...


def _document_id(job_id: str, source_digest: str) -> str:
    from hashlib import sha256

    material = f"{job_id}\0{source_digest}".encode("utf-8")
    return f"sha256:{sha256(material).hexdigest()}"


@dataclass(slots=True)
class IngestCoordinator:
    sources: SourceStore
    submissions: SubmissionRepository
    import_concurrency: int = 8

    async def submit(
        self, spec: SubmissionSpec, inputs: tuple[SourceDocument, ...]
    ) -> SubmissionResult:
        semaphore = asyncio.Semaphore(self.import_concurrency)

        async def import_one(item: SourceDocument) -> CommittedArtifact:
            async with semaphore:
                return await self.sources.import_path(item.path, metadata=item.metadata)

        imported = await asyncio.gather(*(import_one(item) for item in inputs))
        unique: dict[str, CommittedArtifact] = {}
        for artifact in imported:
            unique.setdefault(artifact.digest, artifact)

        prepared: list[PreparedDocument] = []
        for source_digest, artifact in sorted(unique.items()):
            document_id = _document_id(spec.job_id, source_digest)
            idempotency_key = derive_idempotency_key(
                source_content_hash=source_digest,
                logical_unit_id=document_id,
                stage=spec.initial_stage,
                pipeline_version=spec.pipeline_version,
                stage_config={"config_digest": spec.config_digest},
                model_digest=spec.model_digest,
            )
            message_id = str(uuid5(NAMESPACE_URL, f"{spec.job_id}:{idempotency_key}"))
            envelope = StageEnvelope(
                job_id=spec.job_id,
                document_id=document_id,
                stage=spec.initial_stage,
                artifact_uri=artifact.uri,
                idempotency_key=idempotency_key,
                pipeline_version=spec.pipeline_version,
                config_digest=spec.config_digest,
                model_digest=spec.model_digest,
                message_id=message_id,
            )
            prepared.append(PreparedDocument(document_id, artifact, envelope))

        outcome = await self.submissions.register_submission(spec, tuple(prepared))
        return SubmissionResult(
            job_id=outcome.job_id,
            created=outcome.created,
            scheduled_documents=outcome.scheduled_documents,
            duplicate_inputs=len(inputs) - len(prepared),
        )
