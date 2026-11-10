"""Infrastructure ports used by the durable worker core."""

from __future__ import annotations

from datetime import timedelta
from typing import Protocol

from .contracts import (
    ClaimedMessage,
    CommittedArtifact,
    OutboxEvent,
    PendingArtifact,
    StageEnvelope,
    StageResult,
)


class StreamTransport(Protocol):
    async def ensure_group(self, stream: str, group: str) -> None: ...
    async def publish(self, stream: str, envelope: StageEnvelope) -> str: ...
    async def read(
        self, stream: str, group: str, consumer: str, *, count: int, block_ms: int
    ) -> list[ClaimedMessage]: ...
    async def reclaim(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        min_idle_ms: int,
        start_id: str,
        count: int,
    ) -> tuple[str, list[ClaimedMessage]]: ...
    async def acknowledge(self, message: ClaimedMessage) -> None: ...
    async def touch(self, message: ClaimedMessage, *, consumer: str) -> None: ...


class ManifestRepository(Protocol):
    async def get_committed(self, idempotency_key: str) -> CommittedArtifact | None: ...
    async def acquire_lease(
        self, envelope: StageEnvelope, *, owner: str, ttl: timedelta
    ) -> str | None: ...
    async def commit_stage(
        self,
        envelope: StageEnvelope,
        *,
        lease_token: str,
        artifact: CommittedArtifact,
        downstream: tuple[tuple[str, StageEnvelope], ...],
    ) -> None: ...
    async def record_failure(
        self, envelope: StageEnvelope, *, lease_token: str, error: BaseException
    ) -> int: ...
    async def renew_lease(
        self, envelope: StageEnvelope, *, lease_token: str, ttl: timedelta
    ) -> bool: ...
    async def release_lease(self, envelope: StageEnvelope, *, lease_token: str) -> None: ...
    async def deadletter(
        self, message: ClaimedMessage, *, error: BaseException, target_stream: str
    ) -> None: ...
    async def fetch_unpublished_outbox(self, *, limit: int) -> list[OutboxEvent]: ...
    async def mark_outbox_published(self, event_id: str, stream_id: str) -> None: ...


class ArtifactStore(Protocol):
    async def finalize(self, artifact: PendingArtifact) -> CommittedArtifact: ...


class StageProcessor(Protocol):
    async def process(self, envelope: StageEnvelope) -> StageResult: ...
