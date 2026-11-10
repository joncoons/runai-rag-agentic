"""Framework-neutral operational service for submission and recovery."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

from .contracts import Stage
from .coordinator import IngestCoordinator, SubmissionResult, SubmissionSpec
from .scanner_inventory import ScannerInventory, load_scanner_inventory


@dataclass(frozen=True, slots=True)
class JobStatus:
    job_id: str
    corpus_namespace: str
    submission_key: str
    status: str
    expected_documents: int
    document_count: int
    stage_counts: dict[str, int]
    unpublished_outbox: int
    deadletter_count: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ReplayResult:
    replay_id: str
    job_id: str
    replayed_units: int


class OperationsRepository(Protocol):
    async def ping(self) -> None: ...
    async def get_job_status(self, job_id: str) -> JobStatus | None: ...
    async def replay_failed(
        self,
        job_id: str,
        *,
        stage: Stage | None,
        target_stream: str,
        requested_by: str,
        reason: str,
    ) -> ReplayResult: ...
    async def repair_initial_outbox(
        self, *, target_stream: str, stage: Stage, limit: int
    ) -> int: ...


@dataclass(frozen=True, slots=True)
class ScannerSubmissionResult:
    submission: SubmissionResult
    inventory: ScannerInventory


class JobNotFoundError(LookupError):
    pass


@dataclass(slots=True)
class OperationsService:
    coordinator: IngestCoordinator | None
    repository: OperationsRepository

    async def ready(self) -> None:
        """Raise when the authoritative manifest database is unavailable."""
        await self.repository.ping()

    async def submit_scanner_csv(
        self,
        csv_path: Path,
        spec: SubmissionSpec,
        *,
        require_existing: bool = True,
    ) -> ScannerSubmissionResult:
        if self.coordinator is None:
            raise RuntimeError("submission is not configured")
        inventory = load_scanner_inventory(
            csv_path, require_existing=require_existing
        )
        submission = await self.coordinator.submit(spec, inventory.documents)
        return ScannerSubmissionResult(submission, inventory)

    async def status(self, job_id: str) -> JobStatus:
        status = await self.repository.get_job_status(job_id)
        if status is None:
            raise JobNotFoundError(job_id)
        return status

    async def reconcile(
        self,
        *,
        target_stream: str = "ingest.extraction.pending",
        stage: Stage = Stage.EXTRACTION,
        limit: int = 1000,
    ) -> int:
        return await self.repository.repair_initial_outbox(
            target_stream=target_stream, stage=stage, limit=limit
        )

    async def replay(
        self,
        job_id: str,
        *,
        requested_by: str,
        reason: str,
        stage: Stage | None = None,
        target_stream: str = "ingest.extraction.pending",
    ) -> ReplayResult:
        if not requested_by.strip() or not reason.strip():
            raise ValueError("requested_by and reason are required for replay")
        return await self.repository.replay_failed(
            job_id,
            stage=stage,
            target_stream=target_stream,
            requested_by=requested_by,
            reason=reason,
        )
