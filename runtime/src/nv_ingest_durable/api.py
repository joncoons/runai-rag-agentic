"""Thin FastAPI surface over the framework-neutral operations service."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import asdict
from hmac import compare_digest
import os
from pathlib import Path
from typing import Any
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from .contracts import Stage
from .coordinator import INITIAL_STAGE, INITIAL_STREAM, SubmissionSpec
from .operations import JobNotFoundError, OperationsService


class ScannerSubmitRequest(BaseModel):
    csv_path: str
    corpus_namespace: str
    submission_key: str
    pipeline_version: str
    config_digest: str
    model_digest: str = "sha256:none"
    job_id: str | None = None
    require_existing: bool = True


class ReplayRequest(BaseModel):
    requested_by: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    stage: Stage | None = None
    target_stream: str | None = None


class ReconcileRequest(BaseModel):
    stage: Stage | None = None
    target_stream: str | None = None
    limit: int = Field(default=1000, ge=1, le=100_000)


def create_app(
    service: OperationsService | None = None,
    *,
    lifespan: Any = None,
    api_token: str | None = None,
) -> FastAPI:
    app = FastAPI(
        title="NV-Ingest 26.5 Durable Orchestration",
        version="0.4.0",
        lifespan=lifespan,
    )

    def operations() -> OperationsService:
        return service or app.state.operations

    async def authorize(authorization: str | None = Header(default=None)) -> None:
        if api_token is None:
            return
        scheme, _, credential = (authorization or "").partition(" ")
        if scheme.lower() != "bearer" or not compare_digest(credential, api_token):
            raise HTTPException(
                status_code=401,
                detail="invalid operator credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )

    operator = [Depends(authorize)]

    @app.get("/health/live")
    async def live():
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready():
        try:
            await operations().ready()
        except Exception as error:
            raise HTTPException(status_code=503, detail="not ready") from error
        return {"status": "ready"}

    @app.post("/v1/jobs/scanner", dependencies=operator)
    async def submit_scanner(request: ScannerSubmitRequest):
        from uuid import uuid4

        spec = SubmissionSpec(
            job_id=request.job_id or str(uuid4()),
            corpus_namespace=request.corpus_namespace,
            submission_key=request.submission_key,
            pipeline_version=request.pipeline_version,
            config_digest=request.config_digest,
            model_digest=request.model_digest,
        )
        result = await operations().submit_scanner_csv(
            Path(request.csv_path), spec, require_existing=request.require_existing
        )
        return {
            "submission": asdict(result.submission),
            "inventory": {
                **asdict(result.inventory),
                "documents": len(result.inventory.documents),
            },
        }

    @app.get("/v1/jobs/{job_id}", dependencies=operator)
    async def job_status(job_id: UUID):
        try:
            return (await operations().status(str(job_id))).to_dict()
        except JobNotFoundError as error:
            raise HTTPException(status_code=404, detail="job not found") from error

    @app.post("/v1/jobs/{job_id}/replay", dependencies=operator)
    async def replay(job_id: UUID, request: ReplayRequest):
        try:
            return asdict(
                await operations().replay(
                    str(job_id),
                    requested_by=request.requested_by,
                    reason=request.reason,
                    stage=request.stage,
                    target_stream=request.target_stream or INITIAL_STREAM,
                )
            )
        except LookupError as error:
            raise HTTPException(status_code=404, detail="job not found") from error

    @app.post("/v1/reconcile", dependencies=operator)
    async def reconcile(request: ReconcileRequest):
        repaired = await operations().reconcile(
            target_stream=request.target_stream or INITIAL_STREAM,
            stage=request.stage or INITIAL_STAGE,
            limit=request.limit,
        )
        return {"repaired": repaired}

    return app


def create_app_from_env() -> FastAPI:
    """Uvicorn factory using PostgreSQL, artifact, and operator-token settings."""

    api_token = os.environ.get("OPERATOR_API_TOKEN", "")
    if not api_token:
        raise RuntimeError("OPERATOR_API_TOKEN is required")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        import asyncpg

        from .coordinator import IngestCoordinator
        from .coordinator_postgres import PostgresSubmissionRepository
        from .operations_postgres import PostgresOperationsRepository
        from .source_store import FileSourceStore

        dsn = os.environ["POSTGRES_DSN"]
        artifact_root = Path(os.environ["SOURCE_ARTIFACT_ROOT"])
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10)
        app.state.operations = OperationsService(
            IngestCoordinator(
                FileSourceStore(artifact_root),
                PostgresSubmissionRepository(pool),
            ),
            PostgresOperationsRepository(pool),
        )
        try:
            yield
        finally:
            await pool.close()

    return create_app(
        lifespan=lifespan,
        api_token=api_token,
    )
