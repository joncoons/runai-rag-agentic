"""Operational command line interface."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

from .contracts import Stage
from .coordinator import (
    INITIAL_STAGE,
    INITIAL_STREAM,
    IngestCoordinator,
    SubmissionSpec,
)
from .coordinator_postgres import PostgresSubmissionRepository
from .migrations import read_migrations
from .operations import OperationsService
from .operations_postgres import PostgresOperationsRepository
from .postgres import PostgresManifestRepository
from .redis_streams import RedisStreamsTransport
from .source_store import FileSourceStore
from .worker import OutboxRelay


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="durable-ingest")
    parser.add_argument(
        "--postgres-dsn",
        default=os.getenv(
            "POSTGRES_DSN",
            "postgresql://durable_ingest:durable_ingest@127.0.0.1:15432/durable_ingest",
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("migrate")

    submit = sub.add_parser("submit-scanner")
    submit.add_argument("--csv", type=Path, required=True)
    submit.add_argument("--artifacts-root", type=Path, required=True)
    submit.add_argument("--namespace", required=True)
    submit.add_argument("--submission-key", required=True)
    submit.add_argument("--pipeline-version", required=True)
    submit.add_argument("--config-digest", required=True)
    submit.add_argument("--model-digest", default="sha256:none")
    submit.add_argument("--job-id", default=None)
    submit.add_argument("--allow-missing", action="store_true")

    status = sub.add_parser("status")
    status.add_argument("job_id")

    reconcile = sub.add_parser("reconcile")
    reconcile.add_argument("--stream")
    reconcile.add_argument("--stage", choices=[stage.value for stage in Stage])
    reconcile.add_argument("--limit", type=int, default=1000)

    replay = sub.add_parser("replay")
    replay.add_argument("job_id")
    replay.add_argument("--requested-by", required=True)
    replay.add_argument("--reason", required=True)
    replay.add_argument("--stage", choices=[stage.value for stage in Stage])
    replay.add_argument("--stream")

    relay = sub.add_parser("relay-once")
    relay.add_argument(
        "--redis-url", default=os.getenv("REDIS_URL", "redis://127.0.0.1:16379/0")
    )
    relay.add_argument("--limit", type=int, default=100)
    return parser


async def _run(args: argparse.Namespace) -> dict:
    import asyncpg

    pool = await asyncpg.create_pool(args.postgres_dsn, min_size=1, max_size=4)
    try:
        if args.command == "migrate":
            async with pool.acquire() as connection:
                applied = []
                for name, sql in read_migrations():
                    await connection.execute(sql)
                    applied.append(name)
            return {"applied": applied}
        if args.command == "relay-once":
            transport = RedisStreamsTransport.from_url(args.redis_url)
            try:
                count = await OutboxRelay(
                    PostgresManifestRepository(pool), transport
                ).publish_once(limit=args.limit)
                return {"published": count}
            finally:
                await transport.client.aclose()

        repository = PostgresOperationsRepository(pool)
        coordinator = None
        if args.command == "submit-scanner":
            coordinator = IngestCoordinator(
                FileSourceStore(args.artifacts_root),
                PostgresSubmissionRepository(pool),
            )
        service = OperationsService(coordinator, repository)
        if args.command == "submit-scanner":
            spec = SubmissionSpec(
                job_id=args.job_id or str(uuid4()),
                corpus_namespace=args.namespace,
                submission_key=args.submission_key,
                pipeline_version=args.pipeline_version,
                config_digest=args.config_digest,
                model_digest=args.model_digest,
            )
            result = await service.submit_scanner_csv(
                args.csv, spec, require_existing=not args.allow_missing
            )
            return {
                "submission": asdict(result.submission),
                "inventory": {
                    **asdict(result.inventory),
                    "documents": len(result.inventory.documents),
                },
            }
        if args.command == "status":
            return (await service.status(args.job_id)).to_dict()
        if args.command == "reconcile":
            repaired = await service.reconcile(
                target_stream=args.stream or INITIAL_STREAM,
                stage=Stage(args.stage) if args.stage else INITIAL_STAGE,
                limit=args.limit,
            )
            return {"repaired": repaired}
        if args.command == "replay":
            result = await service.replay(
                args.job_id,
                requested_by=args.requested_by,
                reason=args.reason,
                stage=Stage(args.stage) if args.stage else None,
                target_stream=args.stream or INITIAL_STREAM,
            )
            return asdict(result)
        raise RuntimeError(f"unknown command: {args.command}")
    finally:
        await pool.close()


def main() -> None:
    args = _parser().parse_args()
    print(json.dumps(asyncio.run(_run(args)), indent=2, sort_keys=True, default=str))
