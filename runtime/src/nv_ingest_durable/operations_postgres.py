"""PostgreSQL job status, reconciliation, and audited replay operations."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from .contracts import Stage, StageEnvelope
from .coordinator_postgres import PostgresSubmissionRepository
from .operations import JobStatus, ReplayResult


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


class PostgresOperationsRepository:
    def __init__(self, pool: Any) -> None:
        self.pool = pool
        self.submissions = PostgresSubmissionRepository(pool)

    async def ping(self) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute("SELECT 1")

    async def get_job_status(self, job_id: str) -> JobStatus | None:
        async with self.pool.acquire() as connection:
            job = await connection.fetchrow(
                """
                SELECT job_id::text, corpus_namespace, submission_key, status,
                       expected_documents
                  FROM jobs WHERE job_id = $1::uuid
                """,
                job_id,
            )
            if job is None:
                return None
            stage_rows = await connection.fetch(
                """
                SELECT stage, status, count(*) AS count
                  FROM stage_units
                 WHERE job_id = $1::uuid
                 GROUP BY stage, status
                 ORDER BY stage, status
                """,
                job_id,
            )
            document_count = await connection.fetchval(
                "SELECT count(*) FROM documents WHERE job_id = $1::uuid", job_id
            )
            unpublished = await connection.fetchval(
                """
                SELECT count(*) FROM outbox_events o
                JOIN stage_units s USING (idempotency_key)
                WHERE s.job_id = $1::uuid AND o.published_at IS NULL
                """,
                job_id,
            )
            deadletters = await connection.fetchval(
                """
                SELECT count(*) FROM deadletters d
                JOIN stage_units s USING (idempotency_key)
                WHERE s.job_id = $1::uuid AND d.replayed_at IS NULL
                """,
                job_id,
            )
        return JobStatus(
            job_id=job["job_id"],
            corpus_namespace=job["corpus_namespace"],
            submission_key=job["submission_key"] or "",
            status=job["status"],
            expected_documents=job["expected_documents"],
            document_count=document_count,
            stage_counts={
                f"{row['stage']}:{row['status']}": row["count"]
                for row in stage_rows
            },
            unpublished_outbox=unpublished,
            deadletter_count=deadletters,
        )

    async def repair_initial_outbox(
        self,
        *,
        target_stream: str,
        stage: Stage,
        limit: int,
    ) -> int:
        return await self.submissions.repair_initial_outbox(
            target_stream=target_stream, stage=stage, limit=limit
        )

    async def replay_failed(
        self,
        job_id: str,
        *,
        stage: Stage | None,
        target_stream: str,
        requested_by: str,
        reason: str,
    ) -> ReplayResult:
        replay_id = str(uuid4())
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                exists = await connection.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM jobs WHERE job_id = $1::uuid)",
                    job_id,
                )
                if not exists:
                    raise LookupError(job_id)
                rows = await connection.fetch(
                    """
                    SELECT idempotency_key, input_envelope
                      FROM stage_units
                     WHERE job_id = $1::uuid
                       AND status IN ('failed', 'deadlettered')
                       AND input_envelope IS NOT NULL
                       AND ($2::text IS NULL OR stage = $2)
                     ORDER BY created_at
                     FOR UPDATE SKIP LOCKED
                    """,
                    job_id,
                    stage.value if stage else None,
                )
                for row in rows:
                    original = StageEnvelope.from_stream_fields(
                        _json_object(row["input_envelope"])
                    )
                    replay_envelope = replace(
                        original,
                        message_id=str(
                            uuid5(
                                NAMESPACE_URL,
                                f"replay:{replay_id}:{original.message_id}",
                            )
                        ),
                        attempt=0,
                        created_at=datetime.now(UTC).isoformat(),
                    )
                    envelope_json = json.dumps(replay_envelope.to_dict())
                    await connection.execute(
                        """
                        UPDATE stage_units
                           SET status = 'pending', attempt_count = 0,
                               last_error = NULL, lease_owner = NULL,
                               lease_token = NULL, lease_expires_at = NULL,
                               input_envelope = $2::jsonb, updated_at = now()
                         WHERE idempotency_key = $1
                        """,
                        row["idempotency_key"],
                        envelope_json,
                    )
                    await connection.execute(
                        """
                        INSERT INTO outbox_events (
                            event_id, idempotency_key, target_stream, envelope
                        ) VALUES ($1::uuid, $2, $3, $4::jsonb)
                        ON CONFLICT (idempotency_key, target_stream,
                                     (envelope->>'message_id')) DO NOTHING
                        """,
                        str(uuid4()),
                        row["idempotency_key"],
                        target_stream,
                        envelope_json,
                    )
                    await connection.execute(
                        """
                        UPDATE deadletters SET replayed_at = now()
                         WHERE idempotency_key = $1 AND replayed_at IS NULL
                        """,
                        row["idempotency_key"],
                    )

                await connection.execute(
                    """
                    INSERT INTO replay_requests (
                        replay_id, job_id, stage, requested_by, reason,
                        replayed_units
                    ) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6)
                    """,
                    replay_id,
                    job_id,
                    stage.value if stage else None,
                    requested_by,
                    reason,
                    len(rows),
                )
        return ReplayResult(replay_id, job_id, len(rows))
