"""Transactional PostgreSQL submission registration and restart reconciliation."""

from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

from .contracts import Stage, StageEnvelope
from .coordinator import PreparedDocument, RegistrationOutcome, SubmissionSpec


class SubmissionConflictError(RuntimeError):
    pass


class PostgresSubmissionRepository:
    def __init__(self, pool: Any) -> None:
        self.pool = pool

    async def register_submission(
        self, spec: SubmissionSpec, documents: tuple[PreparedDocument, ...]
    ) -> RegistrationOutcome:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                inserted = await connection.fetchval(
                    """
                    INSERT INTO jobs (
                        job_id, corpus_namespace, submission_key, status,
                        pipeline_version, expected_documents
                    ) VALUES ($1::uuid, $2, $3, 'accepted', $4, $5)
                    ON CONFLICT (corpus_namespace, submission_key)
                        WHERE submission_key IS NOT NULL
                    DO NOTHING
                    RETURNING job_id::text
                    """,
                    spec.job_id,
                    spec.corpus_namespace,
                    spec.submission_key,
                    spec.pipeline_version,
                    len(documents),
                )
                created = inserted is not None
                canonical_job_id = inserted
                if canonical_job_id is None:
                    row = await connection.fetchrow(
                        """
                        SELECT job_id::text, pipeline_version
                          FROM jobs
                         WHERE corpus_namespace = $1 AND submission_key = $2
                        """,
                        spec.corpus_namespace,
                        spec.submission_key,
                    )
                    if row is None:
                        raise RuntimeError("submission conflict did not resolve")
                    canonical_job_id = row["job_id"]
                    if row["pipeline_version"] != spec.pipeline_version:
                        raise SubmissionConflictError(
                            "submission key already exists with another pipeline version"
                        )

                if canonical_job_id != spec.job_id:
                    count = await connection.fetchval(
                        "SELECT expected_documents FROM jobs WHERE job_id = $1::uuid",
                        canonical_job_id,
                    )
                    return RegistrationOutcome(canonical_job_id, False, count)

                for document in documents:
                    await connection.execute(
                        """
                        INSERT INTO documents (
                            document_id, job_id, source_uri, source_digest, status
                        ) VALUES ($1, $2::uuid, $3, $4, 'accepted')
                        ON CONFLICT (document_id) DO NOTHING
                        """,
                        document.document_id,
                        spec.job_id,
                        document.source.uri,
                        document.source.digest,
                    )
                    envelope_json = json.dumps(document.envelope.to_dict())
                    await connection.execute(
                        """
                        INSERT INTO stage_units (
                            idempotency_key, job_id, document_id, stage, status,
                            input_envelope
                        ) VALUES ($1, $2::uuid, $3, $4, 'pending', $5::jsonb)
                        ON CONFLICT (idempotency_key) DO NOTHING
                        """,
                        document.envelope.idempotency_key,
                        spec.job_id,
                        document.document_id,
                        document.envelope.stage.value,
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
                        document.envelope.idempotency_key,
                        spec.target_stream,
                        envelope_json,
                    )

                scheduled = await connection.fetchval(
                    "SELECT count(*) FROM documents WHERE job_id = $1::uuid",
                    spec.job_id,
                )
                await connection.execute(
                    """
                    UPDATE jobs
                       SET expected_documents = $2, updated_at = now()
                     WHERE job_id = $1::uuid
                    """,
                    spec.job_id,
                    scheduled,
                )
                return RegistrationOutcome(spec.job_id, created, scheduled)

    async def repair_initial_outbox(
        self,
        *,
        target_stream: str = "ingest.extraction.pending",
        stage: Stage = Stage.EXTRACTION,
        limit: int = 1000,
    ) -> int:
        repaired = 0
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                rows = await connection.fetch(
                    """
                    SELECT s.idempotency_key, s.input_envelope
                      FROM stage_units s
                     WHERE s.status = 'pending'
                       AND s.stage = $1
                       AND s.input_envelope IS NOT NULL
                       AND NOT EXISTS (
                           SELECT 1 FROM outbox_events o
                            WHERE o.idempotency_key = s.idempotency_key
                              AND o.target_stream = $2
                       )
                     ORDER BY s.created_at
                     LIMIT $3
                     FOR UPDATE OF s SKIP LOCKED
                    """,
                    stage.value,
                    target_stream,
                    limit,
                )
                for row in rows:
                    envelope = row["input_envelope"]
                    if not isinstance(envelope, str):
                        envelope = json.dumps(dict(envelope))
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
                        envelope,
                    )
                    repaired += 1
        return repaired
