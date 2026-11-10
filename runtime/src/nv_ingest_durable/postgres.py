"""PostgreSQL manifest, lease, idempotency, and transactional outbox adapter."""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any
from uuid import uuid4

from .contracts import ClaimedMessage, CommittedArtifact, OutboxEvent, StageEnvelope


def _json_object(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


class LeaseLostError(RuntimeError):
    """Raised when a worker attempts to commit without owning the stage lease."""


class PostgresManifestRepository:
    def __init__(self, pool: Any) -> None:
        self.pool = pool

    @classmethod
    async def connect(cls, dsn: str, **kwargs: Any) -> "PostgresManifestRepository":
        import asyncpg

        kwargs.setdefault("min_size", 1)
        kwargs.setdefault("max_size", 4)
        return cls(await asyncpg.create_pool(dsn, **kwargs))

    async def close(self) -> None:
        await self.pool.close()

    async def get_committed(self, idempotency_key: str) -> CommittedArtifact | None:
        row = await self.pool.fetchrow(
            """
            SELECT artifact_uri, artifact_digest, artifact_size_bytes,
                   artifact_metadata
              FROM stage_units
             WHERE idempotency_key = $1 AND status = 'committed'
            """,
            idempotency_key,
        )
        if row is None:
            return None
        return CommittedArtifact(
            uri=row["artifact_uri"],
            digest=row["artifact_digest"],
            size_bytes=row["artifact_size_bytes"],
            metadata=_json_object(row["artifact_metadata"]),
        )

    async def acquire_lease(
        self,
        envelope: StageEnvelope,
        *,
        owner: str,
        ttl: timedelta,
    ) -> str | None:
        token = str(uuid4())
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    INSERT INTO stage_units (
                        idempotency_key, job_id, document_id, stage, status,
                        attempt_count, lease_owner, lease_token, lease_expires_at
                    ) VALUES ($1, $2::uuid, $3, $4, 'running', 1, $5, $6::uuid,
                              now() + $7::interval)
                    ON CONFLICT (idempotency_key) DO UPDATE SET
                        status = 'running',
                        attempt_count = stage_units.attempt_count + 1,
                        lease_owner = EXCLUDED.lease_owner,
                        lease_token = EXCLUDED.lease_token,
                        lease_expires_at = EXCLUDED.lease_expires_at,
                        updated_at = now()
                    WHERE stage_units.status <> 'committed'
                      AND (stage_units.lease_token IS NULL
                           OR stage_units.lease_expires_at < now())
                    RETURNING attempt_count
                    """,
                    envelope.idempotency_key,
                    envelope.job_id,
                    envelope.document_id,
                    envelope.stage.value,
                    owner,
                    token,
                    ttl,
                )
                if row is None:
                    return None
                await connection.execute(
                    """
                    INSERT INTO stage_attempts (
                        idempotency_key, attempt_number, worker_owner
                    ) VALUES ($1, $2, $3)
                    """,
                    envelope.idempotency_key,
                    row["attempt_count"],
                    owner,
                )
        return token

    async def commit_stage(
        self,
        envelope: StageEnvelope,
        *,
        lease_token: str,
        artifact: CommittedArtifact,
        downstream: tuple[tuple[str, StageEnvelope], ...],
    ) -> None:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    UPDATE stage_units
                       SET status = 'committed',
                           artifact_uri = $3,
                           artifact_digest = $4,
                           artifact_size_bytes = $5,
                           artifact_metadata = $6::jsonb,
                           committed_at = now(),
                           updated_at = now(),
                           lease_owner = NULL,
                           lease_token = NULL,
                           lease_expires_at = NULL,
                           last_error = NULL
                     WHERE idempotency_key = $1
                       AND lease_token = $2::uuid
                    RETURNING attempt_count
                    """,
                    envelope.idempotency_key,
                    lease_token,
                    artifact.uri,
                    artifact.digest,
                    artifact.size_bytes,
                    json.dumps(dict(artifact.metadata)),
                )
                if row is None:
                    status = await connection.fetchval(
                        "SELECT status FROM stage_units WHERE idempotency_key = $1",
                        envelope.idempotency_key,
                    )
                    if status == "committed":
                        return
                    raise LeaseLostError(
                        f"lease lost before commit: {envelope.idempotency_key}"
                    )

                for stream, downstream_envelope in downstream:
                    await connection.execute(
                        """
                        INSERT INTO outbox_events (
                            event_id, idempotency_key, target_stream, envelope
                        ) VALUES ($1::uuid, $2, $3, $4::jsonb)
                        ON CONFLICT (idempotency_key, target_stream,
                                     (envelope->>'message_id')) DO NOTHING
                        """,
                        str(uuid4()),
                        envelope.idempotency_key,
                        stream,
                        json.dumps(downstream_envelope.to_dict()),
                    )

                await connection.execute(
                    """
                    UPDATE stage_attempts
                       SET finished_at = now(), outcome = 'committed'
                     WHERE idempotency_key = $1 AND attempt_number = $2
                    """,
                    envelope.idempotency_key,
                    row["attempt_count"],
                )

                terminal = envelope.stage.value == "index" or (
                    not downstream
                    and artifact.metadata.get("index_skipped")
                    == "no_writable_rows"
                )
                if terminal:
                    await connection.execute(
                        """
                        UPDATE documents
                           SET status = 'completed', updated_at = now()
                         WHERE document_id = $1
                        """,
                        envelope.document_id,
                    )
                    await connection.execute(
                        """
                        UPDATE jobs AS j
                           SET status = 'completed', updated_at = now()
                         WHERE j.job_id = $1::uuid
                           AND NOT EXISTS (
                               SELECT 1 FROM documents AS d
                                WHERE d.job_id = j.job_id
                                  AND d.status <> 'completed'
                           )
                        """,
                        envelope.job_id,
                    )

    async def record_failure(
        self,
        envelope: StageEnvelope,
        *,
        lease_token: str,
        error: BaseException,
    ) -> int:
        error_json = json.dumps(
            {"type": type(error).__name__, "message": str(error)[:4000]}
        )
        attempt_count = 0
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    UPDATE stage_units
                       SET status = 'failed', last_error = $3::jsonb,
                           updated_at = now(), lease_owner = NULL,
                           lease_token = NULL, lease_expires_at = NULL
                     WHERE idempotency_key = $1 AND lease_token = $2::uuid
                    RETURNING attempt_count
                    """,
                    envelope.idempotency_key,
                    lease_token,
                    error_json,
                )
                if row is not None:
                    attempt_count = row["attempt_count"]
                    await connection.execute(
                        """
                        UPDATE stage_attempts
                           SET finished_at = now(), outcome = 'failed',
                               error = $3::jsonb
                         WHERE idempotency_key = $1 AND attempt_number = $2
                        """,
                        envelope.idempotency_key,
                        attempt_count,
                        error_json,
                    )
        return attempt_count

    async def renew_lease(
        self,
        envelope: StageEnvelope,
        *,
        lease_token: str,
        ttl: timedelta,
    ) -> bool:
        result = await self.pool.execute(
            """
            UPDATE stage_units
               SET lease_expires_at = now() + $3::interval, updated_at = now()
             WHERE idempotency_key = $1 AND lease_token = $2::uuid
               AND status = 'running'
            """,
            envelope.idempotency_key,
            lease_token,
            ttl,
        )
        return result == "UPDATE 1"

    async def release_lease(
        self, envelope: StageEnvelope, *, lease_token: str
    ) -> None:
        await self.pool.execute(
            """
            UPDATE stage_units
               SET lease_owner = NULL, lease_token = NULL,
                   lease_expires_at = NULL, updated_at = now()
             WHERE idempotency_key = $1 AND lease_token = $2::uuid
               AND status <> 'committed'
            """,
            envelope.idempotency_key,
            lease_token,
        )

    async def deadletter(
        self,
        message: ClaimedMessage,
        *,
        error: BaseException,
        target_stream: str,
    ) -> None:
        envelope = message.envelope
        error_json = json.dumps(
            {"type": type(error).__name__, "message": str(error)[:4000]}
        )
        envelope_json = json.dumps(envelope.to_dict())
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    """
                    UPDATE stage_units
                       SET status = 'deadlettered', updated_at = now(),
                           lease_owner = NULL, lease_token = NULL,
                           lease_expires_at = NULL
                     WHERE idempotency_key = $1
                    """,
                    envelope.idempotency_key,
                )
                await connection.execute(
                    """
                    INSERT INTO deadletters (
                        idempotency_key, source_stream, source_message_id,
                        envelope, error
                    ) VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)
                    ON CONFLICT (source_stream, source_message_id) DO NOTHING
                    """,
                    envelope.idempotency_key,
                    message.stream,
                    message.redis_id,
                    envelope_json,
                    error_json,
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
                    envelope.idempotency_key,
                    target_stream,
                    envelope_json,
                )

    async def fetch_unpublished_outbox(self, *, limit: int) -> list[OutboxEvent]:
        rows = await self.pool.fetch(
            """
            SELECT event_id::text, target_stream, envelope
              FROM outbox_events
             WHERE published_at IS NULL
             ORDER BY created_at
             LIMIT $1
            """,
            limit,
        )
        return [
            OutboxEvent(
                event_id=row["event_id"],
                stream=row["target_stream"],
                envelope=StageEnvelope.from_stream_fields(_json_object(row["envelope"])),
            )
            for row in rows
        ]

    async def mark_outbox_published(self, event_id: str, stream_id: str) -> None:
        await self.pool.execute(
            """
            UPDATE outbox_events
               SET published_at = now(), broker_message_id = $2,
                   publish_attempts = publish_attempts + 1
             WHERE event_id = $1::uuid AND published_at IS NULL
            """,
            event_id,
            stream_id,
        )
