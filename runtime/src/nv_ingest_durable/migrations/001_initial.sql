BEGIN;

CREATE TABLE IF NOT EXISTS jobs (
    job_id UUID PRIMARY KEY,
    corpus_namespace TEXT NOT NULL,
    status TEXT NOT NULL,
    pipeline_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS documents (
    document_id TEXT PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(job_id),
    source_uri TEXT NOT NULL,
    source_digest TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (job_id, source_digest)
);

CREATE TABLE IF NOT EXISTS stage_units (
    idempotency_key TEXT PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(job_id),
    document_id TEXT NOT NULL REFERENCES documents(document_id),
    stage TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_token UUID,
    lease_expires_at TIMESTAMPTZ,
    artifact_uri TEXT,
    artifact_digest TEXT,
    artifact_size_bytes BIGINT,
    artifact_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_error JSONB,
    committed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS stage_units_reclaim_idx
    ON stage_units (status, lease_expires_at);

CREATE TABLE IF NOT EXISTS stage_attempts (
    attempt_id BIGSERIAL PRIMARY KEY,
    idempotency_key TEXT NOT NULL REFERENCES stage_units(idempotency_key),
    attempt_number INTEGER NOT NULL,
    worker_owner TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    outcome TEXT,
    error JSONB,
    UNIQUE (idempotency_key, attempt_number)
);

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id UUID PRIMARY KEY,
    idempotency_key TEXT NOT NULL REFERENCES stage_units(idempotency_key),
    target_stream TEXT NOT NULL,
    envelope JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    broker_message_id TEXT,
    publish_attempts INTEGER NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS outbox_dedup_idx
    ON outbox_events (idempotency_key, target_stream,
                      (envelope->>'message_id'));

CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox_events (created_at) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS deadletters (
    deadletter_id BIGSERIAL PRIMARY KEY,
    idempotency_key TEXT,
    source_stream TEXT NOT NULL,
    source_message_id TEXT NOT NULL,
    envelope JSONB NOT NULL,
    error JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS deadletters_source_message_idx
    ON deadletters (source_stream, source_message_id);

COMMIT;
