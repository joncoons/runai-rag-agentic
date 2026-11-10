-- Additive collection-control schema. No search_path or legacy tables changed.
CREATE SCHEMA IF NOT EXISTS collection_control;
CREATE TABLE IF NOT EXISTS collection_control.collections (
 collection_id text PRIMARY KEY,
 contract_sha256 text NOT NULL CHECK (contract_sha256 ~ '^[0-9a-f]{64}$'),
 revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
 target_epoch bigint NOT NULL DEFAULT 0,
 milvus_enabled boolean NOT NULL DEFAULT false,
 deleted boolean NOT NULL DEFAULT false,
 current_operation uuid,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS collection_control.operations (
 operation_id uuid PRIMARY KEY,
 collection_id text NOT NULL REFERENCES collection_control.collections,
 idempotency_key text NOT NULL,
 kind text NOT NULL CHECK (kind IN ('snapshot','enroll_milvus','remove_milvus')),
 base_revision bigint NOT NULL,
 base_epoch bigint NOT NULL,
 artifact_sha256 text NOT NULL CHECK (artifact_sha256 ~ '^[0-9a-f]{64}$'),
 targets text[] NOT NULL CHECK (targets IN (ARRAY['elasticsearch'],ARRAY['elasticsearch','milvus'])),
 status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','published','cancelled')),
 fence bigint NOT NULL DEFAULT 0,
 lease_token uuid,
 lease_until timestamptz,
 handles jsonb NOT NULL DEFAULT '{}',
 published_at timestamptz,
 UNIQUE(collection_id,idempotency_key)
);
CREATE UNIQUE INDEX IF NOT EXISTS one_pending_collection_mutation
 ON collection_control.operations(collection_id) WHERE status='pending';
CREATE TABLE IF NOT EXISTS collection_control.attempts (
 operation_id uuid NOT NULL REFERENCES collection_control.operations,
 fence bigint NOT NULL,
 lease_token uuid NOT NULL,
 handles jsonb NOT NULL,
 claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(operation_id,fence)
);
CREATE TABLE IF NOT EXISTS collection_control.receipts (
 operation_id uuid NOT NULL REFERENCES collection_control.operations,
 fence bigint NOT NULL,
 backend text NOT NULL CHECK(backend IN ('elasticsearch','milvus')),
 content_sha256 text NOT NULL CHECK(content_sha256 ~ '^[0-9a-f]{64}$'),
 row_count bigint NOT NULL CHECK(row_count>=0),
 handle text NOT NULL,
 visible_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(operation_id,fence,backend)
);
CREATE TABLE IF NOT EXISTS collection_control.outbox (
 event_id uuid PRIMARY KEY,
 collection_id text NOT NULL REFERENCES collection_control.collections,
 operation_id uuid REFERENCES collection_control.operations,
 event_type text NOT NULL CHECK(event_type IN ('generation_requested','generation_published','generation_cancelled','collection_deleted')),
 payload jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 delivered_at timestamptz
);
CREATE INDEX IF NOT EXISTS collection_outbox_pending ON collection_control.outbox(created_at)
 WHERE delivered_at IS NULL;
