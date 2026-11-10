-- Apply after the local durable public schema and collection-control 001.
-- No search_path, legacy table or role changes.
CREATE TABLE IF NOT EXISTS collection_control.stage_admissions (
 stage_key text PRIMARY KEY REFERENCES public.stage_units(idempotency_key),
 operation_id uuid UNIQUE NOT NULL REFERENCES collection_control.operations,
 collection_id text NOT NULL REFERENCES collection_control.collections,
 document_id text NOT NULL REFERENCES public.documents,
 stage_artifact_sha256 text NOT NULL CHECK(stage_artifact_sha256 ~ '^[0-9a-f]{64}$'),
 snapshot_sha256 text NOT NULL CHECK(snapshot_sha256 ~ '^[0-9a-f]{64}$'),
 provenance_sha256 text NOT NULL CHECK(provenance_sha256 ~ '^[0-9a-f]{64}$'),
 envelope_sha256 text NOT NULL CHECK(envelope_sha256 ~ '^[0-9a-f]{64}$'),
 raw_sha256 text NOT NULL CHECK(raw_sha256 ~ '^[0-9a-f]{64}$'),
 source_revision_id text NOT NULL,
 record_count bigint NOT NULL CHECK(record_count>0),
 proof jsonb NOT NULL,
 verified_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS collection_control.generation_admissions (
 operation_id uuid PRIMARY KEY REFERENCES collection_control.operations,
 artifact_sha256 text NOT NULL CHECK(artifact_sha256 ~ '^[0-9a-f]{64}$'),
 content_sha256 text NOT NULL CHECK(content_sha256 ~ '^[0-9a-f]{64}$'),
 row_count bigint NOT NULL CHECK(row_count>0),
 inherited_from uuid REFERENCES collection_control.operations,
 policy_version text NOT NULL CHECK(policy_version='retained-source-stage.v1')
);
CREATE TABLE IF NOT EXISTS collection_control.record_lineage (
 operation_id uuid NOT NULL REFERENCES collection_control.generation_admissions,
 record_id text NOT NULL,
 record_sha256 text NOT NULL CHECK(record_sha256 ~ '^[0-9a-f]{64}$'),
 stage_key text NOT NULL REFERENCES collection_control.stage_admissions,
 PRIMARY KEY(operation_id,record_id)
);
CREATE TABLE IF NOT EXISTS collection_control.source_revisions (
 collection_id text NOT NULL REFERENCES collection_control.collections,
 source_revision_id text NOT NULL,
 canonical_uri text NOT NULL,
 raw_sha256 text NOT NULL CHECK(raw_sha256 ~ '^[0-9a-f]{64}$'),
 observed_at timestamptz NOT NULL,
 ingestion_run_id text NOT NULL,
 PRIMARY KEY(collection_id,source_revision_id)
);
CREATE TABLE IF NOT EXISTS collection_control.source_chunks (
 collection_id text NOT NULL,
 source_chunk_id text NOT NULL,
 source_revision_id text NOT NULL,
 text_sha256 text NOT NULL CHECK(text_sha256 ~ '^[0-9a-f]{64}$'),
 PRIMARY KEY(collection_id,source_chunk_id),
 FOREIGN KEY(collection_id,source_revision_id) REFERENCES collection_control.source_revisions
);
