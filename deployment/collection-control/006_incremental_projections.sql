-- Physical version storage is explicit and durable; old operations stay readable.
ALTER TABLE collection_control.operations ADD COLUMN IF NOT EXISTS storage_mode text NOT NULL DEFAULT 'generation' CHECK(storage_mode IN ('generation','record_versions'));
-- Bounded page batches share one operation, preserving every original group proof.
ALTER TABLE collection_control.web_groups DROP CONSTRAINT IF EXISTS web_groups_operation_id_key;
CREATE TABLE IF NOT EXISTS collection_control.web_operation_groups (
 operation_id uuid NOT NULL REFERENCES collection_control.operations,
 group_id uuid NOT NULL UNIQUE REFERENCES collection_control.web_groups,
 ordinal integer NOT NULL,
 PRIMARY KEY(operation_id,group_id), UNIQUE(operation_id,ordinal)
);
-- Exact new-version byte verification survives worker/process restarts. These
-- entries record verification, never grant query/publication authorization.
CREATE TABLE IF NOT EXISTS collection_control.projection_versions (
 handle text NOT NULL,
 backend text NOT NULL CHECK(backend IN ('elasticsearch','milvus')),
 record_sha256 text NOT NULL CHECK(record_sha256 ~ '^[0-9a-f]{64}$'),
 verified_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(handle,backend,record_sha256)
);
