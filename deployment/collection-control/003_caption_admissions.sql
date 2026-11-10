-- Additive: source record lineage and historical source admissions are unchanged.
CREATE TABLE IF NOT EXISTS collection_control.caption_admissions (
 stage_key text PRIMARY KEY REFERENCES public.stage_units(idempotency_key),
 collection_id text NOT NULL REFERENCES collection_control.collections,
 document_id text REFERENCES public.documents(document_id),
 parent_source_stage_key text REFERENCES collection_control.stage_admissions(stage_key),
 source_revision_id text NOT NULL,
 record_id text NOT NULL,
 record_sha256 text NOT NULL CHECK(record_sha256 ~ '^[0-9a-f]{64}$'),
 stage_artifact_sha256 text NOT NULL CHECK(stage_artifact_sha256 ~ '^[0-9a-f]{64}$'),
 envelope_sha256 text NOT NULL CHECK(envelope_sha256 ~ '^[0-9a-f]{64}$'),
 proof jsonb NOT NULL,
 verified_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS collection_control.caption_record_lineage (
 operation_id uuid NOT NULL REFERENCES collection_control.generation_admissions,
 record_id text NOT NULL,
 record_sha256 text NOT NULL CHECK(record_sha256 ~ '^[0-9a-f]{64}$'),
 stage_key text NOT NULL REFERENCES collection_control.caption_admissions(stage_key),
 PRIMARY KEY(operation_id,record_id)
);

-- A NULL source-stage reference is admitted only with the capture-only proof
-- validated against an actual complete, committed empty EMBEDDING checkpoint.
ALTER TABLE collection_control.caption_admissions ALTER COLUMN parent_source_stage_key DROP NOT NULL;

ALTER TABLE collection_control.caption_admissions ADD COLUMN IF NOT EXISTS document_id text REFERENCES public.documents(document_id);
