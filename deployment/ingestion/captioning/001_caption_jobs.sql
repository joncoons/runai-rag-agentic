-- Stage status, attempt counts, leases and delivery remain in the existing
-- stage_units/stage_attempts/outbox_events tables. This table only adds
-- per-image identity, retry scheduling and terminal-failure policy.
CREATE SCHEMA IF NOT EXISTS caption_control;
CREATE TABLE IF NOT EXISTS caption_control.assets (
 stage_key text PRIMARY KEY,
 parent_job_id uuid NOT NULL,
 document_id text NOT NULL,
 source_digest text NOT NULL,
 source_uri text NOT NULL,
 collection_id text NOT NULL,
 source_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
 manifest_uri text NOT NULL,
 manifest_sha256 text NOT NULL,
 asset_sha256 text NOT NULL,
 next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 max_attempts integer NOT NULL DEFAULT 4 CHECK(max_attempts BETWEEN 1 AND 4),
 terminal_reason text,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
