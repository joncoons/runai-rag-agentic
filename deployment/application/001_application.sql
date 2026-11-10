CREATE SCHEMA IF NOT EXISTS application;
CREATE TABLE IF NOT EXISTS application.collections (
 collection_id text PRIMARY KEY REFERENCES collection_control.collections,
 name text NOT NULL,
 requested_milvus boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS application.jobs (
 job_id uuid PRIMARY KEY REFERENCES public.jobs,
 collection_id text NOT NULL REFERENCES application.collections,
 status text NOT NULL DEFAULT 'queued',
 stage text NOT NULL DEFAULT 'queued',
 errors jsonb NOT NULL DEFAULT '[]'::jsonb,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS application.sources (
 source_id text PRIMARY KEY,
 collection_id text NOT NULL REFERENCES application.collections,
 canonical_uri text NOT NULL,
 current_revision_id text,
 UNIQUE(collection_id,canonical_uri)
);
CREATE TABLE IF NOT EXISTS application.source_revisions (
 source_revision_id text PRIMARY KEY,
 source_id text NOT NULL REFERENCES application.sources,
 collection_id text NOT NULL,
 document_id text NOT NULL UNIQUE REFERENCES public.documents,
 job_id uuid NOT NULL REFERENCES public.jobs,
 canonical_uri text NOT NULL,
 filename text NOT NULL,
 raw_sha256 text NOT NULL,
 raw_uri text NOT NULL,
 observed_at timestamptz NOT NULL,
 ingestion_run_id text NOT NULL,
 page_count integer NOT NULL CHECK(page_count>0),
 previous_source_revision_id text,
 admission_stage_key text,
 operation_id uuid,
 status text NOT NULL DEFAULT 'queued',
 records integer NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS application.events (
 event_id bigserial PRIMARY KEY,
 job_id uuid REFERENCES application.jobs,
 event_type text NOT NULL,
 payload jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS application.caption_publications (
 caption_stage_key text PRIMARY KEY REFERENCES public.stage_units,
 operation_id uuid,
 status text NOT NULL DEFAULT 'pending',
 last_error_type text,
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
