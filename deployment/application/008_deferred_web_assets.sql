-- Streaming page receipts and deferred acquisition are independent of extraction.
CREATE TABLE IF NOT EXISTS application.web_page_admissions (
 observation_id text NOT NULL,
 collection_id text NOT NULL REFERENCES application.collections,
 canonical_uri text NOT NULL,
 raw_sha256 text NOT NULL,
 processing_identity text NOT NULL,
 job_id uuid NOT NULL REFERENCES application.jobs,
 parent_source_revision_id text NOT NULL REFERENCES application.source_revisions,
 PRIMARY KEY(observation_id,collection_id,canonical_uri)
);
CREATE TABLE IF NOT EXISTS application.web_asset_downloads (
 download_id uuid PRIMARY KEY,
 observation_id text NOT NULL,
 collection_id text NOT NULL REFERENCES application.collections,
 requested_uri text NOT NULL,
 kind text NOT NULL CHECK(kind IN ('image','binary')),
 policy jsonb NOT NULL,
 policy_sha256 text NOT NULL,
 state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','running','retry','completed','failed','cancelled')),
 fence bigint NOT NULL DEFAULT 0,
 owner text,
 lease_until timestamptz,
 attempts integer NOT NULL DEFAULT 0 CHECK(attempts BETWEEN 0 AND 4),
 not_before timestamptz NOT NULL DEFAULT clock_timestamp(),
 raw_uri text,
 raw_sha256 text,
 acquisition_uri text,
 acquisition_sha256 text,
 child_job_id uuid REFERENCES application.jobs,
 child_source_revision_id text REFERENCES application.source_revisions,
 publication_manifest_uri text,
 publication_manifest_sha256 text,
 error_code text,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS web_asset_downloads_due ON application.web_asset_downloads(state,not_before);
CREATE TABLE IF NOT EXISTS application.web_asset_references (
 reference_id uuid PRIMARY KEY,
 download_id uuid NOT NULL REFERENCES application.web_asset_downloads,
 parent_source_revision_id text NOT NULL REFERENCES application.source_revisions,
 parent_document_id text NOT NULL REFERENCES public.documents,
 parent_job_id uuid NOT NULL REFERENCES application.jobs,
 parent_url text NOT NULL,
 locator text NOT NULL,
 alt text NOT NULL,
 captions_enabled boolean NOT NULL,
 source_run_id uuid REFERENCES application.website_source_runs,
 source_version bigint,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK((source_run_id IS NULL)=(source_version IS NULL))
);
CREATE INDEX IF NOT EXISTS web_asset_references_request ON application.web_asset_references(download_id);
CREATE TABLE IF NOT EXISTS application.web_asset_acquisition_outbox (
 event_id uuid PRIMARY KEY,
 download_id uuid NOT NULL REFERENCES application.web_asset_downloads,
 not_before timestamptz NOT NULL DEFAULT clock_timestamp(),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 published_at timestamptz,
 redis_id text
);
CREATE INDEX IF NOT EXISTS web_asset_acquisition_outbox_due ON application.web_asset_acquisition_outbox(not_before) WHERE published_at IS NULL;
ALTER TABLE application.source_revisions ADD COLUMN IF NOT EXISTS web_asset_download_id uuid REFERENCES application.web_asset_downloads;
