-- Additive only: existing PDF revisions and proofs remain unchanged.
ALTER TABLE application.source_revisions ALTER COLUMN page_count DROP NOT NULL;
ALTER TABLE application.source_revisions ADD COLUMN IF NOT EXISTS source_kind text NOT NULL DEFAULT 'pdf';
ALTER TABLE application.source_revisions ADD COLUMN IF NOT EXISTS media_type text NOT NULL DEFAULT 'application/pdf';
ALTER TABLE application.source_revisions ADD COLUMN IF NOT EXISTS source_units jsonb;
ALTER TABLE application.source_revisions ADD COLUMN IF NOT EXISTS acquisition jsonb;
-- Distinct captured URLs may legitimately have identical original bytes.
-- Legacy intake still content-deduplicates inputs; its ON CONFLICT target is
-- document_id, not this constraint. Existing rows/identities are untouched.
DROP INDEX IF EXISTS public.documents_job_uri_digest_key;
-- document_id already hashes job + canonical URI + raw SHA in application
-- intake and remains the primary key. Do not index unbounded URL text or
-- duplicate this identity with a second index.
DROP INDEX IF EXISTS public.documents_job_uri_sha_digest_key;
ALTER TABLE public.documents DROP CONSTRAINT IF EXISTS documents_job_id_source_digest_key;
