-- Additive v2 child candidates and immutable publication/enrichment transitions.
CREATE TABLE IF NOT EXISTS collection_control.web_child_candidates (
 stage_key text PRIMARY KEY REFERENCES public.stage_units(idempotency_key),
 collection_id text NOT NULL REFERENCES collection_control.collections,
 document_id text NOT NULL REFERENCES public.documents,
 source_revision_id text NOT NULL,
 publication_uri text NOT NULL,
 publication_sha256 text NOT NULL CHECK(publication_sha256 ~ '^[0-9a-f]{64}$'),
 stage_artifact_sha256 text NOT NULL,
 envelope_sha256 text NOT NULL,
 provenance_sha256 text NOT NULL,
 record_count bigint NOT NULL CHECK(record_count>0),
 proof jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS collection_control.web_child_publications (
 operation_id uuid PRIMARY KEY REFERENCES collection_control.operations,
 base_operation_id uuid NOT NULL REFERENCES collection_control.operations,
 publication_uri text NOT NULL,
 publication_sha256 text NOT NULL CHECK(publication_sha256 ~ '^[0-9a-f]{64}$'),
 caption_stage_key text REFERENCES public.stage_units(idempotency_key)
);
CREATE TABLE IF NOT EXISTS collection_control.web_page_derivations (
 record_sha256 text PRIMARY KEY CHECK(record_sha256 ~ '^[0-9a-f]{64}$'),
 collection_id text NOT NULL REFERENCES collection_control.collections,
 parent_stage_key text NOT NULL REFERENCES public.stage_units(idempotency_key),
 proof_uri text NOT NULL,
 proof_sha256 text NOT NULL CHECK(proof_sha256 ~ '^[0-9a-f]{64}$')
);

ALTER TABLE collection_control.web_child_publications ADD COLUMN IF NOT EXISTS caption_stage_key text REFERENCES public.stage_units(idempotency_key);
