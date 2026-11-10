-- Immutable derived-parent proof only; existing CAPTION stages/outbox are the queue.
CREATE TABLE IF NOT EXISTS collection_control.web_caption_derivations (
 record_sha256 text PRIMARY KEY CHECK(record_sha256 ~ '^[0-9a-f]{64}$'),
 collection_id text NOT NULL REFERENCES collection_control.collections,
 parent_stage_key text NOT NULL REFERENCES public.stage_units(idempotency_key),
 proof_uri text NOT NULL,
 proof_sha256 text NOT NULL CHECK(proof_sha256 ~ '^[0-9a-f]{64}$')
);
