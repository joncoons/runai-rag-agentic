BEGIN;

ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS submission_key TEXT;

ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS expected_documents INTEGER NOT NULL DEFAULT 0;

CREATE UNIQUE INDEX IF NOT EXISTS jobs_submission_key_idx
    ON jobs (corpus_namespace, submission_key)
    WHERE submission_key IS NOT NULL;

ALTER TABLE stage_units
    ADD COLUMN IF NOT EXISTS input_envelope JSONB;

CREATE INDEX IF NOT EXISTS stage_units_pending_reconcile_idx
    ON stage_units (stage, created_at)
    WHERE status = 'pending' AND input_envelope IS NOT NULL;

COMMIT;
