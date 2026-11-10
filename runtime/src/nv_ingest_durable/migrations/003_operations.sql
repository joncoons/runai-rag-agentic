BEGIN;

CREATE TABLE IF NOT EXISTS replay_requests (
    replay_id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(job_id),
    stage TEXT,
    requested_by TEXT NOT NULL,
    reason TEXT NOT NULL,
    replayed_units INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS replay_requests_job_idx
    ON replay_requests (job_id, created_at DESC);

COMMIT;
