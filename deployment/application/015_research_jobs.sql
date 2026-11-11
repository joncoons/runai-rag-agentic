CREATE TABLE IF NOT EXISTS application.research_runs (
 run_id uuid PRIMARY KEY,
 owner_id uuid NOT NULL,
 workspace_id uuid,
 request_key text NOT NULL,
 request_sha256 text NOT NULL,
 request jsonb NOT NULL,
 status text NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','completed','failed','cancelled')),
 checkpoint jsonb NOT NULL DEFAULT '{}',
 report text NOT NULL DEFAULT '',
 error_type text,
 attempts integer NOT NULL DEFAULT 0,
 max_attempts integer NOT NULL DEFAULT 3,
 lease_token uuid,
 lease_until timestamptz,
 next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 expires_at timestamptz NOT NULL DEFAULT clock_timestamp()+interval '7 days',
 UNIQUE(owner_id,request_key)
);
CREATE TABLE IF NOT EXISTS application.research_events (
 event_id bigserial PRIMARY KEY,
 run_id uuid NOT NULL REFERENCES application.research_runs ON DELETE CASCADE,
 kind text NOT NULL,
 payload jsonb NOT NULL DEFAULT '{}',
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS research_events_run ON application.research_events(run_id,event_id);
CREATE INDEX IF NOT EXISTS research_due ON application.research_runs(status,next_attempt_at,lease_until);
CREATE TABLE IF NOT EXISTS application.research_outbox (
 run_id uuid PRIMARY KEY REFERENCES application.research_runs ON DELETE CASCADE,
 published_at timestamptz
);
