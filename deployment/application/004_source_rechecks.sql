-- ADR0047: durable saved website acquisition specs and fenced recheck ledger.
CREATE TABLE IF NOT EXISTS application.website_sources (
 source_id uuid PRIMARY KEY,
 collection_id text NOT NULL REFERENCES application.collections,
 name text NOT NULL,
 seed_urls jsonb NOT NULL CHECK(jsonb_typeof(seed_urls)='array'),
 policy jsonb NOT NULL CHECK(jsonb_typeof(policy)='object'),
 schedule jsonb NOT NULL CHECK(jsonb_typeof(schedule)='object'),
 paused boolean NOT NULL DEFAULT false,
 version bigint NOT NULL DEFAULT 1 CHECK(version>0),
 next_fire timestamptz,
 last_fire timestamptz,
 last_successful_run_id uuid,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS application.website_source_runs (
 run_id uuid PRIMARY KEY,
 source_id uuid NOT NULL REFERENCES application.website_sources,
 collection_id text NOT NULL REFERENCES application.collections,
 source_version bigint NOT NULL,
 spec jsonb NOT NULL,
 nominal_fire timestamptz NOT NULL,
 trigger text NOT NULL CHECK(trigger IN ('manual','schedule')),
 state text NOT NULL DEFAULT 'queued' CHECK(state IN ('queued','scanning','deferred','accepted','published','unchanged','failed','cancelled')),
 fence bigint NOT NULL DEFAULT 0,
 owner text,
 lease_until timestamptz,
 deadline timestamptz NOT NULL,
 not_before timestamptz NOT NULL DEFAULT clock_timestamp(),
 attempts integer NOT NULL DEFAULT 0 CHECK(attempts BETWEEN 0 AND 3),
 job_id uuid UNIQUE REFERENCES application.jobs,
 fingerprint text,
 manifest_uri text,
 manifest_sha256 text,
 observations jsonb NOT NULL DEFAULT '[]'::jsonb,
 counts jsonb NOT NULL DEFAULT '{}'::jsonb,
 error_code text,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 finished_at timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS website_source_one_active ON application.website_source_runs(source_id)
 WHERE state IN ('queued','scanning','accepted','deferred');
CREATE UNIQUE INDEX IF NOT EXISTS website_source_daily_fire ON application.website_source_runs(source_id,source_version,nominal_fire)
 WHERE trigger='schedule';
CREATE INDEX IF NOT EXISTS website_source_run_history ON application.website_source_runs(source_id,created_at DESC);
