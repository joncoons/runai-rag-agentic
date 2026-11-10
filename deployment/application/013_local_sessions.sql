CREATE TABLE IF NOT EXISTS application.local_principals (
 username text PRIMARY KEY,
 policy_version text NOT NULL,
 collection_write jsonb NOT NULL DEFAULT '[]',
 enabled boolean NOT NULL DEFAULT true,
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS application.local_sessions (
 token_sha256 text PRIMARY KEY,
 owner_id uuid NOT NULL,
 principal text REFERENCES application.local_principals,
 policy_version text,
 csrf_token text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 expires_at timestamptz NOT NULL,
 revoked_at timestamptz
);
CREATE INDEX IF NOT EXISTS local_sessions_expiry ON application.local_sessions(expires_at);
CREATE TABLE IF NOT EXISTS application.local_login_attempts (
 rate_key text PRIMARY KEY,
 attempts integer NOT NULL,
 window_started timestamptz NOT NULL DEFAULT clock_timestamp()
);
