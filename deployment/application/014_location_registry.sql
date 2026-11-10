CREATE TABLE IF NOT EXISTS application.location_roots (
 alias text PRIMARY KEY CHECK(alias ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$'),
 path text NOT NULL,
 enabled boolean NOT NULL DEFAULT true,
 version integer NOT NULL DEFAULT 1 CHECK(version>0),
 created_by text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS application.location_root_events (
 event_id bigserial PRIMARY KEY,
 alias text NOT NULL,
 action text NOT NULL,
 version integer NOT NULL,
 actor text NOT NULL,
 occurred_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO application.location_roots(alias,path,created_by)
 VALUES('docs-to-data-to-lora','/source-roots/docs-to-data-to-lora','operator-bootstrap')
 ON CONFLICT(alias) DO NOTHING;
