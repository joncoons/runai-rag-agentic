CREATE TABLE IF NOT EXISTS application.source_configuration_history (
 source_id uuid NOT NULL REFERENCES application.website_sources,
 version bigint NOT NULL,
 previous_spec jsonb NOT NULL,
 new_spec jsonb NOT NULL,
 actor text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(source_id,version)
);
