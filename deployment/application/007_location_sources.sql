-- ADR0050: connector kinds reuse the existing source/schedule/run ledger.
ALTER TABLE application.website_sources ADD COLUMN IF NOT EXISTS kind text NOT NULL DEFAULT 'website' CHECK(kind IN ('website','location'));
ALTER TABLE application.website_sources ADD COLUMN IF NOT EXISTS location jsonb;
ALTER TABLE application.website_sources ADD COLUMN IF NOT EXISTS captions_enabled boolean NOT NULL DEFAULT false;
ALTER TABLE application.website_source_runs DROP CONSTRAINT IF EXISTS website_source_runs_state_check;
ALTER TABLE application.website_source_runs ADD CONSTRAINT website_source_runs_state_check CHECK(state IN ('queued','scanning','deferred','accepted','published','unchanged','observed','failed','cancelled'));
ALTER TABLE application.source_revisions ADD COLUMN IF NOT EXISTS office_visuals_required boolean NOT NULL DEFAULT false;
