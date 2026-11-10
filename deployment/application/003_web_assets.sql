ALTER TABLE application.source_revisions ADD COLUMN IF NOT EXISTS web_group_id uuid;
ALTER TABLE application.source_revisions ADD COLUMN IF NOT EXISTS web_role text;
