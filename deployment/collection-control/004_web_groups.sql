-- Additive grouped web source admission; legacy source/caption proofs remain valid.
CREATE TABLE IF NOT EXISTS collection_control.web_groups (
 group_id uuid PRIMARY KEY,
 collection_id text NOT NULL REFERENCES collection_control.collections,
 job_id uuid NOT NULL REFERENCES public.jobs,
 manifest_uri text NOT NULL,
 manifest_sha256 text NOT NULL CHECK(manifest_sha256 ~ '^[0-9a-f]{64}$'),
 state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','prepared','published','cancelled')),
 operation_id uuid UNIQUE REFERENCES collection_control.operations
);
CREATE TABLE IF NOT EXISTS collection_control.web_group_members (
 group_id uuid NOT NULL REFERENCES collection_control.web_groups,
 document_id text NOT NULL REFERENCES public.documents,
 source_revision_id text NOT NULL,
 role text NOT NULL CHECK(role IN ('page','binary','image')),
 canonical_uri text NOT NULL,
 PRIMARY KEY(group_id,document_id)
);
CREATE TABLE IF NOT EXISTS collection_control.web_source_candidates (
 stage_key text PRIMARY KEY REFERENCES public.stage_units,
 group_id uuid NOT NULL REFERENCES collection_control.web_groups,
 document_id text NOT NULL REFERENCES public.documents,
 source_revision_id text NOT NULL,
 stage_artifact_sha256 text NOT NULL,
 envelope_sha256 text NOT NULL,
 provenance_sha256 text NOT NULL,
 raw_sha256 text NOT NULL,
 record_count bigint NOT NULL CHECK(record_count>=0),
 proof jsonb NOT NULL,
 UNIQUE(group_id,document_id),
 FOREIGN KEY(group_id,document_id) REFERENCES collection_control.web_group_members
);
CREATE TABLE IF NOT EXISTS collection_control.web_generation_manifests (
 operation_id uuid PRIMARY KEY REFERENCES collection_control.operations,
 group_id uuid REFERENCES collection_control.web_groups,
 manifest_uri text NOT NULL,
 manifest_sha256 text NOT NULL,
 raw_root text NOT NULL,
 stage_root text NOT NULL,
 contract jsonb NOT NULL,
 inherited_from uuid REFERENCES collection_control.operations
);
ALTER TABLE collection_control.stage_admissions DROP CONSTRAINT IF EXISTS stage_admissions_operation_id_key;
ALTER TABLE collection_control.generation_admissions DROP CONSTRAINT IF EXISTS generation_admissions_row_count_check;
ALTER TABLE collection_control.generation_admissions ADD CONSTRAINT generation_admissions_row_count_check CHECK(row_count>=0);

ALTER TABLE collection_control.operations ADD COLUMN IF NOT EXISTS web_graph_required boolean NOT NULL DEFAULT false;
