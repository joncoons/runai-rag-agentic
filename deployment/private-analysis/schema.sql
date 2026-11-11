CREATE SCHEMA IF NOT EXISTS private_control;
CREATE TABLE IF NOT EXISTS private_control.workspaces (
 workspace_id uuid PRIMARY KEY,
 owner_id uuid NOT NULL,
 collection_id text NOT NULL UNIQUE,
 filename text NOT NULL,
 raw_sha256 text,
 job_id uuid,
 state text NOT NULL DEFAULT 'active' CHECK(state IN ('active','expired','purged')),
 initial_question text NOT NULL,
 initial_run_id uuid,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 expires_at timestamptz NOT NULL,
 expired_at timestamptz,
 cleanup_manifest jsonb NOT NULL DEFAULT '{}',
 cleanup_error text
);
CREATE OR REPLACE FUNCTION private_control.active(cid text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM private_control.workspaces WHERE collection_id=cid AND state='active' AND expires_at>clock_timestamp());
$$;
CREATE OR REPLACE FUNCTION private_control.guard_stage() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE cid text;
BEGIN
 SELECT corpus_namespace INTO cid FROM public.jobs WHERE job_id=NEW.job_id;
 IF NEW.status IN ('pending','running','committed') AND NOT private_control.active(cid) THEN
  RAISE EXCEPTION 'Private workspace expired or revoked' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END; $$;
DROP TRIGGER IF EXISTS private_scope_stage ON public.stage_units;
CREATE TRIGGER private_scope_stage BEFORE INSERT OR UPDATE ON public.stage_units FOR EACH ROW EXECUTE FUNCTION private_control.guard_stage();
CREATE OR REPLACE FUNCTION private_control.guard_publication() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT private_control.active(NEW.collection_id) THEN
  RAISE EXCEPTION 'Private workspace cannot publish after expiry' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END; $$;
DROP TRIGGER IF EXISTS private_scope_publication ON collection_control.operations;
CREATE TRIGGER private_scope_publication BEFORE INSERT OR UPDATE ON collection_control.operations FOR EACH ROW EXECUTE FUNCTION private_control.guard_publication();
