CREATE TABLE IF NOT EXISTS application.document_promotions (
 promotion_id uuid PRIMARY KEY,
 owner_id uuid NOT NULL,
 workspace_id uuid NOT NULL,
 destination text NOT NULL REFERENCES application.collections,
 request_key text NOT NULL,
 source_sha256 text NOT NULL,
 principal text NOT NULL,
 session_hash text NOT NULL,
 policy_version text NOT NULL,
 copy_expires_at timestamptz NOT NULL,
 authorization_expires_at timestamptz NOT NULL,
 revoked_at timestamptz,
 status text NOT NULL DEFAULT 'authorized' CHECK(status IN ('authorized','copying','submitted','completed','cancelled','failed')),
 job_id uuid,
 operation_id uuid,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(owner_id,request_key)
);
CREATE OR REPLACE FUNCTION application.promotion_allowed(p application.document_promotions) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT p.revoked_at IS NULL AND p.authorization_expires_at>clock_timestamp() AND EXISTS(
 SELECT 1 FROM application.local_sessions s JOIN application.local_principals principal ON principal.username=s.principal
 WHERE s.token_sha256=p.session_hash AND s.owner_id=p.owner_id AND s.revoked_at IS NULL AND s.expires_at>clock_timestamp()
 AND principal.enabled AND principal.policy_version=p.policy_version AND s.policy_version=p.policy_version
 AND (principal.collection_write ? p.destination OR principal.collection_write ? '*'));
$$;
-- Authorize only evidence newly added by this operation. Retained, already
-- published evidence must not make future unrelated collection updates depend
-- on the original uploader's session. Async captions remain new evidence.
CREATE OR REPLACE FUNCTION application.publication_promotions(opid uuid)
RETURNS SETOF application.document_promotions LANGUAGE sql STABLE AS $$
 SELECT promo.* FROM application.document_promotions promo
 JOIN collection_control.operations op ON op.operation_id=opid AND op.collection_id=promo.destination
 JOIN collection_control.collections col ON col.collection_id=op.collection_id
 WHERE promo.status IN ('submitted','completed') AND promo.job_id IN (
  SELECT d.job_id FROM collection_control.record_lineage l
   JOIN collection_control.stage_admissions a USING(stage_key)
   JOIN public.documents d ON d.document_id=a.document_id WHERE l.operation_id=opid
   AND NOT EXISTS(SELECT 1 FROM collection_control.record_lineage prior WHERE prior.operation_id=col.current_operation
    AND prior.record_id=l.record_id AND prior.record_sha256=l.record_sha256 AND prior.stage_key=l.stage_key)
  UNION SELECT asset.parent_job_id FROM collection_control.caption_record_lineage l
   JOIN caption_control.assets asset USING(stage_key) WHERE l.operation_id=opid
   AND NOT EXISTS(SELECT 1 FROM collection_control.caption_record_lineage prior WHERE prior.operation_id=col.current_operation
    AND prior.record_id=l.record_id AND prior.record_sha256=l.record_sha256 AND prior.stage_key=l.stage_key));
$$;
CREATE OR REPLACE FUNCTION application.guard_promotion_publication() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE p application.document_promotions;
BEGIN
 IF NEW.status<>'published' OR OLD.status='published' THEN RETURN NEW; END IF;
 FOR p IN SELECT * FROM application.publication_promotions(NEW.operation_id)
 LOOP
  PERFORM s.token_sha256 FROM application.local_sessions s JOIN application.local_principals principal ON principal.username=s.principal WHERE s.token_sha256=p.session_hash FOR SHARE OF s,principal;
  IF NOT application.promotion_allowed(p) THEN
   RAISE EXCEPTION 'Promotion authorization revoked' USING ERRCODE='23514';
  END IF;
  UPDATE application.document_promotions SET status='completed',operation_id=COALESCE(operation_id,NEW.operation_id) WHERE promotion_id=p.promotion_id;
 END LOOP;
 RETURN NEW;
END; $$;
DROP TRIGGER IF EXISTS promotion_publication_guard ON collection_control.operations;
CREATE TRIGGER promotion_publication_guard BEFORE UPDATE OF status ON collection_control.operations FOR EACH ROW EXECUTE FUNCTION application.guard_promotion_publication();
