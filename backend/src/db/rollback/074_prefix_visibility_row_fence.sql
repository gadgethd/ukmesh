-- Restore 066 without changing packet flags, queue entries or generations.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
DROP TRIGGER IF EXISTS private_node_prefixes_visibility_rows ON private_node_prefixes;
DROP TRIGGER IF EXISTS private_node_prefixes_visibility_truncate ON private_node_prefixes;
CREATE OR REPLACE FUNCTION bump_visibility_for_changed_prefixes()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  IF pg_trigger_depth() > 1 THEN RETURN NULL; END IF;
  IF TG_OP = 'INSERT' THEN
    IF NOT EXISTS (SELECT 1 FROM new_prefix_rows) THEN RETURN NULL; END IF;
  ELSIF TG_OP = 'DELETE' THEN
    IF NOT EXISTS (SELECT 1 FROM old_prefix_rows) THEN RETURN NULL; END IF;
  ELSIF TG_OP = 'UPDATE' THEN
    IF NOT EXISTS (SELECT 1 FROM new_prefix_rows) THEN RETURN NULL; END IF;
  END IF;
  UPDATE public_visibility_state
     SET generation = generation + 1, updated_at = NOW()
   WHERE singleton = TRUE;
  RETURN NULL;
END;
$$;
CREATE TRIGGER private_node_prefixes_visibility_insert
AFTER INSERT ON private_node_prefixes REFERENCING NEW TABLE AS new_prefix_rows
FOR EACH STATEMENT EXECUTE FUNCTION bump_visibility_for_changed_prefixes();
CREATE TRIGGER private_node_prefixes_visibility_update
AFTER UPDATE ON private_node_prefixes REFERENCING NEW TABLE AS new_prefix_rows
FOR EACH STATEMENT EXECUTE FUNCTION bump_visibility_for_changed_prefixes();
CREATE TRIGGER private_node_prefixes_visibility_delete
AFTER DELETE ON private_node_prefixes REFERENCING OLD TABLE AS old_prefix_rows
FOR EACH STATEMENT EXECUTE FUNCTION bump_visibility_for_changed_prefixes();
CREATE TRIGGER private_node_prefixes_visibility_truncate
AFTER TRUNCATE ON private_node_prefixes
FOR EACH STATEMENT EXECUTE FUNCTION bump_visibility_for_changed_prefixes();
ALTER TABLE public_visibility_state DROP COLUMN prefix_fence_xid;
COMMIT;
