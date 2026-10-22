SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- FK cascades issue DELETE statements even when a public node has no private
-- prefixes. Statement-trigger depth does not identify that no-op reliably.
-- Transition tables do: an empty statement cannot change packet privacy.
-- Real direct/cascade changes still invalidate materialization, fail closed.
CREATE OR REPLACE FUNCTION bump_visibility_for_changed_prefixes()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  -- Node-trigger maintenance is covered by the asynchronous queue in 056.
  IF pg_trigger_depth() > 1 THEN RETURN NULL; END IF;

  IF TG_OP = 'INSERT' THEN
    IF NOT EXISTS (SELECT 1 FROM new_prefix_rows) THEN RETURN NULL; END IF;
  ELSIF TG_OP = 'DELETE' THEN
    IF NOT EXISTS (SELECT 1 FROM old_prefix_rows) THEN RETURN NULL; END IF;
  ELSIF TG_OP = 'UPDATE' THEN
    IF NOT EXISTS (SELECT 1 FROM new_prefix_rows) THEN RETURN NULL; END IF;
  END IF;
  -- TRUNCATE has no transition table; conservatively fence it, even if empty.
  UPDATE public_visibility_state
     SET generation = generation + 1, updated_at = NOW()
   WHERE singleton = TRUE;
  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS private_node_prefixes_visibility_generation ON private_node_prefixes;
CREATE TRIGGER private_node_prefixes_visibility_insert
AFTER INSERT ON private_node_prefixes
REFERENCING NEW TABLE AS new_prefix_rows
FOR EACH STATEMENT EXECUTE FUNCTION bump_visibility_for_changed_prefixes();
CREATE TRIGGER private_node_prefixes_visibility_update
AFTER UPDATE ON private_node_prefixes
REFERENCING NEW TABLE AS new_prefix_rows
FOR EACH STATEMENT EXECUTE FUNCTION bump_visibility_for_changed_prefixes();
CREATE TRIGGER private_node_prefixes_visibility_delete
AFTER DELETE ON private_node_prefixes
REFERENCING OLD TABLE AS old_prefix_rows
FOR EACH STATEMENT EXECUTE FUNCTION bump_visibility_for_changed_prefixes();
CREATE TRIGGER private_node_prefixes_visibility_truncate
AFTER TRUNCATE ON private_node_prefixes
FOR EACH STATEMENT EXECUTE FUNCTION bump_visibility_for_changed_prefixes();

-- Alias-only changes can carry a CURRENT materialization forward, but must
-- never certify an unrelated prefix-table change that was already fenced.
CREATE OR REPLACE FUNCTION bump_visibility_for_identity_table()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  previous_generation BIGINT;
BEGIN
  IF TG_TABLE_NAME = 'private_node_prefixes' AND pg_trigger_depth() > 1 THEN
    RETURN NULL;
  END IF;
  SELECT generation INTO STRICT previous_generation
    FROM public_visibility_state WHERE singleton = TRUE FOR UPDATE;
  UPDATE public_visibility_state
     SET generation = generation + 1, updated_at = NOW()
   WHERE singleton = TRUE;
  IF TG_TABLE_NAME = 'node_identity_aliases' THEN
    UPDATE packet_visibility_materialization_state
       SET visibility_generation = previous_generation + 1, updated_at = NOW()
     WHERE singleton = TRUE AND visibility_generation = previous_generation;
  END IF;
  RETURN NULL;
END;
$$;
