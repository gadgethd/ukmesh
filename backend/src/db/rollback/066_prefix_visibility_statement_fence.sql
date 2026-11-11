-- Run in one operator-controlled transaction. This restores migration 042's
-- behavior, including its conservative no-row statement invalidation.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
DROP TRIGGER IF EXISTS private_node_prefixes_visibility_insert ON private_node_prefixes;
DROP TRIGGER IF EXISTS private_node_prefixes_visibility_update ON private_node_prefixes;
DROP TRIGGER IF EXISTS private_node_prefixes_visibility_delete ON private_node_prefixes;
DROP TRIGGER IF EXISTS private_node_prefixes_visibility_truncate ON private_node_prefixes;
DROP FUNCTION IF EXISTS bump_visibility_for_changed_prefixes();

CREATE OR REPLACE FUNCTION bump_visibility_for_identity_table()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  next_generation BIGINT;
BEGIN
  IF TG_TABLE_NAME = 'private_node_prefixes' AND pg_trigger_depth() > 1 THEN
    RETURN NULL;
  END IF;
  UPDATE public_visibility_state
     SET generation = generation + 1, updated_at = NOW()
   WHERE singleton = TRUE
   RETURNING generation INTO next_generation;
  IF TG_TABLE_NAME = 'node_identity_aliases' THEN
    INSERT INTO packet_visibility_materialization_state
      (singleton, visibility_generation, updated_at)
    VALUES (TRUE, next_generation, NOW())
    ON CONFLICT (singleton) DO UPDATE SET
      visibility_generation = EXCLUDED.visibility_generation,
      updated_at = EXCLUDED.updated_at;
  END IF;
  RETURN NULL;
END;
$$;
CREATE TRIGGER private_node_prefixes_visibility_generation
AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON private_node_prefixes
FOR EACH STATEMENT EXECUTE FUNCTION bump_visibility_for_identity_table();
COMMIT;
