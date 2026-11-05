-- Run in one operator-controlled transaction. This restores migration 042's
-- behavior, including its conservative no-row statement invalidation.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
-- 134 re-attaches the prefix fence to core.private_node_prefixes after 113 moved
-- the registry, and 087 no longer creates any triggers. Any of these triggers
-- left in place makes the function drop below fail with SQLSTATE 2BP01, so drop
-- every historical name explicitly, on whichever relation currently owns the
-- registry. Nothing else depends on bump_visibility_for_changed_prefixes() (only
-- these triggers do), so an explicit list is enough and a bare CASCADE would
-- also remove anything unexpected that ever attached to it.
DO $fence_deps$
DECLARE
  registry regclass;
  fence_trigger text;
BEGIN
  registry := COALESCE(
    to_regclass('core.private_node_prefixes'),
    to_regclass('public.private_node_prefixes')
  );
  IF registry IS NULL THEN
    RETURN;
  END IF;
  FOREACH fence_trigger IN ARRAY ARRAY[
    -- 134's row fence on the post-move registry.
    'private_node_prefixes_visibility_insert_rows',
    'private_node_prefixes_visibility_update_rows',
    'private_node_prefixes_visibility_delete_rows',
    -- 074's row fence name, kept for a database that still has it.
    'private_node_prefixes_visibility_rows',
    -- 066's statement triggers, still present pre-move or after a reapply.
    'private_node_prefixes_visibility_insert',
    'private_node_prefixes_visibility_update',
    'private_node_prefixes_visibility_delete',
    'private_node_prefixes_visibility_truncate'
  ] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', fence_trigger, registry);
  END LOOP;
END
$fence_deps$;
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
