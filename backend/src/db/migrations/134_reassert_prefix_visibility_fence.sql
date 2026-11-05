-- 134_reassert_prefix_visibility_fence.sql
-- Re-assert the row-aware prefix visibility fence against the post-move schema.
--
-- History that makes this necessary:
--   * 074 (renamed from 071) installed the row-level fence while both relations
--     still lived in public: it added public_visibility_state.prefix_fence_xid,
--     replaced bump_visibility_for_changed_prefixes() with the row body and put
--     private_node_prefixes_visibility_rows on public.private_node_prefixes.
--   * 087 replaced bump_visibility_for_changed_prefixes() with the statement
--     body from 066, which reads the new_prefix_rows/old_prefix_rows transition
--     tables. It did not recreate the triggers.
--   * 113 moved the prefix registry to core.private_node_prefixes; the ROW
--     trigger moved with it but still executes the statement body, so every
--     write to the registry now fails with
--     `relation "new_prefix_rows" does not exist`.
--   * 108/128 left read-only compatibility views at the old public names, so
--     074 can no longer be replayed against this schema either.
--
-- This migration therefore re-installs 074's semantics, but only against
-- core.private_node_prefixes and ops.public_visibility_state. It never reads or
-- writes the public compatibility views, so it applies whether or not they exist
-- and whether or not the pre-move relations are still in place.
--
-- Idempotent by construction: CREATE OR REPLACE FUNCTION, DROP TRIGGER IF EXISTS,
-- ADD COLUMN IF NOT EXISTS, and a backfill that stamps prefix_fence_xid and can
-- therefore only fire once. Safe to re-run in one transaction.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE OR REPLACE FUNCTION public.bump_visibility_for_changed_prefixes()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  IF TG_LEVEL = 'ROW' THEN
    IF TG_OP = 'UPDATE' AND
       OLD.node_id IS NOT DISTINCT FROM NEW.node_id AND
       OLD.network IS NOT DISTINCT FROM NEW.network AND
       OLD.prefix_size_bytes IS NOT DISTINCT FROM NEW.prefix_size_bytes AND
       OLD.prefix IS NOT DISTINCT FROM NEW.prefix THEN
      RETURN NULL;
    END IF;

    -- 056 owns node-trigger maintenance and queues node privacy transitions.
    -- A real FK removal also runs nested, but its parent is already absent:
    -- it must still fence, including a manually prefixed public identity.
    IF pg_trigger_depth() > 1 AND EXISTS (
      SELECT 1 FROM core.nodes WHERE node_id = COALESCE(NEW.node_id, OLD.node_id)
    ) THEN RETURN NULL; END IF;
  END IF;

  UPDATE ops.public_visibility_state visibility
     SET generation = generation + 1, updated_at = NOW(),
         prefix_fence_xid = pg_current_xact_id()
   WHERE singleton = TRUE
     AND (visibility.prefix_fence_xid IS DISTINCT FROM pg_current_xact_id()
       -- A verified repair may have certified an earlier edit in this same
       -- transaction. Any later edit must invalidate that certificate again.
       OR EXISTS (SELECT 1 FROM ops.packet_visibility_materialization_state materialized
         WHERE materialized.singleton = TRUE
           AND materialized.visibility_generation = visibility.generation));
  RETURN NULL;
END;
$function$;

DO $fence$
BEGIN
  -- Nothing to fence if the post-move relations are not present (for example a
  -- database that stopped before 087/113). Leave the function above installed;
  -- it is inert without triggers.
  IF to_regclass('core.private_node_prefixes') IS NULL
     OR to_regclass('ops.public_visibility_state') IS NULL THEN
    RAISE NOTICE '134: post-move privacy relations absent; fence re-assert skipped';
    RETURN;
  END IF;

  ALTER TABLE ops.public_visibility_state
    ADD COLUMN IF NOT EXISTS prefix_fence_xid xid8;

  -- Drop every historical name, including 039's statement trigger and the four
  -- statement triggers 066 created, so no duplicate or transition-table path
  -- can survive alongside the row fence.
  DROP TRIGGER IF EXISTS private_node_prefixes_visibility_generation ON core.private_node_prefixes;
  DROP TRIGGER IF EXISTS private_node_prefixes_visibility_insert ON core.private_node_prefixes;
  DROP TRIGGER IF EXISTS private_node_prefixes_visibility_update ON core.private_node_prefixes;
  DROP TRIGGER IF EXISTS private_node_prefixes_visibility_delete ON core.private_node_prefixes;
  DROP TRIGGER IF EXISTS private_node_prefixes_visibility_rows ON core.private_node_prefixes;
  DROP TRIGGER IF EXISTS private_node_prefixes_visibility_insert_rows ON core.private_node_prefixes;
  DROP TRIGGER IF EXISTS private_node_prefixes_visibility_update_rows ON core.private_node_prefixes;
  DROP TRIGGER IF EXISTS private_node_prefixes_visibility_delete_rows ON core.private_node_prefixes;
  DROP TRIGGER IF EXISTS private_node_prefixes_visibility_truncate ON core.private_node_prefixes;

  CREATE TRIGGER private_node_prefixes_visibility_insert_rows
  AFTER INSERT ON core.private_node_prefixes
  FOR EACH ROW EXECUTE FUNCTION public.bump_visibility_for_changed_prefixes();

  CREATE TRIGGER private_node_prefixes_visibility_update_rows
  AFTER UPDATE ON core.private_node_prefixes
  FOR EACH ROW EXECUTE FUNCTION public.bump_visibility_for_changed_prefixes();

  CREATE TRIGGER private_node_prefixes_visibility_delete_rows
  AFTER DELETE ON core.private_node_prefixes
  FOR EACH ROW EXECUTE FUNCTION public.bump_visibility_for_changed_prefixes();

  -- PostgreSQL TRUNCATE has no row triggers. Retain conservative invalidation.
  CREATE TRIGGER private_node_prefixes_visibility_truncate
  AFTER TRUNCATE ON core.private_node_prefixes
  FOR EACH STATEMENT EXECUTE FUNCTION public.bump_visibility_for_changed_prefixes();

  -- Guarded backfill. While 087's statement body was attached to a row trigger,
  -- registry writes either failed or (where a statement path still existed)
  -- advanced the singleton generation without stamping prefix_fence_xid. If any
  -- prefixes exist and the materialization state is still certified at the
  -- current generation, invalidate that certificate once and stamp the marker.
  -- After the stamp this predicate is false, so a replay cannot bump again.
  IF to_regclass('ops.packet_visibility_materialization_state') IS NOT NULL THEN
    UPDATE ops.public_visibility_state state
       SET generation = state.generation + 1,
           updated_at = NOW(),
           prefix_fence_xid = pg_current_xact_id()
     WHERE state.singleton = TRUE
       AND state.prefix_fence_xid IS NULL
       AND EXISTS (SELECT 1 FROM core.private_node_prefixes)
       AND EXISTS (
         SELECT 1 FROM ops.packet_visibility_materialization_state materialized
          WHERE materialized.singleton = TRUE
            AND materialized.visibility_generation = state.generation
       );
  END IF;
END
$fence$;
