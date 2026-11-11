-- Restore 066 without changing packet flags, queue entries or generations.
-- Runs in one operator-controlled transaction.
--
-- 134 re-attaches a row fence to core.private_node_prefixes after 113 moved the
-- registry, and 074 itself attaches its row fence to the pre-move relation. Any
-- of those triggers left in place makes both the function replacement and the
-- prefix_fence_xid column drop fail with SQLSTATE 2BP01, so drop every
-- historical name explicitly on whichever relation currently owns the registry
-- before restoring 066's statement fence on that same relation.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
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
    -- 074's row fence, on the pre-move registry.
    'private_node_prefixes_visibility_rows',
    -- 134's row fence on the post-move registry.
    'private_node_prefixes_visibility_insert_rows',
    'private_node_prefixes_visibility_update_rows',
    'private_node_prefixes_visibility_delete_rows',
    -- 066's statement triggers, still present pre-move or after a re-apply.
    'private_node_prefixes_visibility_insert',
    'private_node_prefixes_visibility_update',
    'private_node_prefixes_visibility_delete',
    'private_node_prefixes_visibility_truncate'
  ] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', fence_trigger, registry);
  END LOOP;
END
$fence_deps$;
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
DO $fence_triggers$
DECLARE
  registry regclass;
BEGIN
  registry := COALESCE(
    to_regclass('core.private_node_prefixes'),
    to_regclass('public.private_node_prefixes')
  );
  IF registry IS NULL THEN
    RETURN;
  END IF;
  EXECUTE format('CREATE TRIGGER private_node_prefixes_visibility_insert '
    'AFTER INSERT ON %s REFERENCING NEW TABLE AS new_prefix_rows '
    'FOR EACH STATEMENT EXECUTE FUNCTION bump_visibility_for_changed_prefixes()', registry);
  EXECUTE format('CREATE TRIGGER private_node_prefixes_visibility_update '
    'AFTER UPDATE ON %s REFERENCING NEW TABLE AS new_prefix_rows '
    'FOR EACH STATEMENT EXECUTE FUNCTION bump_visibility_for_changed_prefixes()', registry);
  EXECUTE format('CREATE TRIGGER private_node_prefixes_visibility_delete '
    'AFTER DELETE ON %s REFERENCING OLD TABLE AS old_prefix_rows '
    'FOR EACH STATEMENT EXECUTE FUNCTION bump_visibility_for_changed_prefixes()', registry);
  EXECUTE format('CREATE TRIGGER private_node_prefixes_visibility_truncate '
    'AFTER TRUNCATE ON %s '
    'FOR EACH STATEMENT EXECUTE FUNCTION bump_visibility_for_changed_prefixes()', registry);
END
$fence_triggers$;
-- Drop 074's marker column only where nothing else has taken a dependency on
-- it. After the moves, the public compat view exposes the column, and dropping
-- it would either fail or silently drop the view; the statement fence does not
-- read it, so leaving it in place is the safe rollback on a post-move database.
DO $fence_column$
DECLARE
  dependents INTEGER;
BEGIN
  SELECT count(*) INTO dependents
    FROM pg_depend d
    JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
   WHERE d.refclassid = 'pg_class'::regclass
     AND a.attname = 'prefix_fence_xid'
     AND d.classid <> 'pg_class'::regclass;
  IF dependents = 0 THEN
    ALTER TABLE public_visibility_state DROP COLUMN prefix_fence_xid;
  END IF;
END
$fence_column$;
COMMIT;
