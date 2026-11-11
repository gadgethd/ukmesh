SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Keep the FK and fail-closed handling of real prefix removals. Row triggers
-- cannot fire for the empty FK DELETE that a public-node deletion generates.
-- The xid8 marker coalesces bulk/multi-statement edits in one transaction and
-- rolls back with the invalidation, including at savepoints.
ALTER TABLE public_visibility_state ADD COLUMN prefix_fence_xid XID8;

CREATE OR REPLACE FUNCTION bump_visibility_for_changed_prefixes()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_LEVEL = 'ROW' THEN
    IF TG_OP = 'UPDATE' AND
       ROW(OLD.node_id, OLD.network, OLD.prefix_size_bytes, OLD.prefix)
       IS NOT DISTINCT FROM
       ROW(NEW.node_id, NEW.network, NEW.prefix_size_bytes, NEW.prefix) THEN
      RETURN NULL;
    END IF;

    -- 056 owns node-trigger maintenance and queues node privacy transitions.
    -- A real FK removal also runs nested, but its parent is already absent:
    -- it must still fence, including a manually prefixed public identity.
    IF pg_trigger_depth() > 1 AND EXISTS (
      SELECT 1 FROM public.nodes WHERE node_id = COALESCE(NEW.node_id, OLD.node_id)
    ) THEN RETURN NULL; END IF;
  END IF;

  UPDATE public.public_visibility_state visibility
     SET generation = generation + 1, updated_at = NOW(),
         prefix_fence_xid = pg_current_xact_id()
   WHERE singleton = TRUE
     AND (prefix_fence_xid IS DISTINCT FROM pg_current_xact_id()
       -- A verified repair may have certified an earlier edit in this same
       -- transaction. Any later edit must invalidate that certificate again.
       OR EXISTS (SELECT 1 FROM public.packet_visibility_materialization_state materialized
         WHERE materialized.singleton = TRUE
           AND materialized.visibility_generation = visibility.generation));
  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS private_node_prefixes_visibility_insert ON private_node_prefixes;
DROP TRIGGER IF EXISTS private_node_prefixes_visibility_update ON private_node_prefixes;
DROP TRIGGER IF EXISTS private_node_prefixes_visibility_delete ON private_node_prefixes;
DROP TRIGGER IF EXISTS private_node_prefixes_visibility_truncate ON private_node_prefixes;
CREATE TRIGGER private_node_prefixes_visibility_rows
AFTER INSERT OR UPDATE OR DELETE ON private_node_prefixes
FOR EACH ROW EXECUTE FUNCTION bump_visibility_for_changed_prefixes();
-- PostgreSQL TRUNCATE has no row triggers. Retain conservative invalidation.
CREATE TRIGGER private_node_prefixes_visibility_truncate
AFTER TRUNCATE ON private_node_prefixes
FOR EACH STATEMENT EXECUTE FUNCTION bump_visibility_for_changed_prefixes();
