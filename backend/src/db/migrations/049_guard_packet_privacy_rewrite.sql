SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- 046 restored the stored-packet privacy rewrite inside
-- sync_private_node_prefixes(), but fires it on EVERY node name/network
-- change. The rewrite predicate (rx/src OR network+path-prefix) defeats index
-- use on the ~32M-row packets hypertable, so a routine public->public rename
-- (common in the MQTT status stream) triggers a full-table scan+rewrite that
-- holds the transaction open for many minutes and blocks the whole ingest
-- pipeline (observed 2026-08-12: zero packet writes for 15+ min, every insert
-- failing behind a single rewrite).
--
-- Privacy classification of stored packets only changes when a node's privacy
-- state transitions (name gains or loses the 🚫 marker). For public->public
-- changes there are no private prefixes to apply, so the rewrite is a no-op —
-- skip it. Prefix bookkeeping (cheap, 3 rows) still runs on every change.
CREATE OR REPLACE FUNCTION sync_private_node_prefixes()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  changed_node_id TEXT := COALESCE(NEW.node_id, OLD.node_id);
  changed_network TEXT := COALESCE(NEW.network, OLD.network, 'ukmesh');
  old_private BOOLEAN := FALSE;
  new_private BOOLEAN := FALSE;
BEGIN
  IF TG_OP <> 'INSERT' THEN
    old_private := COALESCE(OLD.name, '') LIKE '%🚫%';
  END IF;
  IF TG_OP <> 'DELETE' THEN
    new_private := COALESCE(NEW.name, '') LIKE '%🚫%';
  END IF;

  IF TG_OP = 'UPDATE'
     AND OLD.name IS NOT DISTINCT FROM NEW.name
     AND OLD.network IS NOT DISTINCT FROM NEW.network THEN
    RETURN NEW;
  END IF;

  DELETE FROM private_node_prefixes WHERE node_id = changed_node_id;

  IF TG_OP <> 'DELETE' AND COALESCE(NEW.name, '') LIKE '%🚫%' THEN
    INSERT INTO private_node_prefixes (node_id, network, prefix_size_bytes, prefix)
    VALUES
      (NEW.node_id, COALESCE(NEW.network, 'ukmesh'), 1, UPPER(LEFT(NEW.node_id, 2))),
      (NEW.node_id, COALESCE(NEW.network, 'ukmesh'), 2, UPPER(LEFT(NEW.node_id, 4))),
      (NEW.node_id, COALESCE(NEW.network, 'ukmesh'), 3, UPPER(LEFT(NEW.node_id, 6)))
    ON CONFLICT (node_id, network, prefix_size_bytes) DO UPDATE SET
      prefix = EXCLUDED.prefix,
      updated_at = NOW();
  END IF;

  -- A privacy-name or network change must also rematerialize packets already
  -- stored for this participant. Restrict the rewrite to rows that can mention
  -- the changed identity, then recompute against the complete prefix index.
  -- Stored-packet privacy only changes across a privacy transition (name
  -- gains/loses 🚫). Public->public renames and network changes have no
  -- private prefixes to apply and no stored packet changes state; running the
  -- rewrite then is a full-scan no-op on the 32M-row hypertable that blocks
  -- ingest for minutes (observed 2026-08-12).
  IF old_private IS DISTINCT FROM new_private THEN
  UPDATE packets p
     SET is_private = EXISTS (
           SELECT 1
           FROM private_node_prefixes pp
           WHERE (
               pp.network = p.network
               OR (
                 pp.network IN ('ukmesh', 'northeast', 'teesside')
                 AND p.network IN ('ukmesh', 'northeast', 'teesside')
               )
             )
             AND (
               pp.node_id IN (p.rx_node_id, p.src_node_id)
               OR (
                 p.path_hash_size_bytes = pp.prefix_size_bytes
                 AND EXISTS (
                   SELECT 1
                   FROM unnest(COALESCE(p.path_hashes, ARRAY[]::text[])) AS packet_prefix
                   WHERE UPPER(packet_prefix) = pp.prefix
                 )
               )
             )
         ),
         visibility_ok = (
           (COALESCE(cardinality(p.path_hashes), 0) = 0 OR p.path_hash_size_bytes BETWEEN 1 AND 3)
           AND NOT EXISTS (
             SELECT 1
             FROM private_node_prefixes pp
             WHERE (
                 pp.network = p.network
                 OR (
                   pp.network IN ('ukmesh', 'northeast', 'teesside')
                   AND p.network IN ('ukmesh', 'northeast', 'teesside')
                 )
               )
               AND (
                 pp.node_id IN (p.rx_node_id, p.src_node_id)
                 OR (
                   p.path_hash_size_bytes = pp.prefix_size_bytes
                   AND EXISTS (
                     SELECT 1
                     FROM unnest(COALESCE(p.path_hashes, ARRAY[]::text[])) AS packet_prefix
                     WHERE UPPER(packet_prefix) = pp.prefix
                   )
                 )
               )
           )
         )
   WHERE (
       p.rx_node_id = changed_node_id
       OR p.src_node_id = changed_node_id
       OR (
         p.network = changed_network
         OR (
           p.network IN ('ukmesh', 'northeast', 'teesside')
           AND changed_network IN ('ukmesh', 'northeast', 'teesside')
         )
       )
       AND EXISTS (
         SELECT 1
         FROM unnest(COALESCE(p.path_hashes, ARRAY[]::text[])) AS packet_prefix
         WHERE UPPER(packet_prefix) IN (
           UPPER(LEFT(changed_node_id, 2)),
           UPPER(LEFT(changed_node_id, 4)),
           UPPER(LEFT(changed_node_id, 6))
         )
       )
     );
  END IF;

  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;

-- Rebuild the trigger so existing sessions pick up the new function body.
DROP TRIGGER IF EXISTS nodes_private_prefix_materialization ON nodes;
CREATE TRIGGER nodes_private_prefix_materialization
AFTER INSERT OR DELETE OR UPDATE ON nodes
FOR EACH ROW
EXECUTE FUNCTION sync_private_node_prefixes();
