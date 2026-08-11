SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- The 042 fence design assumes sync_private_node_prefixes() rewrites packets
-- whenever a node's privacy changes ("nodes_private_prefix_materialization
-- sorts before this trigger and has already updated every affected packet").
-- Migration 026's rewrite of that function dropped the packet UPDATE that 016
-- had, and no later migration restored it. Combined with 6b0aa34 removing the
-- query-time prefix probe, a node that flips private (name gains 🚫) leaves
-- every packet already stored for it publicly visible, and packets inserted
-- outside packetBatch (backfills, legacy rows, direct SQL) are never
-- classified at all.
--
-- This migration restores the DB-level guarantee:
--   1. sync_private_node_prefixes() again rematerializes stored packets when
--      a node's privacy/network changes (the 042 contract).
--   2. A BEFORE INSERT trigger classifies every packet against the current
--      private_node_prefixes index, so no insert path can bypass privacy.
CREATE OR REPLACE FUNCTION sync_private_node_prefixes()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  changed_node_id TEXT := COALESCE(NEW.node_id, OLD.node_id);
  changed_network TEXT := COALESCE(NEW.network, OLD.network, 'ukmesh');
BEGIN
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

  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS nodes_private_prefix_materialization ON nodes;
CREATE TRIGGER nodes_private_prefix_materialization
AFTER INSERT OR UPDATE OR DELETE ON nodes
FOR EACH ROW
EXECUTE FUNCTION sync_private_node_prefixes();

-- Classify every packet at insert time against the current prefix index. This
-- mirrors the packetBatch TS-side privatePrefixCache classification so that no
-- insert path (direct SQL, backfills, legacy rows) can bypass privacy. The
-- function is deliberately simple and index-bound: private_node_prefixes is
-- tiny (only 🚫-named nodes) and the lookup key is the packets PK columns.
CREATE OR REPLACE FUNCTION classify_packet_privacy()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  matches_private BOOLEAN;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM private_node_prefixes pp
    WHERE (
        pp.network = NEW.network
        OR (
          pp.network IN ('ukmesh', 'northeast', 'teesside')
          AND NEW.network IN ('ukmesh', 'northeast', 'teesside')
        )
      )
      AND (
        pp.node_id IN (NEW.rx_node_id, NEW.src_node_id)
        OR (
          NEW.path_hash_size_bytes = pp.prefix_size_bytes
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(NEW.path_hashes, ARRAY[]::text[])) AS packet_prefix
            WHERE UPPER(packet_prefix) = pp.prefix
          )
        )
      )
  ) INTO matches_private;

  NEW.is_private := matches_private;
  NEW.visibility_ok := (
    (COALESCE(cardinality(NEW.path_hashes), 0) = 0 OR NEW.path_hash_size_bytes BETWEEN 1 AND 3)
    AND NOT EXISTS (
      SELECT 1
      FROM unnest(COALESCE(NEW.path_hashes, ARRAY[]::text[])) AS path_hash
      WHERE path_hash IS NULL
         OR length(path_hash) <> NEW.path_hash_size_bytes * 2
         OR path_hash !~ '^[0-9A-Fa-f]+$'
    )
    AND matches_private IS NOT TRUE
  );
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS packets_classify_privacy ON packets;
CREATE TRIGGER packets_classify_privacy
BEFORE INSERT ON packets
FOR EACH ROW
EXECUTE FUNCTION classify_packet_privacy();
