SET LOCAL statement_timeout = 0;

ALTER TABLE packets ADD COLUMN IF NOT EXISTS topic_prefix TEXT NOT NULL DEFAULT '';
ALTER TABLE packets ADD COLUMN IF NOT EXISTS iata TEXT;
ALTER TABLE packets ADD COLUMN IF NOT EXISTS is_private BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE packets ADD COLUMN IF NOT EXISTS visibility_ok BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE nodes ADD COLUMN IF NOT EXISTS last_rx_at TIMESTAMPTZ;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS last_status_at TIMESTAMPTZ;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS observer_iata TEXT;

CREATE TABLE IF NOT EXISTS private_node_prefixes (
  node_id           TEXT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
  network           TEXT NOT NULL,
  prefix_size_bytes SMALLINT NOT NULL CHECK (prefix_size_bytes BETWEEN 1 AND 3),
  prefix            TEXT NOT NULL,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (node_id, network, prefix_size_bytes)
);

CREATE INDEX IF NOT EXISTS private_node_prefixes_lookup_idx
  ON private_node_prefixes (network, prefix_size_bytes, prefix);

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
             FROM unnest(COALESCE(p.path_hashes, ARRAY[]::text[])) AS path_hash
             WHERE path_hash IS NULL
                OR length(path_hash) <> p.path_hash_size_bytes * 2
                OR path_hash !~ '^[0-9A-Fa-f]+$'
           )
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

INSERT INTO private_node_prefixes (node_id, network, prefix_size_bytes, prefix)
SELECT n.node_id, n.network, sizes.size_bytes, UPPER(LEFT(n.node_id, sizes.size_bytes * 2))
FROM nodes n
CROSS JOIN (VALUES (1), (2), (3)) AS sizes(size_bytes)
WHERE n.name LIKE '%🚫%'
ON CONFLICT (node_id, network, prefix_size_bytes) DO UPDATE SET
  prefix = EXCLUDED.prefix,
  updated_at = NOW();

UPDATE packets
   SET topic_prefix = split_part(topic, '/', 1),
       iata = NULLIF(UPPER(split_part(topic, '/', 2)), '')
 WHERE topic_prefix = '' OR iata IS NULL;

WITH latest_rx AS (
  SELECT DISTINCT ON (rx_node_id)
         rx_node_id, time, iata
  FROM packets
  WHERE rx_node_id IS NOT NULL AND rx_node_id <> ''
  ORDER BY rx_node_id, time DESC
)
UPDATE nodes n
   SET last_rx_at = r.time,
       observer_iata = COALESCE(r.iata, n.iata)
  FROM latest_rx r
 WHERE n.node_id = r.rx_node_id;

WITH latest_status AS (
  SELECT DISTINCT ON (node_id) node_id, time
  FROM node_status_samples
  ORDER BY node_id, time DESC
)
UPDATE nodes n
   SET last_status_at = s.time
  FROM latest_status s
 WHERE n.node_id = s.node_id;

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
        FROM unnest(COALESCE(p.path_hashes, ARRAY[]::text[])) AS path_hash
        WHERE path_hash IS NULL
           OR length(path_hash) <> p.path_hash_size_bytes * 2
           OR path_hash !~ '^[0-9A-Fa-f]+$'
      )
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
    );

DELETE FROM packet_daily_stats
WHERE day >= CURRENT_DATE - 30;

INSERT INTO packet_daily_stats (
  network, day, max_hop_count, max_hop_hash, max_hop_seen_at, updated_at
)
SELECT DISTINCT ON (network, time::date)
       network,
       time::date,
       hop_count,
       packet_hash,
       time,
       NOW()
FROM packets
WHERE visibility_ok IS TRUE
  AND hop_count IS NOT NULL
  AND time >= CURRENT_DATE - 30
ORDER BY network, time::date, hop_count DESC, time DESC;

CREATE INDEX IF NOT EXISTS packets_public_visibility_idx
  ON packets (network, time DESC)
  WHERE visibility_ok IS TRUE;
