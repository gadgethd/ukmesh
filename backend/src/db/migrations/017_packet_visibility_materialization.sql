ALTER TABLE packets
  ADD COLUMN IF NOT EXISTS is_private BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE packets
  ADD COLUMN IF NOT EXISTS visibility_ok BOOLEAN NOT NULL DEFAULT TRUE;

CREATE INDEX IF NOT EXISTS packets_visible_network_time_idx
  ON packets (network, time DESC)
  WHERE visibility_ok = TRUE;

CREATE TABLE IF NOT EXISTS private_node_prefixes (
  node_id           TEXT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
  network           TEXT NOT NULL,
  prefix_size_bytes SMALLINT NOT NULL CHECK (prefix_size_bytes BETWEEN 1 AND 3),
  prefix            TEXT NOT NULL,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (node_id, network, prefix_size_bytes)
);

CREATE INDEX IF NOT EXISTS private_node_prefix_lookup_idx
  ON private_node_prefixes (network, prefix_size_bytes, prefix);

INSERT INTO private_node_prefixes (node_id, network, prefix_size_bytes, prefix)
SELECT node_id, network, prefix_size_bytes, UPPER(LEFT(node_id, prefix_size_bytes * 2))
FROM nodes
CROSS JOIN (VALUES (1), (2), (3)) AS lengths(prefix_size_bytes)
WHERE name LIKE '%🚫%'
ON CONFLICT (node_id, network, prefix_size_bytes) DO UPDATE SET
  prefix = EXCLUDED.prefix,
  updated_at = NOW();

-- One-time classification for historical rows. The UPDATE only rewrites
-- packets that are actually private; public rows retain the fast defaults.
UPDATE packets packet
SET is_private = TRUE,
    visibility_ok = FALSE
WHERE EXISTS (
  SELECT 1
  FROM nodes private_node
  WHERE private_node.name LIKE '%🚫%'
    AND private_node.node_id IN (packet.rx_node_id, packet.src_node_id)
    AND (
      private_node.network = packet.network
      OR (
        private_node.network IN ('ukmesh', 'northeast', 'teesside')
        AND packet.network IN ('ukmesh', 'northeast', 'teesside')
      )
    )
)
OR EXISTS (
  SELECT 1
  FROM unnest(COALESCE(packet.path_hashes, ARRAY[]::text[])) AS path_hash(hash)
  JOIN private_node_prefixes private_prefix
    ON private_prefix.prefix_size_bytes = packet.path_hash_size_bytes
   AND private_prefix.prefix = UPPER(path_hash.hash)
  WHERE private_prefix.network = packet.network
     OR (
       private_prefix.network IN ('ukmesh', 'northeast', 'teesside')
       AND packet.network IN ('ukmesh', 'northeast', 'teesside')
     )
);
