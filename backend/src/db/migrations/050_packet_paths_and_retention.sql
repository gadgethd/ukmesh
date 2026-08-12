-- 050: dedicated long-retention raw path store + scheduled retention jobs
--
-- Data policy (Ben 2026-08-12): past 30 days the only thing worth keeping is
-- packet paths. Message content dies at 30 days; path metadata lives forever.
--
-- 1. packet_paths: content-stripped path rows written at ingest (dual-write in
--    packetBatch), backfilled here from existing path-bearing packets.
-- 2. Retention jobs: DATA_LIFECYCLE_POLICIES declared retentions but no
--    timescaledb jobs were ever scheduled; align the DB with the policies.

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. packet_paths hypertable (no retention policy — kept forever)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS packet_paths (
  time                 timestamptz NOT NULL,
  packet_hash          text        NOT NULL,
  rx_node_id           text,
  src_node_id          text,
  topic                text        NOT NULL,
  topic_prefix         text        NOT NULL DEFAULT '',
  route_type           integer,
  hop_count            integer,
  rssi                 double precision,
  snr                  double precision,
  path_hashes          text[],
  path_hash_size_bytes integer,
  is_private           boolean     NOT NULL DEFAULT false,
  visibility_ok        boolean     NOT NULL DEFAULT true,
  network              text        NOT NULL,
  observation_id       uuid,
  PRIMARY KEY (time, packet_hash, rx_node_id, network)
);

SELECT create_hypertable(
  'packet_paths',
  'time',
  chunk_time_interval => INTERVAL '7 days',
  if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_packet_paths_network_time
  ON packet_paths (network, time DESC);

CREATE INDEX IF NOT EXISTS idx_packet_paths_ml
  ON packet_paths (time, packet_hash, network, COALESCE(rx_node_id, ''), topic, path_hash_size_bytes)
  WHERE path_hash_size_bytes > 1 AND path_hashes IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 2. Backfill: every existing path-bearing packet, content-stripped
-- ---------------------------------------------------------------------------
INSERT INTO packet_paths (
  time, packet_hash, rx_node_id, src_node_id, topic, topic_prefix, route_type,
  hop_count, rssi, snr, path_hashes, path_hash_size_bytes, is_private,
  visibility_ok, network, observation_id
)
SELECT
  time, packet_hash, rx_node_id, src_node_id, topic, topic_prefix, route_type,
  hop_count, rssi, snr, path_hashes, path_hash_size_bytes, is_private,
  visibility_ok, network, observation_id
FROM packets
WHERE path_hashes IS NOT NULL AND path_hash_size_bytes > 1
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- 3. Retention jobs aligned with DATA_LIFECYCLE_POLICIES
--    (packets 30d per the 2026-08-12 policy; node samples per policy)
-- ---------------------------------------------------------------------------
SELECT remove_retention_policy('packets', if_exists => TRUE);
SELECT add_retention_policy('packets', INTERVAL '30 days', if_not_exists => TRUE);

SELECT remove_retention_policy('node_status_samples', if_exists => TRUE);
SELECT add_retention_policy('node_status_samples', INTERVAL '180 days', if_not_exists => TRUE);

SELECT remove_retention_policy('node_neighbor_samples', if_exists => TRUE);
SELECT add_retention_policy('node_neighbor_samples', INTERVAL '7 days', if_not_exists => TRUE);

COMMIT;
