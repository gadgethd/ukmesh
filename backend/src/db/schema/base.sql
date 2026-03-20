-- MeshCore Analytics — Database Schema
-- TimescaleDB, no automatic data retention (all data kept indefinitely)

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ─── Persistent tables (no retention) ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS nodes (
  node_id          TEXT PRIMARY KEY,  -- Ed25519 public key hex
  name             TEXT,
  lat              DOUBLE PRECISION,
  lon              DOUBLE PRECISION,
  iata             TEXT,              -- Observer location code from topic
  role             INTEGER,           -- 1=Repeater, 2=RoomServer, 3=Companion
  last_seen        TIMESTAMPTZ DEFAULT NOW(),
  is_online        BOOLEAN DEFAULT FALSE,
  hardware_model   TEXT,
  firmware_version TEXT,
  public_key       TEXT,              -- Same as node_id, kept for clarity
  created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Tracks which networks each node has been heard on (maintained by insertPacket).
-- Used for cross-network node visibility (e.g. showing ukmesh nodes on teesside site).
-- Replaces expensive correlated EXISTS subquery against the packets hypertable.
CREATE TABLE IF NOT EXISTS node_network_sightings (
  node_id       TEXT NOT NULL,
  network       TEXT NOT NULL,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (node_id, network)
);

CREATE TABLE IF NOT EXISTS planned_nodes (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  owner_pubkey TEXT NOT NULL,
  name         TEXT NOT NULL,
  lat          DOUBLE PRECISION NOT NULL,
  lon          DOUBLE PRECISION NOT NULL,
  height_m     DOUBLE PRECISION DEFAULT 10,
  notes        TEXT,
  created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS observers (
  public_key     TEXT PRIMARY KEY,
  name           TEXT NOT NULL,
  location       TEXT,
  registered_at  TIMESTAMPTZ DEFAULT NOW(),
  is_active      BOOLEAN DEFAULT TRUE
);

-- ─── Migrate existing nodes table if columns are missing ─────────────────
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS iata TEXT;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS role INTEGER;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS advert_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS elevation_m DOUBLE PRECISION;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS network TEXT NOT NULL DEFAULT 'teesside';
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS last_predicted_online_at TIMESTAMPTZ;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS last_path_evidence_at TIMESTAMPTZ;

-- ─── Packets hypertable (no retention — data kept indefinitely) ──────────

CREATE TABLE IF NOT EXISTS packets (
  time          TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
  packet_hash   TEXT             NOT NULL,
  rx_node_id    TEXT,
  src_node_id   TEXT,
  topic         TEXT             NOT NULL,
  packet_type   INTEGER,
  route_type    INTEGER,
  hop_count     INTEGER,
  rssi          DOUBLE PRECISION,
  snr           DOUBLE PRECISION,
  payload       JSONB,
  raw_hex       TEXT
);

SELECT create_hypertable('packets', 'time', if_not_exists => TRUE);

-- Remove any existing retention policy so packets are kept indefinitely.
DO $$
DECLARE
  _job_id INTEGER;
BEGIN
  SELECT job_id INTO _job_id
  FROM timescaledb_information.jobs
  WHERE proc_name = 'policy_retention'
    AND config->>'hypertable_id' = (
      SELECT id::text FROM _timescaledb_catalog.hypertable WHERE table_name = 'packets'
    );

  IF _job_id IS NOT NULL THEN
    PERFORM remove_retention_policy('packets');
  END IF;
END $$;

ALTER TABLE packets ADD COLUMN IF NOT EXISTS advert_count INTEGER;
ALTER TABLE packets ADD COLUMN IF NOT EXISTS path_hashes TEXT[];
ALTER TABLE packets ADD COLUMN IF NOT EXISTS path_hash_size_bytes INTEGER;
ALTER TABLE packets ADD COLUMN IF NOT EXISTS network      TEXT NOT NULL DEFAULT 'teesside';

-- Legacy whole-table backfills were previously run here on every app startup:
--   * packets.path_hash_size_bytes
--   * packets.network
--   * nodes.network
-- On a large production database they can exceed the app statement timeout and
-- prevent the backend from booting at all. Leave startup schema init idempotent
-- and cheap; run any full historical backfill as a one-off maintenance task.

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS packets_hash_idx   ON packets (packet_hash, time DESC);
CREATE INDEX IF NOT EXISTS packets_rx_idx     ON packets (rx_node_id, time DESC);
CREATE INDEX IF NOT EXISTS packets_src_idx    ON packets (src_node_id, time DESC);
CREATE INDEX IF NOT EXISTS packets_network_time_idx ON packets (network, time DESC);
CREATE INDEX IF NOT EXISTS packets_path_hashes_idx ON packets USING GIN (path_hashes) WHERE path_hashes IS NOT NULL;

-- Performance optimization indexes
CREATE INDEX IF NOT EXISTS idx_nodes_last_seen ON nodes(last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_packets_time_hash ON packets(time DESC, packet_hash);
CREATE INDEX IF NOT EXISTS idx_nodes_network_last_seen ON nodes(network, last_seen DESC) WHERE is_online = TRUE;

-- ─── Observer / repeater status telemetry samples ───────────────────────────

CREATE TABLE IF NOT EXISTS node_status_samples (
  time                 TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
  node_id              TEXT             NOT NULL,
  network              TEXT             NOT NULL DEFAULT 'teesside',
  battery_mv           INTEGER,
  uptime_secs          BIGINT,
  tx_air_secs          BIGINT,
  rx_air_secs          BIGINT,
  channel_utilization  DOUBLE PRECISION,
  air_util_tx          DOUBLE PRECISION,
  stats                JSONB
);

SELECT create_hypertable('node_status_samples', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS node_status_samples_node_time_idx
  ON node_status_samples (node_id, time DESC);
CREATE INDEX IF NOT EXISTS node_status_samples_network_time_idx
  ON node_status_samples (network, time DESC);

-- ─── Cached beta-path history (pre-aggregated purple segments) ───────────────

CREATE TABLE IF NOT EXISTS path_history_cache (
  scope                  TEXT PRIMARY KEY,
  window_start           TIMESTAMPTZ NOT NULL,
  updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  packet_count           INTEGER NOT NULL DEFAULT 0,
  resolved_packet_count  INTEGER NOT NULL DEFAULT 0,
  segment_counts         JSONB NOT NULL DEFAULT '[]'::jsonb
);

-- ─── Coverage polygons (one row per node, recalculated on position change) ───

-- ─── Observed + ITM-validated RF links between nodes ─────────────────────────
-- Populated by the viewshed worker as real packets with path data arrive.
-- node_a_id < node_b_id (sorted) so each pair has exactly one row.

CREATE TABLE IF NOT EXISTS node_links (
  node_a_id        TEXT        NOT NULL,
  node_b_id        TEXT        NOT NULL,
  observed_count   INTEGER     NOT NULL DEFAULT 1,
  last_observed    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  itm_path_loss_db DOUBLE PRECISION,
  itm_viable       BOOLEAN,
  itm_computed_at  TIMESTAMPTZ,
  count_a_to_b     INTEGER     NOT NULL DEFAULT 0,
  count_b_to_a     INTEGER     NOT NULL DEFAULT 0,
  PRIMARY KEY (node_a_id, node_b_id)
);
CREATE INDEX IF NOT EXISTS node_links_b_idx ON node_links(node_b_id);
ALTER TABLE node_links ADD COLUMN IF NOT EXISTS count_a_to_b  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE node_links ADD COLUMN IF NOT EXISTS count_b_to_a  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE node_links ADD COLUMN IF NOT EXISTS force_viable   BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE node_links ADD COLUMN IF NOT EXISTS multibyte_observed_count INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS node_link_radio_reports (
  node_a_id         TEXT        NOT NULL,
  node_b_id         TEXT        NOT NULL,
  reporter_node_id  TEXT        NOT NULL,
  peer_node_id      TEXT        NOT NULL,
  last_snr_db       DOUBLE PRECISION,
  best_snr_db       DOUBLE PRECISION,
  last_seen         TIMESTAMPTZ NOT NULL,
  sample_count      INTEGER     NOT NULL DEFAULT 1,
  PRIMARY KEY (node_a_id, node_b_id, reporter_node_id)
);
CREATE INDEX IF NOT EXISTS node_link_radio_reports_pair_idx
  ON node_link_radio_reports (node_a_id, node_b_id);
CREATE INDEX IF NOT EXISTS node_link_radio_reports_last_seen_idx
  ON node_link_radio_reports (last_seen DESC);

-- ─── Coverage polygons (one row per node, recalculated on position change) ───

CREATE TABLE IF NOT EXISTS node_coverage (
  node_id          TEXT PRIMARY KEY,
  geom             JSONB NOT NULL,            -- GeoJSON Polygon or MultiPolygon
  strength_geoms   JSONB,
  antenna_height_m DOUBLE PRECISION DEFAULT 10,
  radius_m         DOUBLE PRECISION DEFAULT 30000,
  model_version    INTEGER NOT NULL DEFAULT 1,
  calculated_at    TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE node_coverage ADD COLUMN IF NOT EXISTS strength_geoms JSONB;
ALTER TABLE node_coverage ADD COLUMN IF NOT EXISTS model_version INTEGER NOT NULL DEFAULT 1;

-- ─── Learned path priors from historical packets ─────────────────────────────

CREATE TABLE IF NOT EXISTS path_prefix_priors (
  network         TEXT        NOT NULL,
  prefix          TEXT        NOT NULL,
  receiver_region TEXT        NOT NULL,
  prev_prefix     TEXT,
  node_id         TEXT        NOT NULL,
  count           INTEGER     NOT NULL,
  probability     DOUBLE PRECISION NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (network, prefix, receiver_region, prev_prefix, node_id)
);
CREATE INDEX IF NOT EXISTS path_prefix_priors_lookup_idx
  ON path_prefix_priors(network, receiver_region, prefix);

CREATE TABLE IF NOT EXISTS path_transition_priors (
  network         TEXT        NOT NULL,
  from_node_id    TEXT        NOT NULL,
  to_node_id      TEXT        NOT NULL,
  receiver_region TEXT        NOT NULL,
  count           INTEGER     NOT NULL,
  probability     DOUBLE PRECISION NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (network, from_node_id, to_node_id, receiver_region)
);
CREATE INDEX IF NOT EXISTS path_transition_priors_lookup_idx
  ON path_transition_priors(network, receiver_region, from_node_id);

CREATE TABLE IF NOT EXISTS path_edge_priors (
  network             TEXT        NOT NULL,
  from_node_id        TEXT        NOT NULL,
  to_node_id          TEXT        NOT NULL,
  receiver_region     TEXT        NOT NULL,
  hour_bucket         SMALLINT    NOT NULL,
  observed_count      INTEGER     NOT NULL,
  expected_count      INTEGER     NOT NULL,
  missing_count       INTEGER     NOT NULL,
  directional_support DOUBLE PRECISION NOT NULL,
  recency_score       DOUBLE PRECISION NOT NULL,
  reliability         DOUBLE PRECISION NOT NULL,
  itm_path_loss_db    DOUBLE PRECISION,
  score               DOUBLE PRECISION NOT NULL,
  consistency_penalty DOUBLE PRECISION NOT NULL DEFAULT 0,
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (network, receiver_region, hour_bucket, from_node_id, to_node_id)
);
CREATE INDEX IF NOT EXISTS path_edge_priors_lookup_idx
  ON path_edge_priors(network, receiver_region, hour_bucket, from_node_id);

CREATE TABLE IF NOT EXISTS path_motif_priors (
  network         TEXT        NOT NULL,
  receiver_region TEXT        NOT NULL,
  hour_bucket     SMALLINT    NOT NULL,
  motif_len       SMALLINT    NOT NULL,
  node_ids        TEXT        NOT NULL,
  count           INTEGER     NOT NULL,
  probability     DOUBLE PRECISION NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (network, receiver_region, hour_bucket, motif_len, node_ids)
);
CREATE INDEX IF NOT EXISTS path_motif_priors_lookup_idx
  ON path_motif_priors(network, receiver_region, hour_bucket, motif_len);

CREATE TABLE IF NOT EXISTS path_model_calibration (
  network               TEXT PRIMARY KEY,
  evaluated_packets     INTEGER NOT NULL DEFAULT 0,
  top1_accuracy         DOUBLE PRECISION NOT NULL DEFAULT 0,
  mean_pred_confidence  DOUBLE PRECISION NOT NULL DEFAULT 0,
  confidence_scale      DOUBLE PRECISION NOT NULL DEFAULT 1,
  confidence_bias       DOUBLE PRECISION NOT NULL DEFAULT 0,
  recommended_threshold DOUBLE PRECISION NOT NULL DEFAULT 0.5,
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE path_model_calibration ADD COLUMN IF NOT EXISTS confidence_bias DOUBLE PRECISION NOT NULL DEFAULT 0;

-- ─── Public health snapshots / telemetry ────────────────────────────────────

CREATE TABLE IF NOT EXISTS worker_health_snapshots (
  ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  worker_name     TEXT        NOT NULL,
  status          TEXT        NOT NULL,
  queue_depth     INTEGER     NOT NULL DEFAULT 0,
  processed_5m    INTEGER     NOT NULL DEFAULT 0,
  processed_1h    INTEGER     NOT NULL DEFAULT 0,
  last_activity_at TIMESTAMPTZ,
  cpu_load_1m     DOUBLE PRECISION,
  mem_used_pct    DOUBLE PRECISION,
  disk_used_pct   DOUBLE PRECISION
);
ALTER TABLE worker_health_snapshots ADD COLUMN IF NOT EXISTS processed_1h INTEGER NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS worker_health_snapshots_ts_idx
  ON worker_health_snapshots(ts DESC);
CREATE INDEX IF NOT EXISTS worker_health_snapshots_worker_ts_idx
  ON worker_health_snapshots(worker_name, ts DESC);

CREATE TABLE IF NOT EXISTS frontend_error_events (
  id          BIGSERIAL PRIMARY KEY,
  time        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  kind        TEXT        NOT NULL,
  message     TEXT        NOT NULL,
  stack       TEXT,
  page        TEXT,
  user_agent  TEXT
);
CREATE INDEX IF NOT EXISTS frontend_error_events_time_idx
  ON frontend_error_events(time DESC);
