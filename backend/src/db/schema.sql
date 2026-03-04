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

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS packets_hash_idx   ON packets (packet_hash, time DESC);
CREATE INDEX IF NOT EXISTS packets_rx_idx     ON packets (rx_node_id, time DESC);
CREATE INDEX IF NOT EXISTS packets_src_idx    ON packets (src_node_id, time DESC);

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

-- ─── Coverage polygons (one row per node, recalculated on position change) ───

CREATE TABLE IF NOT EXISTS node_coverage (
  node_id          TEXT PRIMARY KEY,
  geom             JSONB NOT NULL,            -- GeoJSON Polygon or MultiPolygon
  antenna_height_m DOUBLE PRECISION DEFAULT 10,
  radius_m         DOUBLE PRECISION DEFAULT 30000,
  calculated_at    TIMESTAMPTZ DEFAULT NOW()
);
