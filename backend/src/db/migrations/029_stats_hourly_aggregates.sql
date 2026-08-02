SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Expand-only maintained rollups. Historical population is intentionally
-- performed by the resumable stats backfill tool, never in this migration.
CREATE TABLE IF NOT EXISTS packet_hourly_stats (
  network         TEXT NOT NULL,
  hour            TIMESTAMPTZ NOT NULL,
  packet_type     INTEGER NOT NULL,
  hop_count       INTEGER NOT NULL,
  route_type      INTEGER NOT NULL,
  transport_code  TEXT NOT NULL,
  region_scope    TEXT NOT NULL,
  packet_count    BIGINT NOT NULL DEFAULT 0 CHECK (packet_count >= 0),
  rssi_sum        DOUBLE PRECISION NOT NULL DEFAULT 0,
  rssi_count      BIGINT NOT NULL DEFAULT 0 CHECK (rssi_count >= 0),
  snr_sum         DOUBLE PRECISION NOT NULL DEFAULT 0,
  snr_count       BIGINT NOT NULL DEFAULT 0 CHECK (snr_count >= 0),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (
    network, hour, packet_type, hop_count, route_type,
    transport_code, region_scope
  )
);

CREATE INDEX IF NOT EXISTS packet_hourly_stats_hour_network_idx
  ON packet_hourly_stats (hour DESC, network);

CREATE TABLE IF NOT EXISTS maintenance_backfill_checkpoints (
  job_name       TEXT PRIMARY KEY,
  cursor_value   TEXT NOT NULL,
  window_end     TIMESTAMPTZ NOT NULL,
  rows_processed BIGINT NOT NULL DEFAULT 0 CHECK (rows_processed >= 0),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at   TIMESTAMPTZ
);
