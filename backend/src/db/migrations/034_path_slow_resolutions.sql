-- 034_path_slow_resolutions.sql
-- Observability for slow-mode packet resolution: one row per packet that
-- received a final multi-observer resolution after its propagation window
-- closed (see path-beta/slowMode.ts). Best-effort inserts.
CREATE TABLE IF NOT EXISTS path_slow_resolutions (
  packet_hash    TEXT NOT NULL,
  network        TEXT NOT NULL,
  window_ms      INTEGER NOT NULL,
  observers_seen INTEGER NOT NULL DEFAULT 0,
  scheduled_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at    TIMESTAMPTZ,
  canonical_path JSONB,
  PRIMARY KEY (packet_hash, network)
);

CREATE INDEX IF NOT EXISTS idx_path_slow_resolutions_resolved
  ON path_slow_resolutions (resolved_at DESC);
