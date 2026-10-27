CREATE TABLE IF NOT EXISTS node_map_suppressions (
  node_id TEXT PRIMARY KEY,
  suppressed_by_node_id TEXT NOT NULL,
  reason TEXT NOT NULL CHECK (reason IN ('stale-colocated')),
  distance_m DOUBLE PRECISION NOT NULL,
  stale_age_days DOUBLE PRECISION NOT NULL,
  freshest_age_days DOUBLE PRECISION NOT NULL,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_node_map_suppressions_computed_at
  ON node_map_suppressions (computed_at);
