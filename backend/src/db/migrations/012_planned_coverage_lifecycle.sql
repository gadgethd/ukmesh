ALTER TABLE node_coverage
  ADD COLUMN IF NOT EXISTS is_planned BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_node_coverage_planned_expiry
  ON node_coverage (expires_at)
  WHERE is_planned = TRUE;
