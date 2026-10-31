-- Persist only completed, generation-bound automatic identity refreshes.
-- Existing aliases and raw evidence are preserved. Algorithm changes must bump
-- algorithm_version before they can reuse a prior result.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
CREATE TABLE IF NOT EXISTS node_identity_refresh_state (
  singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
  algorithm_version INTEGER NOT NULL CHECK (algorithm_version > 0),
  visibility_generation BIGINT NOT NULL,
  completed_at TIMESTAMPTZ NOT NULL,
  result JSONB NOT NULL
);
