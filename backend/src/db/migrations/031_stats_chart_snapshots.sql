SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Complete, privacy-filtered public chart responses survive process restarts.
-- Observer-scoped responses are intentionally never written to this table.
CREATE TABLE IF NOT EXISTS stats_chart_snapshots (
  scope_key      TEXT PRIMARY KEY
    CHECK (scope_key ~ '^[a-z0-9][a-z0-9_-]{0,63}$'),
  schema_version INTEGER NOT NULL CHECK (schema_version > 0),
  generated_at   TIMESTAMPTZ NOT NULL,
  payload        JSONB NOT NULL
    CHECK (jsonb_typeof(payload) = 'object')
    CHECK (octet_length(payload::text) <= 2097152),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS stats_chart_snapshots_generated_at_idx
  ON stats_chart_snapshots (generated_at DESC);
