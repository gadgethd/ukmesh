CREATE TABLE IF NOT EXISTS analysis_runs (
  run_id          TEXT PRIMARY KEY,
  workload        TEXT NOT NULL,
  scope           TEXT NOT NULL,
  status          TEXT NOT NULL CHECK (status IN ('running', 'complete', 'partial', 'failed', 'timed_out', 'stale')),
  window_start    TIMESTAMPTZ NOT NULL,
  window_end      TIMESTAMPTZ NOT NULL,
  checkpoint      BIGINT NOT NULL DEFAULT 0,
  total_items     BIGINT NOT NULL DEFAULT 0,
  generation      TEXT,
  metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
  started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  heartbeat_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at    TIMESTAMPTZ,
  CONSTRAINT analysis_runs_window_order CHECK (window_start <= window_end)
);

CREATE INDEX IF NOT EXISTS analysis_runs_workload_status_idx
  ON analysis_runs(workload, scope, status, started_at DESC);

CREATE TABLE IF NOT EXISTS analysis_workload_state (
  workload                 TEXT NOT NULL,
  scope                    TEXT NOT NULL,
  active_run_id            TEXT REFERENCES analysis_runs(run_id) ON DELETE SET NULL,
  last_complete_generation TEXT,
  last_complete_at         TIMESTAMPTZ,
  last_status              TEXT,
  last_error               TEXT,
  updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (workload, scope)
);
