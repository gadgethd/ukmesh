CREATE TABLE IF NOT EXISTS network_unification_runs (
  run_id          TEXT PRIMARY KEY,
  started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at    TIMESTAMPTZ,
  status          TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
  backup_reference TEXT NOT NULL,
  detail          JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS network_unification_one_running_idx
  ON network_unification_runs ((status))
  WHERE status = 'running';
