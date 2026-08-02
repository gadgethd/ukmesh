ALTER TABLE analysis_workload_state
  ADD COLUMN IF NOT EXISTS active_lease_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS active_lease_owner TEXT,
  ADD COLUMN IF NOT EXISTS active_lease_token TEXT,
  ADD COLUMN IF NOT EXISTS active_run_deadline_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS active_attempt INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS expected_privacy_generation BIGINT,
  ADD COLUMN IF NOT EXISTS expected_model_generation TEXT,
  ADD COLUMN IF NOT EXISTS last_terminal_reason TEXT;

ALTER TABLE analysis_runs
  ADD COLUMN IF NOT EXISTS lease_owner TEXT,
  ADD COLUMN IF NOT EXISTS lease_token TEXT,
  ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS run_deadline_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS terminal_reason TEXT,
  ADD COLUMN IF NOT EXISTS privacy_generation BIGINT,
  ADD COLUMN IF NOT EXISTS model_generation TEXT;

ALTER TABLE analysis_runs
  DROP CONSTRAINT IF EXISTS analysis_runs_attempt_positive;
ALTER TABLE analysis_runs
  ADD CONSTRAINT analysis_runs_attempt_positive CHECK (attempt > 0);

CREATE INDEX IF NOT EXISTS analysis_workload_state_lease_idx
  ON analysis_workload_state (active_lease_expires_at)
  WHERE active_run_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS analysis_runs_lease_token_idx
  ON analysis_runs (lease_token)
  WHERE lease_token IS NOT NULL;
