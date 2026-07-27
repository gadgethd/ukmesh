SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS link_job_commits (
  job_id        TEXT PRIMARY KEY,
  job_type      TEXT NOT NULL CHECK (job_type IN ('observe', 'physical_pair')),
  generation    TEXT,
  logical_job_id TEXT,
  payload_hash  TEXT NOT NULL,
  completed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS link_job_commits_generation_idx
  ON link_job_commits(generation, completed_at);
CREATE INDEX IF NOT EXISTS link_job_commits_logical_idx
  ON link_job_commits(logical_job_id)
  WHERE logical_job_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS link_rebuild_runs (
  generation       TEXT PRIMARY KEY,
  schema_name      TEXT NOT NULL UNIQUE,
  status           TEXT NOT NULL CHECK (status IN ('preparing', 'admitting', 'processing', 'complete', 'published', 'failed')),
  window_end       TIMESTAMPTZ NOT NULL,
  expected_jobs    BIGINT NOT NULL DEFAULT 0,
  admitted_jobs    BIGINT NOT NULL DEFAULT 0,
  completed_jobs   BIGINT NOT NULL DEFAULT 0,
  error             TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at      TIMESTAMPTZ
);
