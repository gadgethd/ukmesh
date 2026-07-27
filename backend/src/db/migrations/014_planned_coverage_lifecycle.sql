SET LOCAL lock_timeout = '5s';

ALTER TABLE node_coverage
  ADD COLUMN IF NOT EXISTS is_planned BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS planned_coverage_jobs (
  job_id        TEXT PRIMARY KEY,
  fingerprint   TEXT NOT NULL,
  lat           DOUBLE PRECISION NOT NULL,
  lon           DOUBLE PRECISION NOT NULL,
  status        TEXT NOT NULL CHECK (status IN ('queued', 'running', 'ready', 'failed')),
  heartbeat_at  TIMESTAMPTZ,
  expires_at    TIMESTAMPTZ NOT NULL,
  error         TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS planned_coverage_jobs_expiry_idx
  ON planned_coverage_jobs(expires_at);
CREATE INDEX IF NOT EXISTS planned_coverage_jobs_fingerprint_idx
  ON planned_coverage_jobs(fingerprint, created_at DESC);

CREATE TABLE IF NOT EXISTS planned_coverage_handles (
  handle_hash  TEXT PRIMARY KEY,
  hash_alg     TEXT NOT NULL CHECK (hash_alg IN ('sha256', 'md5')),
  job_id       TEXT NOT NULL REFERENCES planned_coverage_jobs(job_id) ON DELETE CASCADE,
  expires_at   TIMESTAMPTZ NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS planned_coverage_handles_job_idx
  ON planned_coverage_handles(job_id);
CREATE INDEX IF NOT EXISTS planned_coverage_handles_expiry_idx
  ON planned_coverage_handles(expires_at);

-- Explicitly migrate legacy plan_* results into the capability lifecycle.
-- Their existing raw plan ID remains a temporary dual-read capability, but it
-- now expires and participates in the same cleanup path.
INSERT INTO planned_coverage_jobs (
  job_id, fingerprint, lat, lon, status, expires_at, created_at, updated_at
)
SELECT
  nc.node_id,
  'legacy:' || nc.node_id,
  0,
  0,
  'ready',
  NOW() + INTERVAL '24 hours',
  COALESCE(nc.calculated_at, NOW()),
  NOW()
FROM node_coverage nc
WHERE nc.node_id LIKE 'plan_%'
ON CONFLICT (job_id) DO NOTHING;

INSERT INTO planned_coverage_handles (handle_hash, hash_alg, job_id, expires_at)
SELECT
  MD5(job_id),
  'md5',
  job_id,
  expires_at
FROM planned_coverage_jobs
WHERE fingerprint LIKE 'legacy:%'
ON CONFLICT (handle_hash) DO NOTHING;

UPDATE node_coverage nc
SET is_planned = TRUE,
    expires_at = jobs.expires_at
FROM planned_coverage_jobs jobs
WHERE nc.node_id = jobs.job_id
  AND jobs.fingerprint LIKE 'legacy:%';
