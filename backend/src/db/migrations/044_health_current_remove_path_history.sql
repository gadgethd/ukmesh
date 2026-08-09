SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Keep only the latest status for request-time health reads. Historical rows
-- remain a short, bounded operator timeline maintained by the health worker.
CREATE TABLE IF NOT EXISTS worker_health_current (
  worker_name      TEXT PRIMARY KEY,
  captured_at      TIMESTAMPTZ NOT NULL,
  status           TEXT NOT NULL,
  queue_depth      INTEGER NOT NULL DEFAULT 0,
  processed_1h     INTEGER NOT NULL DEFAULT 0,
  last_activity_at TIMESTAMPTZ,
  cpu_load_1m      DOUBLE PRECISION,
  cpu_usage_pct    DOUBLE PRECISION,
  mem_used_pct     DOUBLE PRECISION,
  disk_used_pct    DOUBLE PRECISION,
  queue_bytes      BIGINT,
  dead_jobs        INTEGER,
  retries          INTEGER,
  active_leases    INTEGER,
  oldest_age_s     DOUBLE PRECISION
);

INSERT INTO worker_health_current (
  worker_name, captured_at, status, queue_depth, processed_1h,
  last_activity_at, cpu_load_1m, mem_used_pct, disk_used_pct
)
SELECT DISTINCT ON (worker_name)
       worker_name, ts, status, queue_depth, processed_1h,
       last_activity_at, cpu_load_1m, mem_used_pct, disk_used_pct
  FROM worker_health_snapshots
 ORDER BY worker_name, ts DESC
ON CONFLICT (worker_name) DO NOTHING;

-- The retired path-history API and worker were the only consumers. The live
-- multibyte Paths layer uses separate observation/fact tables and is retained.
DROP TABLE IF EXISTS path_history_cache;
