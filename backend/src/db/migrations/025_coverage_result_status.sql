SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE node_coverage
  ADD COLUMN IF NOT EXISTS calculation_status TEXT NOT NULL DEFAULT 'legacy',
  ADD COLUMN IF NOT EXISTS permanent_reason TEXT,
  ADD COLUMN IF NOT EXISTS last_error TEXT,
  ADD COLUMN IF NOT EXISTS retry_after TIMESTAMPTZ;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'node_coverage'::regclass
      AND conname = 'node_coverage_calculation_status_check'
  ) THEN
    ALTER TABLE node_coverage
      ADD CONSTRAINT node_coverage_calculation_status_check
      CHECK (calculation_status IN ('legacy', 'computed', 'permanent')) NOT VALID;
  END IF;
END
$$;

-- A non-empty polygon is authoritative evidence of successful historical
-- computation. Empty legacy polygons remain explicitly unclassified so the
-- rollout inventory can requeue them instead of treating them as success.
UPDATE node_coverage
SET calculation_status = 'computed'
WHERE calculation_status = 'legacy'
  AND jsonb_typeof(geom -> 'coordinates') = 'array'
  AND jsonb_array_length(geom -> 'coordinates') > 0;

CREATE INDEX IF NOT EXISTS node_coverage_retry_idx
  ON node_coverage (calculation_status, retry_after)
  WHERE calculation_status = 'legacy';

DO $$
DECLARE
  constraint_name TEXT;
BEGIN
  SELECT conname
  INTO constraint_name
  FROM pg_constraint
  WHERE conrelid = 'planned_coverage_jobs'::regclass
    AND contype = 'c'
    AND pg_get_constraintdef(oid) LIKE '%status%'
  LIMIT 1;

  IF constraint_name IS NOT NULL
     AND pg_get_constraintdef((
       SELECT oid FROM pg_constraint
       WHERE conrelid = 'planned_coverage_jobs'::regclass
         AND conname = constraint_name
     )) NOT LIKE '%permanent%' THEN
    EXECUTE format(
      'ALTER TABLE planned_coverage_jobs DROP CONSTRAINT %I',
      constraint_name
    );
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'planned_coverage_jobs'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%permanent%'
  ) THEN
    ALTER TABLE planned_coverage_jobs
      ADD CONSTRAINT planned_coverage_jobs_status_v2_check
      CHECK (status IN ('queued', 'running', 'ready', 'failed', 'permanent')) NOT VALID;
  END IF;
END
$$;
