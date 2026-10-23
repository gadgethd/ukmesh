-- Phase B region reads use exact scope equality and a bounded recent window.
-- No historical rewrite/backfill. Apply before enabling region scoping.
CREATE INDEX IF NOT EXISTS packets_region_scope_time_idx
  ON packets (region_scope, time DESC)
  WHERE region_scope IS NOT NULL;
