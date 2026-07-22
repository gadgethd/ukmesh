-- Bring existing deployments in line with the planned-coverage API.
--
-- Historical rollup reconstruction intentionally lives in
-- `tools/backfillStatsRollups.ts`, not this migration. The migration runner
-- wraps each file in a transaction, so pairing a table ALTER with a long
-- packet scan would retain AccessExclusive locks for the entire scan.

-- Fail quickly rather than queue behind a busy ingest table during deployment.
SET LOCAL lock_timeout = '5s';

ALTER TABLE node_coverage
  ADD COLUMN IF NOT EXISTS predicted_links JSONB;

-- Existing databases were originally created with a legacy production default.
-- New ingest writes an explicit scope, but tools and older callers must not
-- silently create fresh rows under the retired label after the upgrade.
ALTER TABLE nodes ALTER COLUMN network SET DEFAULT 'ukmesh';
ALTER TABLE packets ALTER COLUMN network SET DEFAULT 'ukmesh';
ALTER TABLE node_status_samples ALTER COLUMN network SET DEFAULT 'ukmesh';
