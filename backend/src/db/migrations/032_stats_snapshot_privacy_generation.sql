SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE stats_chart_snapshots
  ADD COLUMN IF NOT EXISTS visibility_generation BIGINT;

-- A pre-v2 snapshot is safe to adopt only if it was generated after the most
-- recent public/private visibility change. Other rows are disposable derived
-- cache data and must fail closed rather than crossing the privacy boundary.
UPDATE stats_chart_snapshots snapshot
   SET visibility_generation = visibility.generation,
       schema_version = 2,
       payload = jsonb_set(
         snapshot.payload,
         '{snapshot,visibilityGeneration}',
         to_jsonb(visibility.generation),
         TRUE
       ),
       updated_at = NOW()
  FROM public_visibility_state visibility
 WHERE visibility.singleton = TRUE
   AND snapshot.visibility_generation IS NULL
   AND snapshot.generated_at >= visibility.updated_at;

DELETE FROM stats_chart_snapshots
 WHERE visibility_generation IS NULL;

ALTER TABLE stats_chart_snapshots
  ALTER COLUMN visibility_generation SET NOT NULL;

ALTER TABLE stats_chart_snapshots
  DROP CONSTRAINT IF EXISTS stats_chart_snapshots_visibility_generation_check;
ALTER TABLE stats_chart_snapshots
  ADD CONSTRAINT stats_chart_snapshots_visibility_generation_check
  CHECK (visibility_generation > 0);

CREATE INDEX IF NOT EXISTS stats_chart_snapshots_visibility_idx
  ON stats_chart_snapshots (visibility_generation, generated_at DESC);
