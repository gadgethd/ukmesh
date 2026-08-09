-- Publication metadata and run heartbeat for change-driven path learning.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE path_model_calibration
  ADD COLUMN IF NOT EXISTS input_hash TEXT,
  ADD COLUMN IF NOT EXISTS model_hash TEXT,
  ADD COLUMN IF NOT EXISTS algorithm_version TEXT,
  ADD COLUMN IF NOT EXISTS privacy_generation BIGINT,
  ADD COLUMN IF NOT EXISTS window_start TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS window_end TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_mutation_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE path_model_calibration
  DROP CONSTRAINT IF EXISTS path_model_calibration_hash_format_check;
ALTER TABLE path_model_calibration
  ADD CONSTRAINT path_model_calibration_hash_format_check CHECK (
    (input_hash IS NULL OR input_hash ~ '^[0-9a-f]{64}$')
    AND (model_hash IS NULL OR model_hash ~ '^[0-9a-f]{64}$')
    AND (privacy_generation IS NULL OR privacy_generation > 0)
    AND (window_start IS NULL OR window_end IS NULL OR window_end >= window_start)
    AND last_mutation_count >= 0
  );
