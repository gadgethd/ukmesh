-- Add evolutionary training metadata to ml_model_versions
ALTER TABLE ml_model_versions
  ADD COLUMN IF NOT EXISTS hyperparams     JSONB,
  ADD COLUMN IF NOT EXISTS generation      INTEGER NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS variant_rank    INTEGER NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS population_size INTEGER NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS evaluated_packets INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS evaluated_hops INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS complete_path_accuracy DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS mean_path_completion DOUBLE PRECISION;

ALTER TABLE ml_path_prefix_scores
  ADD COLUMN IF NOT EXISTS correct_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS packet_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS complete_path_count INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS ml_model_variant_runs (
  training_run_id        TEXT NOT NULL,
  model_network          TEXT NOT NULL,
  generation             INTEGER NOT NULL,
  variant_rank           INTEGER NOT NULL,
  population_size        INTEGER NOT NULL,
  hyperparams            JSONB,
  evaluated_packets      INTEGER NOT NULL,
  evaluated_hops         INTEGER NOT NULL,
  hop_accuracy           DOUBLE PRECISION,
  hop_top3_accuracy      DOUBLE PRECISION,
  complete_path_accuracy DOUBLE PRECISION,
  mean_path_completion   DOUBLE PRECISION,
  val_evaluated_packets  INTEGER NOT NULL,
  val_evaluated_hops     INTEGER NOT NULL,
  val_hop_accuracy       DOUBLE PRECISION,
  val_hop_top3_accuracy  DOUBLE PRECISION,
  val_complete_path_accuracy DOUBLE PRECISION,
  val_mean_path_completion   DOUBLE PRECISION,
  created_at             TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (training_run_id, variant_rank)
);

CREATE INDEX IF NOT EXISTS ml_variant_runs_generation_idx
  ON ml_model_variant_runs (model_network, generation, variant_rank);

CREATE TABLE IF NOT EXISTS ml_model_variant_packet_results (
  training_run_id  TEXT NOT NULL,
  model_network    TEXT NOT NULL,
  generation       INTEGER NOT NULL,
  variant_rank     INTEGER NOT NULL,
  packet_network   TEXT NOT NULL,
  packet_hash      TEXT NOT NULL,
  expected_hops    INTEGER NOT NULL,
  predicted_hops   INTEGER NOT NULL,
  correct_hops     INTEGER NOT NULL,
  complete_path    BOOLEAN NOT NULL,
  path_completion  DOUBLE PRECISION NOT NULL,
  created_at       TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (training_run_id, variant_rank, packet_network, packet_hash)
);

CREATE INDEX IF NOT EXISTS ml_variant_packet_results_packet_idx
  ON ml_model_variant_packet_results (packet_network, packet_hash);

CREATE INDEX IF NOT EXISTS ml_variant_packet_results_generation_idx
  ON ml_model_variant_packet_results (model_network, generation, variant_rank);
