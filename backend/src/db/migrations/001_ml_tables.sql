-- ML path learner tables

CREATE TABLE IF NOT EXISTS ml_gold_paths (
  id                   BIGSERIAL PRIMARY KEY,
  packet_hash          TEXT NOT NULL,
  network              TEXT NOT NULL,
  observed_at          TIMESTAMPTZ NOT NULL,
  hop_position         INTEGER NOT NULL,
  true_node_id         TEXT NOT NULL,
  hash_2char           TEXT NOT NULL,
  hash_4char           TEXT NOT NULL,
  hash_6char           TEXT NOT NULL,
  path_hash_size_bytes INTEGER NOT NULL,
  observer_ids         TEXT[] NOT NULL,
  rx_region            TEXT,
  created_at           TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ml_gold_paths_dedup
  ON ml_gold_paths (packet_hash, hop_position, true_node_id);
CREATE INDEX IF NOT EXISTS ml_gold_paths_net_time
  ON ml_gold_paths (network, observed_at DESC);

CREATE TABLE IF NOT EXISTS ml_path_prefix_scores (
  network           TEXT NOT NULL,
  hash_2char        TEXT NOT NULL,
  node_id           TEXT NOT NULL,
  score             DOUBLE PRECISION NOT NULL,
  observation_count INTEGER NOT NULL DEFAULT 1,
  correct_count     INTEGER NOT NULL DEFAULT 0,
  packet_count      INTEGER NOT NULL DEFAULT 0,
  complete_path_count INTEGER NOT NULL DEFAULT 0,
  model_version     TEXT NOT NULL,
  updated_at        TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (network, hash_2char, node_id)
);

CREATE INDEX IF NOT EXISTS ml_scores_lookup
  ON ml_path_prefix_scores (network, hash_2char);

CREATE TABLE IF NOT EXISTS ml_model_versions (
  version          TEXT PRIMARY KEY,
  network          TEXT NOT NULL,
  trained_at       TIMESTAMPTZ NOT NULL,
  gold_paths_used  INTEGER NOT NULL,
  top1_accuracy    DOUBLE PRECISION,
  top3_accuracy    DOUBLE PRECISION,
  evaluated_packets INTEGER NOT NULL DEFAULT 0,
  evaluated_hops    INTEGER NOT NULL DEFAULT 0,
  complete_path_accuracy DOUBLE PRECISION,
  mean_path_completion DOUBLE PRECISION,
  is_active        BOOLEAN NOT NULL DEFAULT FALSE,
  promoted_at      TIMESTAMPTZ,
  model_artifact   BYTEA
);

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

CREATE TABLE IF NOT EXISTS ml_extraction_state (
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
