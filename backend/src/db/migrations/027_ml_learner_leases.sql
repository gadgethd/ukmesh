SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS ml_learner_state (
  singleton             BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
  cursor_observed_at    TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00',
  cursor_packet_hash    TEXT NOT NULL DEFAULT '',
  cursor_network        TEXT NOT NULL DEFAULT '',
  cursor_rx_node_id     TEXT NOT NULL DEFAULT '',
  cursor_topic          TEXT NOT NULL DEFAULT '',
  cursor_raw_hex        TEXT NOT NULL DEFAULT '',
  leader_token          TEXT,
  lease_expires_at      TIMESTAMPTZ,
  heartbeat_at          TIMESTAMPTZ,
  run_started_at        TIMESTAMPTZ,
  run_deadline_at       TIMESTAMPTZ,
  next_run_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_trained_at       TIMESTAMPTZ,
  next_training_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  model_version         TEXT NOT NULL DEFAULT 'lightgbm-path-v1',
  data_version          TEXT NOT NULL DEFAULT 'gold-multibyte-v2',
  last_terminal_reason  TEXT,
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO ml_learner_state (singleton, cursor_observed_at)
SELECT
  TRUE,
  CASE
    WHEN state.value ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T'
    THEN state.value::timestamptz
    ELSE '1970-01-01 00:00:00+00'::timestamptz
  END
FROM (SELECT value FROM ml_extraction_state WHERE key = 'gold_extraction_checkpoint') state
ON CONFLICT (singleton) DO NOTHING;

INSERT INTO ml_learner_state (singleton)
VALUES (TRUE)
ON CONFLICT (singleton) DO NOTHING;

ALTER TABLE ml_model_versions
  ADD COLUMN IF NOT EXISTS data_version TEXT NOT NULL DEFAULT 'gold-multibyte-v2';

CREATE INDEX IF NOT EXISTS ml_learner_lease_expiry_idx
  ON ml_learner_state (next_run_at, lease_expires_at);
