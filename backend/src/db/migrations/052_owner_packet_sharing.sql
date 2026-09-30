-- Owner-controlled packet replication. Destination rows are operator-managed;
-- owners can only select destinations that already exist here.

CREATE TABLE IF NOT EXISTS owner_packet_share_destinations (
  destination_id              TEXT PRIMARY KEY,
  display_name                TEXT NOT NULL CHECK (char_length(display_name) BETWEEN 1 AND 120),
  broker_url                  TEXT,
  topic_template              TEXT,
  broker_username             TEXT,
  broker_password_ciphertext  TEXT,
  enabled                     BOOLEAN NOT NULL DEFAULT FALSE,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (destination_id ~ '^[a-z0-9][a-z0-9_-]{0,63}$'),
  CHECK (broker_url IS NULL OR char_length(broker_url) <= 2_048),
  CHECK (topic_template IS NULL OR char_length(topic_template) BETWEEN 1 AND 512),
  CHECK (broker_username IS NULL OR char_length(broker_username) BETWEEN 1 AND 128),
  CHECK (broker_password_ciphertext IS NULL OR char_length(broker_password_ciphertext) BETWEEN 1 AND 2_048)
);

CREATE TABLE IF NOT EXISTS owner_packet_share_rules (
  owner_username TEXT NOT NULL,
  node_id        TEXT NOT NULL CHECK (node_id ~ '^[0-9A-F]{64}$'),
  destination_id TEXT NOT NULL REFERENCES owner_packet_share_destinations(destination_id) ON DELETE RESTRICT,
  enabled        BOOLEAN NOT NULL DEFAULT TRUE,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (owner_username, node_id, destination_id)
);

CREATE INDEX IF NOT EXISTS owner_packet_share_rules_node_idx
  ON owner_packet_share_rules (node_id, enabled, destination_id);

CREATE TABLE IF NOT EXISTS owner_packet_share_deliveries (
  id                BIGSERIAL PRIMARY KEY,
  owner_username    TEXT NOT NULL,
  node_id           TEXT NOT NULL CHECK (node_id ~ '^[0-9A-F]{64}$'),
  destination_id     TEXT NOT NULL REFERENCES owner_packet_share_destinations(destination_id) ON DELETE RESTRICT,
  event_key         TEXT NOT NULL UNIQUE,
  packet_hash       TEXT NOT NULL,
  source_topic      TEXT NOT NULL,
  destination_topic TEXT NOT NULL,
  payload           JSONB NOT NULL,
  status            TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'delivering', 'succeeded', 'failed', 'dead_lettered')),
  attempts          INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0 AND attempts <= 5),
  next_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_attempt_at   TIMESTAMPTZ,
  delivered_at      TIMESTAMPTZ,
  last_error        TEXT,
  claim_token       TEXT,
  claim_expires_at  TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS owner_packet_share_deliveries_due_idx
  ON owner_packet_share_deliveries (next_attempt_at, id)
  WHERE status IN ('pending', 'failed') AND attempts < 5;

CREATE INDEX IF NOT EXISTS owner_packet_share_deliveries_destination_idx
  ON owner_packet_share_deliveries (destination_id, created_at DESC);

CREATE INDEX IF NOT EXISTS owner_packet_share_deliveries_retention_idx
  ON owner_packet_share_deliveries (created_at, id)
  WHERE status IN ('succeeded', 'dead_lettered');
