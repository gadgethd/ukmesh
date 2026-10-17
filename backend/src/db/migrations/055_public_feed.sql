-- Account-level opt-in for the aggregate public MeshCore MQTT feed.
-- Ownership is stored as a materialised mapping because the owner-auth
-- database is intentionally separate from the analytics database. The
-- backend refreshes this mapping from verified owner grants periodically.

CREATE TABLE IF NOT EXISTS owner_public_feed_settings (
  owner_username TEXT PRIMARY KEY CHECK (char_length(owner_username) BETWEEN 1 AND 128),
  enabled        BOOLEAN NOT NULL DEFAULT FALSE,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS owner_public_feed_nodes (
  owner_username TEXT NOT NULL REFERENCES owner_public_feed_settings(owner_username) ON DELETE CASCADE,
  node_id        TEXT NOT NULL CHECK (node_id ~ '^[0-9A-F]{64}$'),
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (owner_username, node_id)
);

CREATE INDEX IF NOT EXISTS owner_public_feed_nodes_node_idx
  ON owner_public_feed_nodes (node_id, owner_username);

CREATE TABLE IF NOT EXISTS public_feed_deliveries (
  id                BIGSERIAL PRIMARY KEY,
  node_id           TEXT NOT NULL CHECK (node_id ~ '^[0-9A-F]{64}$'),
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

CREATE INDEX IF NOT EXISTS public_feed_deliveries_due_idx
  ON public_feed_deliveries (next_attempt_at, id)
  WHERE status IN ('pending', 'failed') AND attempts < 5;

CREATE INDEX IF NOT EXISTS public_feed_deliveries_retention_idx
  ON public_feed_deliveries (created_at, id)
  WHERE status IN ('succeeded', 'dead_lettered');
