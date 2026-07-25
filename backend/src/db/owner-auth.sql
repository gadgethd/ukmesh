CREATE TABLE IF NOT EXISTS owner_accounts (
  mqtt_username TEXT PRIMARY KEY,
  is_active     BOOLEAN NOT NULL DEFAULT TRUE,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS owner_account_nodes (
  mqtt_username TEXT NOT NULL REFERENCES owner_accounts(mqtt_username) ON DELETE CASCADE,
  node_id       TEXT NOT NULL,
  verification_method TEXT,
  verified_at   TIMESTAMPTZ,
  grant_id      TEXT,
  revoked_at    TIMESTAMPTZ,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (mqtt_username, node_id)
);

ALTER TABLE owner_account_nodes
  ADD COLUMN IF NOT EXISTS verification_method TEXT;
ALTER TABLE owner_account_nodes
  ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ;
ALTER TABLE owner_account_nodes
  ADD COLUMN IF NOT EXISTS grant_id TEXT;
ALTER TABLE owner_account_nodes
  ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS owner_account_nodes_node_idx
  ON owner_account_nodes(node_id);

-- Non-authoritative connection observations retained only for incident review.
-- Nothing may use these rows to grant dashboard access or write broker ACLs.
CREATE TABLE IF NOT EXISTS mqtt_node_logins (
  mqtt_username     TEXT NOT NULL,
  node_id           TEXT NOT NULL,
  last_connected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (mqtt_username, node_id)
);

CREATE INDEX IF NOT EXISTS mqtt_node_logins_username_idx
  ON mqtt_node_logins(mqtt_username, last_connected_at DESC);
