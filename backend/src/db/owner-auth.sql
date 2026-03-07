CREATE TABLE IF NOT EXISTS owner_accounts (
  mqtt_username TEXT PRIMARY KEY,
  is_active     BOOLEAN NOT NULL DEFAULT TRUE,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS owner_account_nodes (
  mqtt_username TEXT NOT NULL REFERENCES owner_accounts(mqtt_username) ON DELETE CASCADE,
  node_id       TEXT NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (mqtt_username, node_id)
);

CREATE INDEX IF NOT EXISTS owner_account_nodes_node_idx
  ON owner_account_nodes(node_id);
