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
  revocation_reason TEXT,
  grant_generation TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (mqtt_username, node_id)
);

ALTER TABLE owner_account_nodes ADD COLUMN IF NOT EXISTS verification_method TEXT;
ALTER TABLE owner_account_nodes ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ;
ALTER TABLE owner_account_nodes ADD COLUMN IF NOT EXISTS grant_id TEXT;
ALTER TABLE owner_account_nodes ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ;
ALTER TABLE owner_account_nodes ADD COLUMN IF NOT EXISTS revocation_reason TEXT;
ALTER TABLE owner_account_nodes ADD COLUMN IF NOT EXISTS grant_generation TEXT;
ALTER TABLE owner_account_nodes ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX IF NOT EXISTS owner_account_nodes_node_idx
  ON owner_account_nodes(node_id);

-- Broker observations are retained for incident review only. They are never
-- authorization evidence and must not be copied into owner_account_nodes.
CREATE TABLE IF NOT EXISTS mqtt_node_logins (
  mqtt_username     TEXT NOT NULL,
  node_id           TEXT NOT NULL,
  last_connected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (mqtt_username, node_id)
);

CREATE INDEX IF NOT EXISTS mqtt_node_logins_username_idx
  ON mqtt_node_logins(mqtt_username, last_connected_at DESC);

CREATE TABLE IF NOT EXISTS owner_grant_audit (
  event_id       TEXT PRIMARY KEY,
  mqtt_username  TEXT NOT NULL,
  node_id        TEXT NOT NULL,
  event_type     TEXT NOT NULL,
  source         TEXT NOT NULL,
  actor          TEXT NOT NULL,
  reason         TEXT,
  generation     TEXT,
  occurred_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  metadata       JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS owner_grant_audit_subject_idx
  ON owner_grant_audit(mqtt_username, node_id, occurred_at DESC);

CREATE TABLE IF NOT EXISTS owner_acl_state (
  singleton            BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
  desired_generation   TEXT,
  rendered_generation  TEXT,
  applied_generation   TEXT,
  desired_at           TIMESTAMPTZ,
  rendered_at          TIMESTAMPTZ,
  applied_at           TIMESTAMPTZ,
  last_verified_at     TIMESTAMPTZ,
  last_error           TEXT,
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO owner_acl_state (singleton)
VALUES (TRUE)
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS owner_acl_artifacts (
  generation       TEXT PRIMARY KEY,
  renderer_version TEXT NOT NULL,
  mode             TEXT NOT NULL,
  content_sha256   TEXT NOT NULL,
  content          TEXT NOT NULL,
  semantic_json    JSONB NOT NULL,
  validation_json  JSONB NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  applied_at       TIMESTAMPTZ
);
