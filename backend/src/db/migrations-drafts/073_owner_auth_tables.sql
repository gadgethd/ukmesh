-- 073_owner_auth_tables.sql
-- P2-1: target-side owner-auth tables in the consolidated meshcore database.
-- This migration creates structure only. The data import from meshcore_owner_auth
-- is a separate, fenced, idempotent P2-1 operation with its own checksums.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS core.owner_accounts (
  mqtt_username TEXT PRIMARY KEY,
  is_active     BOOLEAN NOT NULL DEFAULT TRUE,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.owner_account_nodes (
  mqtt_username TEXT NOT NULL REFERENCES core.owner_accounts(mqtt_username) ON DELETE CASCADE,
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

CREATE INDEX IF NOT EXISTS owner_account_nodes_node_idx
  ON core.owner_account_nodes(node_id);

-- Broker observations are incident-review data, never authorization evidence.
CREATE TABLE IF NOT EXISTS core.mqtt_node_logins (
  mqtt_username     TEXT NOT NULL,
  node_id           TEXT NOT NULL,
  last_connected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (mqtt_username, node_id)
);

CREATE INDEX IF NOT EXISTS mqtt_node_logins_username_idx
  ON core.mqtt_node_logins(mqtt_username, last_connected_at DESC);

CREATE TABLE IF NOT EXISTS core.owner_grant_audit (
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
  ON core.owner_grant_audit(mqtt_username, node_id, occurred_at DESC);

CREATE TABLE IF NOT EXISTS core.owner_acl_state (
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

INSERT INTO core.owner_acl_state (singleton)
VALUES (TRUE)
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS core.owner_acl_artifacts (
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
