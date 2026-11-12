-- 075_owner_sessions.sql
-- Server-side owner sessions (revocable cookies) land in the consolidated
-- owner-auth namespace, next to the other core.owner_* tables from 073.
-- Structure only: runtime schema DDL is disabled, so this must exist before
-- the owner session store is used.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS core.owner_sessions (
  session_id    TEXT PRIMARY KEY,
  mqtt_username TEXT NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at    TIMESTAMPTZ NOT NULL,
  revoked_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS owner_sessions_active_idx
  ON core.owner_sessions(mqtt_username, revoked_at, expires_at);
