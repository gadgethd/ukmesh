-- 057: region-scope participation tables.
--
-- Derived data owned solely by the regions:rebuild tool
-- (src/tools/rebuildRegionParticipation.ts), which refreshes all four tables
-- wholesale inside one transaction. This migration only owns the schema and is
-- intentionally additive: no existing table is touched.
--
-- Region names are public hashtag regions (#name) whose transport key is
-- SHA256("#name")[0:16]; attribution is a candidate-name test against each
-- scoped packet's transport code. Names are stored without the leading '#'.
-- A node is 'green' when it was observed relaying cryptographically scoped
-- traffic and 'red' when only unscoped relay evidence (or none) exists.

CREATE TABLE IF NOT EXISTS region_scope_stats (
  region                TEXT PRIMARY KEY CHECK (char_length(region) BETWEEN 1 AND 64),
  packets_total         INTEGER NOT NULL DEFAULT 0 CHECK (packets_total >= 0),
  member_count          INTEGER NOT NULL DEFAULT 0 CHECK (member_count >= 0),
  repeater_member_count INTEGER NOT NULL DEFAULT 0 CHECK (repeater_member_count >= 0),
  first_seen            TIMESTAMPTZ,
  last_seen             TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS region_scope_members (
  region   TEXT NOT NULL REFERENCES region_scope_stats(region) ON DELETE CASCADE,
  node_id  TEXT NOT NULL CHECK (node_id ~ '^[0-9A-F]{64}$'),
  packets  INTEGER NOT NULL DEFAULT 0 CHECK (packets >= 0),
  PRIMARY KEY (region, node_id)
);

CREATE INDEX IF NOT EXISTS region_scope_members_node_idx
  ON region_scope_members (node_id);

CREATE TABLE IF NOT EXISTS repeater_scope_status (
  node_id             TEXT PRIMARY KEY CHECK (node_id ~ '^[0-9A-F]{64}$'),
  status              TEXT NOT NULL CHECK (status IN ('green', 'red')),
  evidence            TEXT NOT NULL CHECK (evidence IN ('scoped', 'unscoped-only', 'none')),
  scoped_packets      INTEGER NOT NULL DEFAULT 0 CHECK (scoped_packets >= 0),
  unscoped_packets_7d INTEGER NOT NULL DEFAULT 0 CHECK (unscoped_packets_7d >= 0),
  regions             JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS repeater_scope_status_status_idx
  ON repeater_scope_status (status);

-- Single-row run record: when the tool last refreshed the tables and over what
-- window. API responses surface this as generatedAt / windowDays.
CREATE TABLE IF NOT EXISTS region_scope_run (
  id              BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
  finished_at     TIMESTAMPTZ NOT NULL,
  window_days     INTEGER NOT NULL CHECK (window_days BETWEEN 1 AND 365),
  scoped_packets  INTEGER NOT NULL CHECK (scoped_packets >= 0),
  decoded_packets INTEGER NOT NULL CHECK (decoded_packets >= 0)
);
