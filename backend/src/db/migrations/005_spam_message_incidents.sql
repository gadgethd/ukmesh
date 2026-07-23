-- Message-spam detection: derived analysis tables.
-- Additive only. Raw text / exact coordinates / sender names live here for the
-- operator view; the public API serves the pre-sanitized `public_json` column.

CREATE TABLE IF NOT EXISTS spam_message_incidents (
  incident_key      TEXT PRIMARY KEY,
  network           TEXT NOT NULL,
  status            TEXT NOT NULL DEFAULT 'active',   -- 'active' | 'closed'
  first_seen        TIMESTAMPTZ NOT NULL,
  last_seen         TIMESTAMPTZ NOT NULL,
  message_count     INTEGER NOT NULL DEFAULT 0,
  observer_count    INTEGER NOT NULL DEFAULT 0,
  channels          TEXT[]  NOT NULL DEFAULT '{}',
  username_variants INTEGER NOT NULL DEFAULT 0,

  -- Local-only raw evidence (never returned by the public API):
  sender_names       TEXT[] NOT NULL DEFAULT '{}',
  representative_text TEXT,
  canonical_text      TEXT,

  spam_marker       BOOLEAN NOT NULL DEFAULT FALSE,
  score             DOUBLE PRECISION NOT NULL DEFAULT 0,
  reasons           JSONB NOT NULL DEFAULT '[]',

  -- Local-only precise origin; coarsened version lives inside public_json:
  origin_lat        DOUBLE PRECISION,
  origin_lon        DOUBLE PRECISION,
  origin_radius_km  DOUBLE PRECISION,
  origin_region     TEXT,
  origin_confidence DOUBLE PRECISION,
  origin_level      TEXT,

  -- Fully sanitized, publishable snapshot of this incident:
  public_json       JSONB NOT NULL DEFAULT '{}',

  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS spam_message_incidents_status_idx
  ON spam_message_incidents (status, last_seen DESC);
CREATE INDEX IF NOT EXISTS spam_message_incidents_network_idx
  ON spam_message_incidents (network, last_seen DESC);

CREATE TABLE IF NOT EXISTS spam_message_members (
  incident_key   TEXT NOT NULL REFERENCES spam_message_incidents(incident_key) ON DELETE CASCADE,
  packet_hash    TEXT NOT NULL,
  network        TEXT NOT NULL,
  observed_at    TIMESTAMPTZ NOT NULL,
  sender         TEXT,                 -- local-only raw sender name
  channel_label  TEXT,
  channel_hash   TEXT,
  observer_count INTEGER NOT NULL DEFAULT 0,
  min_hop_count  INTEGER,
  best_rssi      DOUBLE PRECISION,
  best_snr       DOUBLE PRECISION,
  PRIMARY KEY (incident_key, packet_hash)
);

CREATE INDEX IF NOT EXISTS spam_message_members_incident_idx
  ON spam_message_members (incident_key, observed_at);
