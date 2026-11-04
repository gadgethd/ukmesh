-- Packet/message reads always join this optional enrichment table. Own its
-- schema in migrations so a fresh backend works before the tagger starts, or
-- when the external classifier is disabled. Matches tagger_worker.py's DDL.
CREATE TABLE IF NOT EXISTS message_tags (
  packet_hash TEXT PRIMARY KEY,
  packet_time TIMESTAMPTZ NOT NULL,
  tagged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  model TEXT,
  latency_ms INTEGER,
  tags JSONB NOT NULL,
  confidence JSONB
);
