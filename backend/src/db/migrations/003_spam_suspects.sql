CREATE TABLE IF NOT EXISTS spam_suspects (
  time             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  first_seen       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  src_node_id      TEXT NOT NULL,
  spoofed_name     TEXT NOT NULL,
  public_key       TEXT,
  claimed_lat      DOUBLE PRECISION,
  claimed_lon      DOUBLE PRECISION,
  canonical_key    TEXT,
  verdict          TEXT NOT NULL,
  signals          JSONB NOT NULL DEFAULT '[]',
  total_score      INTEGER NOT NULL DEFAULT 0,
  network          TEXT NOT NULL DEFAULT 'ukmesh',
  PRIMARY KEY (src_node_id)
);
-- Applied as ALTER TABLE on existing deployments (DATABASE_SKIP_SCHEMA_INIT):
-- ALTER TABLE spam_suspects ADD COLUMN IF NOT EXISTS first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW();
CREATE INDEX IF NOT EXISTS spam_suspects_name_idx ON spam_suspects (spoofed_name);
CREATE INDEX IF NOT EXISTS spam_suspects_time_idx ON spam_suspects (time DESC);
