SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Durable deduplication lets the node UPSERT increment advert_count exactly
-- once for a canonical advert identity, including across process restarts.
CREATE TABLE IF NOT EXISTS node_counted_adverts (
  canonical_advert_hash TEXT PRIMARY KEY,
  node_id                TEXT NOT NULL,
  counted_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS node_counted_adverts_node_time_idx
  ON node_counted_adverts (node_id, counted_at DESC);
