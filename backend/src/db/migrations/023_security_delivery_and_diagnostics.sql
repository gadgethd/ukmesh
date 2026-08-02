CREATE TABLE IF NOT EXISTS owner_alert_deliveries (
  id BIGSERIAL PRIMARY KEY,
  rule_id BIGINT NOT NULL REFERENCES owner_alert_rules(id) ON DELETE CASCADE,
  event_key TEXT NOT NULL,
  destination_host TEXT NOT NULL,
  payload JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'delivering', 'succeeded', 'failed')),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0 AND attempts <= 5),
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_attempt_at TIMESTAMPTZ,
  delivered_at TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (rule_id, event_key)
);
CREATE INDEX IF NOT EXISTS owner_alert_deliveries_due_idx
  ON owner_alert_deliveries (next_attempt_at, id)
  WHERE status IN ('pending', 'failed') AND attempts < 5;
CREATE INDEX IF NOT EXISTS owner_alert_deliveries_created_idx
  ON owner_alert_deliveries (created_at DESC);

ALTER TABLE frontend_error_events
  ADD COLUMN IF NOT EXISTS fingerprint TEXT,
  ADD COLUMN IF NOT EXISTS source_hash TEXT,
  ADD COLUMN IF NOT EXISTS bucket_start TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS occurrences INTEGER NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
CREATE UNIQUE INDEX IF NOT EXISTS frontend_error_events_dedupe_idx
  ON frontend_error_events (fingerprint, source_hash, bucket_start)
  WHERE fingerprint IS NOT NULL AND source_hash IS NOT NULL AND bucket_start IS NOT NULL;
CREATE INDEX IF NOT EXISTS frontend_error_events_source_time_idx
  ON frontend_error_events (source_hash, time DESC);
