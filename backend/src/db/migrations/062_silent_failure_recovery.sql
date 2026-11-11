SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE privacy_rematerialization_queue ADD COLUMN IF NOT EXISTS claim_token UUID;
ALTER TABLE privacy_rematerialization_queue ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS privacy_remat_expired_idx
  ON privacy_rematerialization_queue (claim_expires_at) WHERE status = 'processing';

-- Keep payload-free evidence of deterministic poison messages. No credentials
-- or decoded message content are copied into this table.
CREATE TABLE IF NOT EXISTS ingest_rejections (
  message_hash TEXT PRIMARY KEY,
  topic TEXT NOT NULL,
  reason TEXT NOT NULL,
  payload_bytes INTEGER NOT NULL,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  occurrences BIGINT NOT NULL DEFAULT 1
);

-- Written in the packet transaction, drained only after commit. Retrying each
-- destination enqueue is safe because its delivery event key is unique.
CREATE TABLE IF NOT EXISTS packet_forwarding_outbox (
  observation_id UUID PRIMARY KEY,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  attempts INTEGER NOT NULL DEFAULT 0,
  claim_token UUID,
  claim_expires_at TIMESTAMPTZ,
  last_error TEXT
);
CREATE INDEX IF NOT EXISTS packet_forwarding_outbox_due_idx
  ON packet_forwarding_outbox (next_attempt_at, created_at);

-- Repair exhausted rows stranded by the previous claim reclaimer.
UPDATE owner_alert_deliveries SET status = 'dead_lettered', claim_token = NULL,
  claim_expires_at = NULL, updated_at = NOW()
WHERE attempts >= 5 AND (status IN ('pending', 'failed')
  OR (status = 'delivering' AND (claim_expires_at IS NULL OR claim_expires_at <= NOW())));
