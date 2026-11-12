-- 130_packet_identity_backfill_state.sql
-- P2-5: resumable packet identity backfill checkpoint.
SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE IF NOT EXISTS ops.packet_identity_backfill_state (
  singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
  phase TEXT NOT NULL DEFAULT 'packets',
  status TEXT NOT NULL DEFAULT 'idle'
    CHECK (status IN ('idle', 'running', 'packets_complete', 'complete', 'failed')),
  high_water TIMESTAMPTZ,
  last_time TIMESTAMPTZ,
  last_hash TEXT,
  last_rx_node_id TEXT,
  resolved BIGINT NOT NULL DEFAULT 0,
  unresolved BIGINT NOT NULL DEFAULT 0,
  ambiguous BIGINT NOT NULL DEFAULT 0,
  batch_count BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO ops.packet_identity_backfill_state (singleton)
VALUES (TRUE)
ON CONFLICT (singleton) DO NOTHING;

COMMENT ON TABLE ops.packet_identity_backfill_state IS
  'P2-5 resumable checkpoint for packet UID/alias backfill; one batch in flight.';
