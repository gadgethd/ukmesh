-- A regular table provides an enforceable observation identity across packet
-- hypertable chunks. The batch writer holds its transaction advisory lock
-- before allocating seq, so committed seq values follow writer commit order.
-- Gaps from rolled-back transactions are harmless for a seq > cursor reader.
CREATE TABLE IF NOT EXISTS ingest_observation_ledger (
  seq BIGSERIAL PRIMARY KEY,
  observation_id UUID NOT NULL UNIQUE,
  observed_at TIMESTAMPTZ NOT NULL,
  packet_hash TEXT NOT NULL,
  network TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ingest_observation_ledger_network_seq_idx
  ON ingest_observation_ledger (network, seq);
