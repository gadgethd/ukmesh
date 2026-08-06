-- Decrypted content for historical packets.
-- Kept OUT of the packets hypertable: UPDATEs on the hypertable seq-scan every
-- chunk (TimescaleDB limitation), while this table is append-only and joined
-- in at read time. Live packets continue to carry decrypted/_summary inside
-- their payload jsonb (written by the ingest path).
CREATE TABLE IF NOT EXISTS packet_decryptions (
  packet_hash text PRIMARY KEY,
  decrypted   jsonb NOT NULL,
  summary     text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);
