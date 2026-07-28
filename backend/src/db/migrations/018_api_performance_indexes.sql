ALTER TABLE packets
  ADD COLUMN IF NOT EXISTS companion_sender TEXT;

CREATE INDEX IF NOT EXISTS idx_nodes_role_coverage
  ON nodes(role, lat, lon)
  WHERE role = 2
    AND lat IS NOT NULL
    AND lon IS NOT NULL
    AND (name IS NULL OR name NOT LIKE '%🚫%');

CREATE INDEX IF NOT EXISTS idx_packets_companion_sender_recent
  ON packets(network, time DESC, companion_sender)
  WHERE packet_type = 5 AND companion_sender IS NOT NULL;

-- Backfill in the migration so existing 24-hour companion activity is visible
-- immediately. Future packets populate the column once at ingest.
UPDATE packets
SET companion_sender = NULLIF(BTRIM(payload->'decrypted'->>'sender'), '')
WHERE packet_type = 5
  AND companion_sender IS NULL
  AND payload->'decrypted'->>'sender' IS NOT NULL
  AND time > NOW() - INTERVAL '24 hours';
