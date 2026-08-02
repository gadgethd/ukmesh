-- meshcore:migration-mode non-transactional
CREATE INDEX IF NOT EXISTS packets_ml_cursor_idx
  ON packets (
    time,
    packet_hash,
    network,
    COALESCE(rx_node_id, ''),
    topic,
    COALESCE(raw_hex, '')
  )
  WITH (timescaledb.transaction_per_chunk)
  WHERE path_hash_size_bytes > 1
    AND path_hashes IS NOT NULL;
