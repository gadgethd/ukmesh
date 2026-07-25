CREATE INDEX IF NOT EXISTS packets_hash_network_time_idx
  ON packets (packet_hash, network, time DESC);
