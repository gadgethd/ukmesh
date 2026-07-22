-- Small rollup tables for API stats that were previously derived by scanning
-- recent packet chunks on every cold request.

CREATE TABLE IF NOT EXISTS packet_daily_stats (
  network         TEXT NOT NULL,
  day             DATE NOT NULL,
  max_hop_count   INTEGER,
  max_hop_hash    TEXT,
  max_hop_seen_at TIMESTAMPTZ,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (network, day)
);
CREATE INDEX IF NOT EXISTS packet_daily_stats_network_max_hop_idx
  ON packet_daily_stats (network, max_hop_count DESC NULLS LAST, day DESC);

CREATE TABLE IF NOT EXISTS observer_region_packet_sightings (
  network     TEXT NOT NULL,
  iata        TEXT NOT NULL,
  packet_hash TEXT NOT NULL,
  first_seen  TIMESTAMPTZ NOT NULL,
  last_seen   TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (network, iata, packet_hash)
);
CREATE INDEX IF NOT EXISTS observer_region_packet_sightings_network_last_seen_idx
  ON observer_region_packet_sightings (network, last_seen DESC, iata);

CREATE TABLE IF NOT EXISTS observer_region_observer_sightings (
  network    TEXT NOT NULL,
  iata       TEXT NOT NULL,
  rx_node_id TEXT NOT NULL,
  first_seen TIMESTAMPTZ NOT NULL,
  last_seen  TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (network, iata, rx_node_id)
);
CREATE INDEX IF NOT EXISTS observer_region_observer_sightings_network_last_seen_idx
  ON observer_region_observer_sightings (network, last_seen DESC, iata);
