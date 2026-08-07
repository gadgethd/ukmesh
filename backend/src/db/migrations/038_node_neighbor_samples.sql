-- Store the observer's latest neighbor advertisements as an isolated telemetry stream.
CREATE TABLE IF NOT EXISTS node_neighbor_samples (
  node_id              TEXT             NOT NULL,
  time                 TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
  neighbors            JSONB            NOT NULL CHECK (jsonb_typeof(neighbors) = 'array'),
  network              TEXT             NOT NULL DEFAULT 'ukmesh'
);

SELECT create_hypertable('node_neighbor_samples', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS node_neighbor_samples_node_time_idx
  ON node_neighbor_samples (node_id, time DESC);
CREATE INDEX IF NOT EXISTS node_neighbor_samples_network_time_idx
  ON node_neighbor_samples (network, time DESC);
