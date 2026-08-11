SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS path_position_prefix_priors (
  network      TEXT        NOT NULL,
  prefix       TEXT        NOT NULL,
  position     SMALLINT    NOT NULL CHECK (position >= 0),
  node_id      TEXT        NOT NULL,
  count        INTEGER     NOT NULL CHECK (count > 0),
  probability  DOUBLE PRECISION NOT NULL CHECK (probability >= 0 AND probability <= 1),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (network, prefix, position, node_id)
);
CREATE INDEX IF NOT EXISTS path_position_prefix_priors_lookup_idx
  ON path_position_prefix_priors (network, prefix, position);

CREATE TABLE IF NOT EXISTS path_corridor_priors (
  network      TEXT        NOT NULL,
  src_node_id  TEXT        NOT NULL,
  rx_node_id   TEXT        NOT NULL,
  position     SMALLINT    NOT NULL CHECK (position >= 0),
  node_id      TEXT        NOT NULL,
  count        INTEGER     NOT NULL CHECK (count > 0),
  probability  DOUBLE PRECISION NOT NULL CHECK (probability >= 0 AND probability <= 1),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (network, src_node_id, rx_node_id, position, node_id)
);
CREATE INDEX IF NOT EXISTS path_corridor_priors_lookup_idx
  ON path_corridor_priors (network, src_node_id, rx_node_id, position);

CREATE TABLE IF NOT EXISTS path_position_transition_priors (
  network       TEXT        NOT NULL,
  position      SMALLINT    NOT NULL CHECK (position >= 0),
  from_node_id  TEXT        NOT NULL,
  to_node_id    TEXT        NOT NULL,
  count         INTEGER     NOT NULL CHECK (count > 0),
  probability   DOUBLE PRECISION NOT NULL CHECK (probability >= 0 AND probability <= 1),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (network, position, from_node_id, to_node_id)
);
CREATE INDEX IF NOT EXISTS path_position_transition_priors_lookup_idx
  ON path_position_transition_priors (network, position, from_node_id);
