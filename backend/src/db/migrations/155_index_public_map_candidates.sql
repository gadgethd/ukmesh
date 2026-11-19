-- Scope discovery can skip repeated receptions using a covering partial index
-- when packet privacy is fully materialized. The ordinary source
-- index remains usable for live rematerialization and older app releases.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
CREATE INDEX packets_observer_public_source_idx
  ON mesh.packets (rx_node_id, src_node_id, time DESC) INCLUDE (network)
  WHERE src_node_id IS NOT NULL AND visibility_ok IS TRUE AND is_private IS NOT TRUE;

-- Join normalized fields only for candidate discovery. A literal that spans
-- the separator is rejected by the API's exact per-field identity predicate.
CREATE FUNCTION core.node_literal_search_text(node_name text, raw_id text, node_iata text)
RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
RETURN LOWER(COALESCE(node_name, '')) || chr(31)
  || LOWER(UPPER(BTRIM(raw_id))) || chr(31) || LOWER(COALESCE(node_iata, ''));

CREATE INDEX nodes_literal_search_idx ON core.nodes USING gin
  (core.node_literal_search_text(name, node_id, iata) public.gin_trgm_ops);
