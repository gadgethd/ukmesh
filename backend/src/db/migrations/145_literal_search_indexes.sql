-- Candidate selection uses escaped literal LIKE, then rechecks the original
-- substring/history predicate against the complete canonical identity.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;
CREATE INDEX nodes_lookup_name_trgm_idx ON core.nodes USING gin
  (LOWER(COALESCE(name,'')) public.gin_trgm_ops);
CREATE INDEX nodes_lookup_id_trgm_idx ON core.nodes USING gin
  (LOWER(UPPER(BTRIM(node_id))) public.gin_trgm_ops);
CREATE INDEX nodes_lookup_iata_trgm_idx ON core.nodes USING gin
  (LOWER(COALESCE(iata,'')) public.gin_trgm_ops);
CREATE INDEX node_name_history_lookup_trgm_idx ON core.node_name_history USING gin
  (LOWER(name) public.gin_trgm_ops);
ANALYZE core.nodes;
ANALYZE core.node_name_history;
