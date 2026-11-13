-- The final collision probe visits only legacy keys; bulk index-only
-- normalization scans are no longer needed. Public pages fetch complete rows.
-- Keep both ordered keys, without storing a second copy of every node ID.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
DROP INDEX core.nodes_normalized_lookup_idx;
CREATE INDEX nodes_normalized_lookup_idx ON core.nodes (UPPER(BTRIM(node_id)));
DROP INDEX core.nodes_public_map_order_idx;
CREATE INDEX nodes_public_map_order_idx ON core.nodes (UPPER(BTRIM(node_id)))
  WHERE lat BETWEEN -90 AND 90
    AND lon BETWEEN -180 AND 180
    AND NOT (ABS(lat) < 5 AND ABS(lon) < 5)
    AND (name IS NULL OR name NOT LIKE '%🚫%')
    AND (role IS NULL OR role NOT IN (1, 3));

-- GIN supports predicates on any of its columns. One pending-list write
-- serves all three literal-search fields instead of maintaining three trees.
CREATE INDEX nodes_lookup_text_trgm_idx ON core.nodes USING gin
  (LOWER(COALESCE(name,'')) public.gin_trgm_ops,
   LOWER(UPPER(BTRIM(node_id))) public.gin_trgm_ops,
   LOWER(COALESCE(iata,'')) public.gin_trgm_ops);
DROP INDEX core.nodes_lookup_name_trgm_idx;
DROP INDEX core.nodes_lookup_id_trgm_idx;
DROP INDEX core.nodes_lookup_iata_trgm_idx;
ANALYZE core.nodes;
