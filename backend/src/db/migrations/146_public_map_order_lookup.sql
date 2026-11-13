-- Bound page reads before fetching complete singleton rows. Aliases and
-- normalization collisions still use their complete canonical projection.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
CREATE INDEX nodes_public_map_order_idx ON core.nodes (UPPER(BTRIM(node_id)))
  INCLUDE (node_id)
  WHERE lat BETWEEN -90 AND 90
    AND lon BETWEEN -180 AND 180
    AND NOT (ABS(lat) < 5 AND ABS(lon) < 5)
    AND (name IS NULL OR name NOT LIKE '%🚫%')
    AND (role IS NULL OR role NOT IN (1, 3));
ANALYZE core.nodes;
