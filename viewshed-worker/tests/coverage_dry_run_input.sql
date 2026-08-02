-- Read-only input stream for dry_run_coverage.py. Run psql with -At so each
-- returned value is passed through as one tab-separated protocol line.
WITH eligible AS (
  SELECT n.node_id, n.lat, n.lon
  FROM nodes n
  WHERE n.lat BETWEEN 49.5 AND 61.5
    AND n.lon BETWEEN -8.5 AND 2.5
    AND n.lat IS NOT NULL
    AND n.lon IS NOT NULL
    AND NOT (ABS(n.lat) < 1e-9 AND ABS(n.lon) < 1e-9)
    AND (n.role IS NULL OR n.role = 2)
    AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
), input_lines AS (
  SELECT 0 AS section, e.lat AS sort_lat, e.node_id AS sort_id,
         concat_ws(E'\t', 'N', e.node_id, e.lat::text, e.lon::text) AS line
  FROM eligible e
  UNION ALL
  SELECT 1 AS section, 0::double precision AS sort_lat,
         nl.node_a_id || ':' || nl.node_b_id AS sort_id,
         concat_ws(
           E'\t', 'L', nl.node_a_id, nl.node_b_id,
           na.lat::text, na.lon::text, nb.lat::text, nb.lon::text
         ) AS line
  FROM node_links nl
  JOIN nodes na ON na.node_id = nl.node_a_id
  JOIN nodes nb ON nb.node_id = nl.node_b_id
  WHERE na.lat IS NOT NULL
    AND na.lon IS NOT NULL
    AND nb.lat IS NOT NULL
    AND nb.lon IS NOT NULL
    AND (nl.itm_viable = true OR nl.force_viable = true)
)
SELECT line FROM input_lines ORDER BY section, sort_lat, sort_id;
