/** Keep the existing three-day activity and test-observer exclusion semantics
 * while deriving all per-observer timestamps in one packets scan. */
export const INGEST_HEALTH_SQL = `WITH latest_rx AS (
  SELECT rx_node_id,
         MAX(time) FILTER (
           WHERE network IS DISTINCT FROM 'test'
             AND split_part(topic, '/', 1) <> 'meshcore-test'
         ) AS last_packet_at,
         MAX(time) AS latest_at,
         MAX(time) FILTER (WHERE network = 'test') AS latest_test_at
    FROM packets
   WHERE time > NOW() - INTERVAL '3 days'
     AND rx_node_id IS NOT NULL AND rx_node_id <> ''
   GROUP BY rx_node_id
), active_rx AS (
  SELECT rx_node_id, last_packet_at
    FROM latest_rx
   WHERE last_packet_at > NOW() - INTERVAL '3 days'
     AND latest_at IS DISTINCT FROM latest_test_at
)
SELECT
  COUNT(*) FILTER (WHERE last_packet_at < NOW() - INTERVAL '15 minutes')::text AS stale_nodes,
  COUNT(*)::text AS active_nodes,
  MAX(CASE WHEN last_packet_at < NOW() - INTERVAL '15 minutes'
    THEN FLOOR(EXTRACT(EPOCH FROM (NOW() - last_packet_at)) / 60)
    ELSE NULL END)::text AS max_stale_minutes,
  '15'::text AS stale_threshold_minutes,
  (SELECT MAX(time)::text FROM packets
    WHERE network IS DISTINCT FROM 'test'
      AND split_part(topic, '/', 1) <> 'meshcore-test') AS global_last_packet_at
FROM active_rx`;

/** Count actual hash widths, including malformed/mixed-width historical paths.
 * One array traversal supplies both hop counts and packet-level diagnostics;
 * do not infer these diagnostics from the advertised path_hash_size_bytes. */
export const PATH_HASH_HEALTH_SQL = `SELECT
  COALESCE(SUM(hops.one_byte), 0)::text AS one_byte,
  COALESCE(SUM(hops.two_byte), 0)::text AS two_byte,
  COALESCE(SUM(hops.three_byte), 0)::text AS three_byte,
  MAX(p.time) FILTER (WHERE hops.multibyte)::text AS latest_multibyte_at,
  COUNT(*) FILTER (WHERE hops.multibyte)::text AS multibyte_packets_24h
FROM packets p
CROSS JOIN LATERAL (
  SELECT COUNT(*) FILTER (WHERE length(h) = 2) AS one_byte,
         COUNT(*) FILTER (WHERE length(h) = 4) AS two_byte,
         COUNT(*) FILTER (WHERE length(h) = 6) AS three_byte,
         BOOL_OR(length(h) > 2) AS multibyte
    FROM unnest(p.path_hashes) AS h
) hops
WHERE p.time > NOW() - INTERVAL '24 hours'
  AND p.network IS DISTINCT FROM 'test'`;
