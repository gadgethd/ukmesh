BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SELECT format($readpath$EXPLAIN (ANALYZE, BUFFERS, SETTINGS, SUMMARY, TIMING OFF)
SELECT DISTINCT ON (nss.node_id)
           nss.node_id,
           n.name,
           nss.time AS last_seen,
           nss.battery_mv,
           nss.uptime_secs,
           nss.channel_utilization,
           nss.air_util_tx,
           nss.rx_air_secs,
           nss.tx_air_secs,
           COALESCE(pc.packet_count, 0) AS packets_24h
         FROM node_identity_status_samples nss
         LEFT JOIN node_identity_nodes n ON n.node_id = nss.node_id
         LEFT JOIN (SELECT * FROM (VALUES %s) AS frozen(rx_node_id, packet_count)
         ) pc ON pc.rx_node_id = nss.node_id
         WHERE nss.time > NOW() - INTERVAL '15 minutes'
           AND (n.name IS NULL OR n.name NOT LIKE '%%🚫%%')
           AND nss.network = ANY(ARRAY['ukmesh','northeast','teesside']::text[])
         ORDER BY nss.node_id, COALESCE(nss.uptime_secs, 0) DESC, nss.time DESC;$readpath$, string_agg(format('(%L::text,%L::bigint)', rx_node_id, packet_count), ','))
FROM (
           SELECT meshcore_canonical_node_id(rx_node_id) AS rx_node_id,
                  SUM(packet_count)::bigint AS packet_count
           FROM (
             SELECT rx_node_id, COUNT(*) AS packet_count
             FROM packets
             WHERE time > NOW() - INTERVAL '24 hours'
               AND rx_node_id IS NOT NULL
               AND network = ANY(ARRAY['ukmesh','northeast','teesside']::text[])
             GROUP BY rx_node_id
           ) raw_counts
           GROUP BY meshcore_canonical_node_id(rx_node_id)) counts
\gexec
ROLLBACK;
