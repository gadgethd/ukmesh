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
         LEFT JOIN (
           SELECT DISTINCT ON (COALESCE(a.canonical_node_id, upper(btrim(n.node_id))))
             COALESCE(a.canonical_node_id, upper(btrim(n.node_id))) AS node_id, n.name
           FROM nodes n
           LEFT JOIN node_identity_aliases a ON a.source_node_id = upper(btrim(n.node_id))
           ORDER BY COALESCE(a.canonical_node_id, upper(btrim(n.node_id))),
             CASE
               WHEN n.role = 2 AND n.lat BETWEEN -90 AND 90 AND n.lon BETWEEN -180 AND 180
                 AND NOT (ABS(n.lat) < 1e-9 AND ABS(n.lon) < 1e-9) THEN 0
               WHEN n.role = 2 THEN 1
               WHEN COALESCE(n.advert_count, 0) > 0 THEN 2
               ELSE 3
             END,
             COALESCE(n.advert_count, 0) DESC, n.last_seen DESC NULLS LAST, n.node_id
         ) n ON n.node_id = nss.node_id
         LEFT JOIN (
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
           GROUP BY meshcore_canonical_node_id(rx_node_id)
         ) pc ON pc.rx_node_id = nss.node_id
         WHERE nss.time > NOW() - INTERVAL '15 minutes'
           AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
           AND nss.network = ANY(ARRAY['ukmesh','northeast','teesside']::text[])
         ORDER BY nss.node_id, COALESCE(nss.uptime_secs, 0) DESC, nss.time DESC;
