import type { QueryResultRow } from 'pg';
import type { NetworkFilters } from '../api/utils/networkFilters.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type StatsRepositoryDeps = {
  networkFilters: (network?: string, observer?: string) => NetworkFilters;
  query: QueryFn;
};

export type StatsRepository = ReturnType<typeof createStatsRepository>;

export function createStatsRepository(deps: StatsRepositoryDeps) {
  const { networkFilters, query } = deps;

  async function fetchObserverRegionSummary(network: string | undefined, observer: string | undefined) {
    // Public aggregates are computed from privacy-filtered source rows. Legacy
    // rollups predate visibility state and cannot safely be filtered afterward.
    const filters = networkFilters(network, observer);
    return query(`
      SELECT
        COALESCE(NULLIF(TRIM(UPPER(split_part(p.topic, '/', 2))), ''), 'UNK') AS iata,
        COUNT(DISTINCT p.packet_hash) FILTER (WHERE p.time > NOW() - INTERVAL '24 hours') AS packets_24h,
        COUNT(DISTINCT p.packet_hash) AS packets_7d,
        COUNT(DISTINCT p.rx_node_id) FILTER (WHERE p.time > NOW() - INTERVAL '1 minute') AS active_observers,
        COUNT(DISTINCT p.rx_node_id) AS observers,
        MAX(p.time)::text AS last_packet_at
      FROM packets p
      WHERE p.time > NOW() - INTERVAL '7 days'
        AND p.rx_node_id IS NOT NULL
        AND p.rx_node_id <> ''
        AND p.rx_node_id ~ '^[0-9A-Fa-f]{64}$'
        ${filters.packetsAlias('p')}
      GROUP BY 1
      ORDER BY packets_7d DESC, iata ASC
    `, filters.params);
  }

  async function fetchChannelTraffic(network: string | undefined, observer: string | undefined) {
    const filters = networkFilters(network, observer);
    return query<{ channel: string; count: string; total_count: string }>(`
      WITH decoded_group_packets AS (
        SELECT
          COALESCE(p.payload->>'_summary', '') AS summary,
          NULLIF(TRIM((regexp_match(COALESCE(p.payload->>'_summary', ''), '^\\[([^\\]]+)\\]'))[1]), '') AS parsed_channel
        FROM packets p
        WHERE p.time > NOW() - INTERVAL '24 hours'
          AND p.packet_type = 5
          ${filters.packetsAlias('p')}
      ),
      group_packets AS (
        SELECT
          COALESCE(
            CASE
              WHEN parsed_channel IS NOT NULL
                AND LOWER(parsed_channel) NOT IN ('encrypted', 'unknown')
                THEN parsed_channel
            END,
            CASE
              WHEN summary ILIKE '%encrypted%' THEN 'Encrypted'
              ELSE 'Unknown'
            END
          ) AS channel
        FROM decoded_group_packets
      ),
      channel_counts AS (
        SELECT channel, COUNT(*)::text AS count
        FROM group_packets
        GROUP BY channel
      )
      SELECT channel, count, SUM(count::bigint) OVER ()::text AS total_count
      FROM channel_counts
      ORDER BY count::bigint DESC, channel ASC
      LIMIT 12
    `, filters.params);
  }

  async function fetchChartsData(network: string | undefined, observer: string | undefined) {
    const filters = networkFilters(network, observer);

    const [
      phResult, pdResult, rhResult, rdResult,
      ptResult, hdResult, pcResult, sumResult, orSummaryResult, orSeriesResult,
      pathHashWidthsResult, multibyteSummaryResult, observerDiversityResult, signalSummaryResult,
      routeTypesResult, transportCodesResult, pathDecodeTrendResult,
    ] = await Promise.all([
      query(`
        WITH buckets AS (
          SELECT generate_series(
            date_trunc('hour', NOW() - INTERVAL '24 hours'),
            date_trunc('hour', NOW()),
            INTERVAL '1 hour'
          ) AS bucket
        ),
        counts AS (
          SELECT time_bucket('1 hour', p.time) AS bucket, COUNT(*)::int AS count
          FROM packets p
          WHERE p.time > NOW() - INTERVAL '24 hours'
            ${filters.packetsAlias('p')}
          GROUP BY 1
        )
        SELECT b.bucket AS hour, COALESCE(c.count, 0) AS count
        FROM buckets b
        LEFT JOIN counts c ON c.bucket = b.bucket
        ORDER BY b.bucket
      `, filters.params),
      query(`
        SELECT time_bucket('1 day', time) AS day, COUNT(*) AS count
        FROM packets
        WHERE time > NOW() - INTERVAL '7 days' ${filters.packets}
        GROUP BY day ORDER BY day
      `, filters.params),
      query(`
        WITH buckets AS (
          SELECT generate_series(
            date_trunc('hour', NOW() - INTERVAL '24 hours'),
            date_trunc('hour', NOW()),
            INTERVAL '1 hour'
          ) AS bucket
        ),
        counts AS (
          SELECT time_bucket('1 hour', p.time) AS bucket, COUNT(DISTINCT p.src_node_id)::int AS count
          FROM packets p
          WHERE p.time > NOW() - INTERVAL '24 hours'
            AND p.src_node_id IS NOT NULL
            ${filters.packetsAlias('p')}
          GROUP BY 1
        )
        SELECT b.bucket AS hour, COALESCE(c.count, 0) AS count
        FROM buckets b
        LEFT JOIN counts c ON c.bucket = b.bucket
        ORDER BY b.bucket
      `, filters.params),
      query(`
        SELECT time_bucket('1 day', time) AS day, COUNT(DISTINCT src_node_id) AS count
        FROM packets
        WHERE time > NOW() - INTERVAL '7 days' AND src_node_id IS NOT NULL ${filters.packets}
        GROUP BY day ORDER BY day
      `, filters.params),
      query(`
        SELECT packet_type, COUNT(*) AS count
        FROM packets
        WHERE time > NOW() - INTERVAL '24 hours' ${filters.packets}
        GROUP BY packet_type ORDER BY count DESC
      `, filters.params),
      query(`
        SELECT hop_count AS hops, COUNT(*) AS count
        FROM packets
        WHERE time > NOW() - INTERVAL '7 days'
          AND hop_count IS NOT NULL
          ${filters.packets}
        GROUP BY hop_count ORDER BY hop_count
      `, filters.params),
      query(`
        WITH prefix_counts AS (
          SELECT UPPER(h) AS prefix, COUNT(*)::int AS node_count
          FROM (
            SELECT p.path_hashes
            FROM packets p
            WHERE p.time > NOW() - INTERVAL '24 hours'
              AND COALESCE(array_length(p.path_hashes, 1), 0) > 0
              ${filters.packetsAlias('p')}
          ) p
          CROSS JOIN LATERAL unnest(p.path_hashes) AS h
          WHERE NULLIF(TRIM(h), '') IS NOT NULL
          GROUP BY 1
          HAVING COUNT(*) > 1
        )
        SELECT prefix, node_count AS repeats
        FROM prefix_counts
        ORDER BY node_count DESC, prefix ASC
        LIMIT 10
      `, filters.params),
      query(`
        SELECT
          (SELECT COUNT(*) FROM packets WHERE time > NOW() - INTERVAL '24 hours' ${filters.packets}) AS total_24h,
          (SELECT COUNT(*) FROM packets WHERE time > NOW() - INTERVAL '7 days' ${filters.packets}) AS total_7d,
          (SELECT COUNT(DISTINCT src_node_id) FROM packets WHERE time > NOW() - INTERVAL '24 hours' AND src_node_id IS NOT NULL ${filters.packets}) AS unique_radios_24h
      `, filters.params),
      fetchObserverRegionSummary(network, observer),
      query(`
        SELECT
          COALESCE(NULLIF(TRIM(UPPER(split_part(p.topic, '/', 2))), ''), 'UNK') AS iata,
          time_bucket('1 day', p.time) AS day,
          COUNT(DISTINCT p.packet_hash) AS count
        FROM packets p
        WHERE p.time > NOW() - INTERVAL '7 days'
          AND p.rx_node_id IS NOT NULL
          AND p.rx_node_id <> ''
          AND p.rx_node_id ~ '^[0-9A-Fa-f]{64}$'
          ${filters.packetsAlias('p')}
        GROUP BY 1, 2
        ORDER BY iata ASC, day ASC
      `, filters.params),
      query<{ hash_hex_len: string; hop_count: string }>(
        `SELECT length(h)::text AS hash_hex_len, COUNT(*)::text AS hop_count
         FROM (
           SELECT p.path_hashes
           FROM packets p
           WHERE p.time > NOW() - INTERVAL '24 hours'
             AND COALESCE(array_length(p.path_hashes, 1), 0) > 0
             ${filters.packetsAlias('p')}
         ) p
         CROSS JOIN LATERAL unnest(p.path_hashes) AS h
         GROUP BY 1`,
        filters.params,
      ),
      query<{
        latest_multibyte_at: string | null;
        latest_multibyte_hash: string | null;
        multibyte_packets_24h: string;
        fully_decoded_multibyte_24h: string;
        latest_fully_decoded_at: string | null;
        latest_fully_decoded_hash: string | null;
        latest_fully_decoded_hops: string | null;
        latest_fully_decoded_path: string | null;
        latest_fully_decoded_nodes: Array<{ ord: number; node_id: string; name: string | null; lat: number | null; lon: number | null; last_seen: string | null; }> | null;
        longest_fully_decoded_at: string | null;
        longest_fully_decoded_hash: string | null;
        longest_fully_decoded_hops: string | null;
        longest_fully_decoded_path: string | null;
        longest_fully_decoded_nodes: Array<{ ord: number; node_id: string; name: string | null; lat: number | null; lon: number | null; last_seen: string | null; }> | null;
      }>(
        `WITH multibyte AS (
           SELECT
             row_number() OVER () AS obs_id,
             p.packet_hash,
             p.network,
             p.time,
             p.rx_node_id,
             p.hop_count,
             p.path_hash_size_bytes,
             p.path_hashes,
             rx.role AS rx_role
           FROM packets p
           LEFT JOIN nodes rx ON rx.node_id = p.rx_node_id
           WHERE p.time > NOW() - INTERVAL '24 hours'
             AND p.path_hash_size_bytes > 1
             AND COALESCE(array_length(p.path_hashes, 1), 0) > 0
             ${filters.packetsAlias('p')}
         ),
         prepared AS (
           SELECT m.*,
             CASE
               WHEN m.hop_count IS NOT NULL THEN (
                 SELECT COALESCE(array_agg(UPPER(h) ORDER BY ord), ARRAY[]::text[])
                 FROM unnest(m.path_hashes) WITH ORDINALITY u(h, ord)
                 WHERE LENGTH(h) = m.path_hash_size_bytes * 2
                   AND ord <= GREATEST(m.hop_count, 0)
               )
               ELSE (
                 SELECT COALESCE(array_agg(UPPER(h) ORDER BY ord), ARRAY[]::text[])
                 FROM unnest(m.path_hashes) WITH ORDINALITY u(h, ord)
                 WHERE LENGTH(h) = m.path_hash_size_bytes * 2
               )
             END AS valid_hops
           FROM multibyte m
         ),
         trimmed AS (
           SELECT p.*,
             CASE
               WHEN p.rx_role = 2
                 AND CARDINALITY(p.valid_hops) > 1
                 AND UPPER(p.rx_node_id) LIKE p.valid_hops[CARDINALITY(p.valid_hops)] || '%'
               THEN p.valid_hops[1:CARDINALITY(p.valid_hops) - 1]
               ELSE p.valid_hops
             END AS hops
           FROM prepared p
         ),
         distinct_hashes AS (
           -- Distinct on hash only. node_prefixes ignores network, so keeping
           -- network here fans the JOIN out by the number of networks a hash
           -- was seen on (ukmesh scope = ukmesh+northeast), multiplying
           -- match_count and breaking BOOL_AND(match_count = 1) for any hop
           -- whose hash appears on >1 network — which collapsed the decode rate.
           SELECT DISTINCT hash
           FROM trimmed
           CROSS JOIN LATERAL unnest(hops) h(hash)
           WHERE hash IS NOT NULL
         ),
         node_prefixes AS (
           SELECT
             UPPER(LEFT(n.node_id, 4)) AS hash,
             COUNT(*)::int AS match_count,
             MIN(n.node_id) AS node_id
           FROM nodes n
           JOIN distinct_hashes dh
             ON LENGTH(dh.hash) = 4
            AND dh.hash = UPPER(LEFT(n.node_id, 4))
           WHERE n.lat IS NOT NULL
             AND n.lon IS NOT NULL
             AND (n.role IS NULL OR n.role = 2)
           GROUP BY UPPER(LEFT(n.node_id, 4))
           UNION ALL
           SELECT
             UPPER(LEFT(n.node_id, 6)) AS hash,
             COUNT(*)::int AS match_count,
             MIN(n.node_id) AS node_id
           FROM nodes n
           JOIN distinct_hashes dh
             ON LENGTH(dh.hash) = 6
            AND dh.hash = UPPER(LEFT(n.node_id, 6))
           WHERE n.lat IS NOT NULL
             AND n.lon IS NOT NULL
             AND (n.role IS NULL OR n.role = 2)
           GROUP BY UPPER(LEFT(n.node_id, 6))
         ),
         hop_eval AS (
           SELECT
             t.obs_id,
             t.packet_hash,
             t.network,
             t.time,
             h.ord,
             h.hash,
             COALESCE(np.match_count, 0) AS match_count,
             np.node_id
           FROM trimmed t
           CROSS JOIN LATERAL unnest(t.hops) WITH ORDINALITY h(hash, ord)
           LEFT JOIN node_prefixes np ON np.hash = h.hash
         ),
         fully_decoded AS (
           SELECT
             t.obs_id,
             t.packet_hash,
             t.network,
             t.time,
             CARDINALITY(t.hops)::int AS decoded_hops,
             string_agg(UPPER(LEFT(he.node_id, 6)), ' -> ' ORDER BY he.ord) AS decoded_path
           FROM trimmed t
           JOIN hop_eval he ON he.obs_id = t.obs_id
           GROUP BY t.obs_id, t.packet_hash, t.network, t.time, t.hops
           HAVING CARDINALITY(t.hops) >= 2
              AND COUNT(he.ord) = CARDINALITY(t.hops)
              AND BOOL_AND(he.match_count = 1)
              AND COUNT(DISTINCT he.node_id) FILTER (WHERE he.match_count = 1) = CARDINALITY(t.hops)
         ),
         decoded_hops AS (
           SELECT he.*
           FROM hop_eval he
           JOIN fully_decoded fd ON fd.obs_id = he.obs_id
         ),
         latest_fully_decoded AS (
           SELECT *
           FROM fully_decoded
           ORDER BY time DESC, packet_hash DESC, obs_id DESC
           LIMIT 1
         ),
         longest_fully_decoded AS (
           SELECT *
           FROM fully_decoded
           ORDER BY decoded_hops DESC, time DESC, packet_hash DESC, obs_id DESC
           LIMIT 1
         )
         SELECT
           (SELECT MAX(time)::text FROM multibyte) AS latest_multibyte_at,
           (SELECT packet_hash FROM multibyte ORDER BY time DESC, packet_hash DESC LIMIT 1) AS latest_multibyte_hash,
           (SELECT COUNT(*)::text FROM multibyte) AS multibyte_packets_24h,
           (SELECT COUNT(*)::text FROM fully_decoded) AS fully_decoded_multibyte_24h,
           (SELECT time::text FROM latest_fully_decoded) AS latest_fully_decoded_at,
           (SELECT packet_hash FROM latest_fully_decoded) AS latest_fully_decoded_hash,
           (SELECT decoded_hops::text FROM latest_fully_decoded) AS latest_fully_decoded_hops,
           (SELECT decoded_path FROM latest_fully_decoded) AS latest_fully_decoded_path,
           (
             SELECT jsonb_agg(jsonb_build_object(
               'ord', dh.ord,
               'node_id', dh.node_id,
               'name', n.name,
               'lat', n.lat,
               'lon', n.lon,
               'last_seen', n.last_seen
             ) ORDER BY dh.ord)
             FROM latest_fully_decoded l
             JOIN decoded_hops dh ON dh.obs_id = l.obs_id
             LEFT JOIN nodes n ON n.node_id = dh.node_id
           ) AS latest_fully_decoded_nodes,
           (SELECT time::text FROM longest_fully_decoded) AS longest_fully_decoded_at,
           (SELECT packet_hash FROM longest_fully_decoded) AS longest_fully_decoded_hash,
           (SELECT decoded_hops::text FROM longest_fully_decoded) AS longest_fully_decoded_hops,
           (SELECT decoded_path FROM longest_fully_decoded) AS longest_fully_decoded_path,
           (
             SELECT jsonb_agg(jsonb_build_object(
               'ord', dh.ord,
               'node_id', dh.node_id,
               'name', n.name,
               'lat', n.lat,
               'lon', n.lon,
               'last_seen', n.last_seen
             ) ORDER BY dh.ord)
             FROM longest_fully_decoded l
             JOIN decoded_hops dh ON dh.obs_id = l.obs_id
             LEFT JOIN nodes n ON n.node_id = dh.node_id
           ) AS longest_fully_decoded_nodes`,
        filters.params,
      ),
      query<{
        avg_observers: string | null;
        max_observers: string | null;
        total_packets: string;
        single_observer_packets: string;
      }>(
        `WITH per_packet AS (
           SELECT packet_hash, COUNT(DISTINCT rx_node_id)::int AS observer_count
           FROM (
             SELECT p.packet_hash, p.rx_node_id
           FROM packets p
           WHERE p.time > NOW() - INTERVAL '24 hours'
             AND p.packet_hash IS NOT NULL
             AND p.rx_node_id IS NOT NULL
             ${filters.packetsAlias('p')}
           ) recent
           GROUP BY packet_hash
         )
         SELECT
           AVG(observer_count)::text AS avg_observers,
           MAX(observer_count)::text AS max_observers,
           COUNT(*)::text AS total_packets,
           COUNT(*) FILTER (WHERE observer_count = 1)::text AS single_observer_packets
         FROM per_packet`,
        filters.params,
      ),
      query<{
        avg_rssi: string | null;
        median_rssi: string | null;
        avg_snr: string | null;
        median_snr: string | null;
        rssi_samples: string;
        snr_samples: string;
      }>(
        `SELECT
           AVG(p.rssi)::text AS avg_rssi,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY p.rssi)::text AS median_rssi,
           AVG(p.snr)::text AS avg_snr,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY p.snr)::text AS median_snr,
           COUNT(p.rssi)::text AS rssi_samples,
           COUNT(p.snr)::text AS snr_samples
         FROM packets p
         WHERE p.time > NOW() - INTERVAL '24 hours'
           AND (p.rssi IS NOT NULL OR p.snr IS NOT NULL)
           ${filters.packetsAlias('p')}`,
        filters.params,
      ),
      query<{ route_type: string; count: string }>(
        `SELECT COALESCE(p.route_type::text, 'Unknown') AS route_type, COUNT(*)::text AS count
         FROM packets p
         WHERE p.time > NOW() - INTERVAL '24 hours'
           ${filters.packetsAlias('p')}
         GROUP BY p.route_type
         ORDER BY COUNT(*) DESC, route_type ASC`,
        filters.params,
      ),
      query<{ transport_code: string; region_scope: string | null; count: string }>(
        `SELECT
           NULLIF(TRIM(p.transport_codes), '') AS transport_code,
           NULLIF(TRIM(p.region_scope), '') AS region_scope,
           COUNT(*)::text AS count
         FROM packets p
         WHERE p.time > NOW() - INTERVAL '24 hours'
           AND NULLIF(TRIM(p.transport_codes), '') IS NOT NULL
           ${filters.packetsAlias('p')}
         GROUP BY 1, 2
         ORDER BY COUNT(*) DESC, region_scope ASC, transport_code ASC
         LIMIT 12`,
        filters.params,
      ),
      query<{
        day: string;
        multibyte_count: string;
        fully_decoded_count: string;
      }>(
        `WITH buckets AS (
           SELECT generate_series(
             date_trunc('day', NOW() - INTERVAL '7 days'),
             date_trunc('day', NOW()),
             INTERVAL '1 day'
           ) AS day
         ),
         multibyte AS (
           SELECT
             row_number() OVER () AS obs_id,
             p.packet_hash,
             p.network,
             time_bucket('1 day', p.time) AS day,
             p.rx_node_id,
             p.hop_count,
             p.path_hash_size_bytes,
             p.path_hashes,
             rx.role AS rx_role
           FROM packets p
           LEFT JOIN nodes rx ON rx.node_id = p.rx_node_id
           WHERE p.time > NOW() - INTERVAL '7 days'
             AND p.path_hash_size_bytes > 1
             AND COALESCE(array_length(p.path_hashes, 1), 0) > 0
             ${filters.packetsAlias('p')}
         ),
         multibyte_counts AS (
           SELECT day, COUNT(*)::text AS count
           FROM multibyte
           GROUP BY day
         ),
         prepared AS (
           SELECT m.*,
             CASE
               WHEN m.hop_count IS NOT NULL THEN (
                 SELECT COALESCE(array_agg(UPPER(h) ORDER BY ord), ARRAY[]::text[])
                 FROM unnest(m.path_hashes) WITH ORDINALITY u(h, ord)
                 WHERE LENGTH(h) = m.path_hash_size_bytes * 2
                   AND ord <= GREATEST(m.hop_count, 0)
               )
               ELSE (
                 SELECT COALESCE(array_agg(UPPER(h) ORDER BY ord), ARRAY[]::text[])
                 FROM unnest(m.path_hashes) WITH ORDINALITY u(h, ord)
                 WHERE LENGTH(h) = m.path_hash_size_bytes * 2
               )
             END AS valid_hops
           FROM multibyte m
         ),
         trimmed AS (
           SELECT p.*,
             CASE
               WHEN p.rx_role = 2
                 AND CARDINALITY(p.valid_hops) > 1
                 AND UPPER(p.rx_node_id) LIKE p.valid_hops[CARDINALITY(p.valid_hops)] || '%'
               THEN p.valid_hops[1:CARDINALITY(p.valid_hops) - 1]
               ELSE p.valid_hops
             END AS hops
           FROM prepared p
         ),
         distinct_hashes AS (
           -- Distinct on hash only. node_prefixes ignores network, so keeping
           -- network here fans the JOIN out by the number of networks a hash
           -- was seen on (ukmesh scope = ukmesh+northeast), multiplying
           -- match_count and breaking BOOL_AND(match_count = 1) for any hop
           -- whose hash appears on >1 network — which collapsed the decode rate.
           SELECT DISTINCT hash
           FROM trimmed
           CROSS JOIN LATERAL unnest(hops) h(hash)
           WHERE hash IS NOT NULL
         ),
         node_prefixes AS (
           SELECT
             UPPER(LEFT(n.node_id, 4)) AS hash,
             COUNT(*)::int AS match_count,
             MIN(n.node_id) AS node_id
           FROM nodes n
           JOIN distinct_hashes dh
             ON LENGTH(dh.hash) = 4
            AND dh.hash = UPPER(LEFT(n.node_id, 4))
           WHERE n.lat IS NOT NULL
             AND n.lon IS NOT NULL
             AND (n.role IS NULL OR n.role = 2)
           GROUP BY UPPER(LEFT(n.node_id, 4))
           UNION ALL
           SELECT
             UPPER(LEFT(n.node_id, 6)) AS hash,
             COUNT(*)::int AS match_count,
             MIN(n.node_id) AS node_id
           FROM nodes n
           JOIN distinct_hashes dh
             ON LENGTH(dh.hash) = 6
            AND dh.hash = UPPER(LEFT(n.node_id, 6))
           WHERE n.lat IS NOT NULL
             AND n.lon IS NOT NULL
             AND (n.role IS NULL OR n.role = 2)
           GROUP BY UPPER(LEFT(n.node_id, 6))
         ),
         hop_eval AS (
           SELECT
             t.obs_id,
             h.ord,
             COALESCE(np.match_count, 0) AS match_count,
             np.node_id
           FROM trimmed t
           CROSS JOIN LATERAL unnest(t.hops) WITH ORDINALITY h(hash, ord)
           LEFT JOIN node_prefixes np ON np.hash = h.hash
         ),
         fully_decoded AS (
           SELECT m.day, COUNT(*)::text AS count
           FROM trimmed m
           JOIN hop_eval he ON he.obs_id = m.obs_id
           GROUP BY m.day, m.obs_id, m.hops
           HAVING CARDINALITY(m.hops) >= 2
              AND COUNT(he.ord) = CARDINALITY(m.hops)
              AND BOOL_AND(he.match_count = 1)
              AND COUNT(DISTINCT he.node_id) FILTER (WHERE he.match_count = 1) = CARDINALITY(m.hops)
         ),
         fully_decoded_counts AS (
           SELECT day, COUNT(*)::text AS count
           FROM fully_decoded
           GROUP BY day
         )
         SELECT
           b.day::text,
           COALESCE(MAX(m.count), '0') AS multibyte_count,
           COALESCE(MAX(f.count), '0') AS fully_decoded_count
         FROM buckets b
         LEFT JOIN multibyte_counts m ON m.day = b.day
         LEFT JOIN fully_decoded_counts f ON f.day = b.day
         GROUP BY b.day
         ORDER BY b.day`,
        filters.params,
      ),
    ]);

    return {
      phResult,
      pdResult,
      rhResult,
      rdResult,
      ptResult,
      hdResult,
      pcResult,
      sumResult,
      orSummaryResult,
      orSeriesResult,
      pathHashWidthsResult,
      multibyteSummaryResult,
      observerDiversityResult,
      signalSummaryResult,
      routeTypesResult,
      transportCodesResult,
      pathDecodeTrendResult,
    };
  }

  async function fetchStatsSummary(network: string | undefined, observer: string | undefined) {
    const filters = networkFilters(network, observer);
    const longestHopResult = () => query(`SELECT hop_count AS count, packet_hash AS hash
           FROM packets
           WHERE hop_count IS NOT NULL
             AND time > NOW() - INTERVAL '30 days'
             ${filters.packets}
           ORDER BY hop_count DESC LIMIT 1`, filters.params);

    const [mqttCount, packetCount, staleCount, mapNodeCount, totalNodeCount, longestHopCount, nodesDayCount, internationalCount] = await Promise.all([
      network != null
        ? query(`SELECT COUNT(DISTINCT rx_node_id) AS count
                 FROM packets
                 WHERE time > NOW() - INTERVAL '10 minutes'
                   AND rx_node_id IS NOT NULL
                   ${filters.packets}`, filters.params)
        : query(`
          WITH test_active AS (
            SELECT rx_node_id FROM packets WHERE rx_node_id IS NOT NULL AND rx_node_id <> ''
              AND time > NOW() - INTERVAL '7 days'
            GROUP BY rx_node_id HAVING MAX(time) = MAX(time) FILTER (WHERE network = 'test')
          )
          SELECT COUNT(DISTINCT rx_node_id) AS count
          FROM packets
          WHERE time > NOW() - INTERVAL '10 minutes'
            AND rx_node_id IS NOT NULL
            AND rx_node_id NOT IN (SELECT rx_node_id FROM test_active)
            ${filters.packets}
        `, filters.params),
      query(`SELECT COUNT(*) AS count FROM packets WHERE time > NOW() - INTERVAL '24 hours' ${filters.packets}`, filters.params),
      query(`SELECT COUNT(*) AS count FROM nodes
             WHERE lat BETWEEN -90 AND 90
               AND lon BETWEEN -180 AND 180
               AND NOT (ABS(lat) < 5 AND ABS(lon) < 5)
               AND (name IS NULL OR name NOT LIKE '%🚫%')
               AND (role IS NULL OR role NOT IN (1, 3))
               AND GREATEST(last_seen, last_path_evidence_at) <= NOW() - INTERVAL '14 days'
               AND GREATEST(last_seen, last_path_evidence_at) >  NOW() - INTERVAL '28 days'
               ${filters.nodes}`, filters.params),
      query(`SELECT COUNT(*) AS count FROM nodes
             WHERE lat BETWEEN -90 AND 90
               AND lon BETWEEN -180 AND 180
               AND NOT (ABS(lat) < 5 AND ABS(lon) < 5)
               AND (name IS NULL OR name NOT LIKE '%🚫%')
               AND (role IS NULL OR role NOT IN (1, 3))
               AND GREATEST(last_seen, last_path_evidence_at) > NOW() - INTERVAL '28 days'
               ${filters.nodes}`, filters.params),
      query(`SELECT COUNT(*) AS count FROM nodes
             WHERE (name IS NULL OR name NOT LIKE '%🚫%')
               AND (role IS NULL OR role != 4)
               ${filters.nodes}`, filters.params),
      longestHopResult(),
      query(`SELECT COUNT(DISTINCT src_node_id) AS count
             FROM packets
             WHERE time > NOW() - INTERVAL '24 hours'
               AND src_node_id IS NOT NULL
               ${filters.packets}`, filters.params),
      query(`WITH intl AS (
               SELECT lat, lon, last_seen, advert_count
               FROM nodes
               WHERE lat IS NOT NULL AND lon IS NOT NULL
                 AND lat != 0 AND lon != 0
                 AND last_seen > NOW() - INTERVAL '7 days'
                 AND (name IS NULL OR name NOT LIKE '%🚫%')
                 AND NOT (lat >= 49.8 AND lat <= 60.9 AND lon >= -8.7 AND lon <= 1.8)
                 AND (
                   (lat >= 50.75 AND lat <= 53.60 AND lon >= 3.35 AND lon <= 7.22) OR
                   (lat >= 49.50 AND lat <= 51.51 AND lon >= 2.54 AND lon <= 6.41) OR
                   (lat >= 47.27 AND lat <= 55.07 AND lon >= 5.87 AND lon <= 15.04) OR
                   (lat >= 54.56 AND lat <= 57.75 AND lon >= 8.07 AND lon <= 15.20) OR
                   (lat >= 51.44 AND lat <= 55.39 AND lon >= -10.48 AND lon <= -5.34) OR
                   (lat >= 41.33 AND lat <= 51.12 AND lon >= -5.14 AND lon <= 9.56)  OR
                   (lat >= 35.92 AND lat <= 43.79 AND lon >= -9.30 AND lon <= 4.29)  OR
                   (lat >= 36.62 AND lat <= 47.10 AND lon >= 6.61 AND lon <= 18.52)  OR
                   (lat >= 55.34 AND lat <= 69.06 AND lon >= 11.12 AND lon <= 24.17) OR
                   (lat >= 57.98 AND lat <= 71.19 AND lon >= 4.50 AND lon <= 31.10)  OR
                   (lat >= 59.81 AND lat <= 70.09 AND lon >= 19.09 AND lon <= 31.59) OR
                   (lat >= 46.37 AND lat <= 54.84 AND lon >= 14.12 AND lon <= 24.15)
                 )
                 ${filters.nodes}
             )
             SELECT
               COUNT(*) FILTER (WHERE last_seen > NOW() - INTERVAL '1 hour') AS count_connected,
               SUM(advert_count) AS total_adverts,
               MAX(last_seen)::text AS last_seen_at,
               (SELECT lat FROM intl ORDER BY last_seen DESC LIMIT 1) AS last_lat,
               (SELECT lon FROM intl ORDER BY last_seen DESC LIMIT 1) AS last_lon
             FROM intl`, filters.params),
    ]);

    return {
      mqttCount,
      packetCount,
      staleCount,
      mapNodeCount,
      totalNodeCount,
      longestHopCount,
      nodesDayCount,
      internationalCount,
    };
  }

  async function fetchObserverActivity(network: string | undefined) {
    const filters = networkFilters(network);
    return query<{ node_id: string; name: string | null; rx_24h: string; tx_24h: string; last_tx: string | null; last_rx: string | null }>(
      `SELECT
         n.node_id,
         n.name,
         COUNT(p.packet_hash) FILTER (WHERE p.rx_node_id  = n.node_id) AS rx_24h,
         COUNT(p.packet_hash) FILTER (WHERE p.src_node_id = n.node_id) AS tx_24h,
         MAX(p.time)          FILTER (WHERE p.src_node_id = n.node_id)::text AS last_tx,
         MAX(p.time)          FILTER (WHERE p.rx_node_id  = n.node_id)::text AS last_rx
       FROM nodes n
       JOIN packets p ON (p.rx_node_id = n.node_id OR p.src_node_id = n.node_id)
       WHERE p.time > NOW() - INTERVAL '24 hours'
         AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
         ${filters.packetsAlias('p')}
       GROUP BY n.node_id, n.name
       HAVING COUNT(p.packet_hash) FILTER (WHERE p.rx_node_id = n.node_id) > 0
       ORDER BY rx_24h DESC`,
      filters.params,
    );
  }

  return {
    fetchObserverRegionSummary,
    fetchChannelTraffic,
    fetchChartsData,
    fetchStatsSummary,
    fetchObserverActivity,
  };
}
