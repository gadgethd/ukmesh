import type { QueryResultRow } from 'pg';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type NetworkFilters = {
  params: string[];
  packets: string;
  packetsAlias: (alias: string) => string;
  nodes: string;
  nodesAlias: (alias: string) => string;
};

type MaskDecodedPathNodesFn = (
  rawNodes: Array<{
    ord: number;
    node_id: string | null;
    name: string | null;
    lat: number | null;
    lon: number | null;
    last_seen?: string | null;
  }> | null | undefined,
) => Array<{
  ord: number;
  node_id: string | null;
  name: string | null;
  lat: number | null;
  lon: number | null;
}>;

type StatsServiceDeps = {
  statsCache: Map<string, { ts: number; data: unknown }>;
  statsCacheTtlMs: number;
  chartsCache: Map<string, { ts: number; data: unknown }>;
  chartsCacheTtlMs: number;
  chartsInflight: Map<string, Promise<unknown>>;
  crossNetworkCache: Map<string, { ts: number; data: unknown }>;
  crossNetworkCacheTtlMs: number;
  networkFilters: (network?: string, observer?: string) => NetworkFilters;
  query: QueryFn;
  maskDecodedPathNodes: MaskDecodedPathNodesFn;
};

export type StatsService = ReturnType<typeof createStatsService>;

export function createStatsService(deps: StatsServiceDeps) {
  const {
    statsCache,
    statsCacheTtlMs,
    chartsCache,
    chartsCacheTtlMs,
    chartsInflight,
    crossNetworkCache,
    crossNetworkCacheTtlMs,
    networkFilters,
    query,
    maskDecodedPathNodes,
  } = deps;

  const PAYLOAD_LABELS: Record<number, string> = {
    0: 'Request', 1: 'Response', 2: 'DM', 3: 'Ack',
    4: 'Advert', 5: 'GroupText', 6: 'GroupData',
    7: 'AnonReq', 8: 'Path', 9: 'Trace', 11: 'Control',
  };

  const fmtHour = (ts: Date | string) => {
    const d = new Date(ts);
    return `${d.getHours().toString().padStart(2, '0')}:00`;
  };
  const fmtHourMinute = (ts: Date | string) => {
    const d = new Date(ts);
    return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}`;
  };
  const fmtDay = (ts: Date | string) => {
    const d = new Date(ts);
    return d.toLocaleDateString('en-GB', { weekday: 'short', month: 'short', day: 'numeric' });
  };

  async function computeChartsData(network: string | undefined, observer: string | undefined): Promise<unknown> {
    const filters = networkFilters(network, observer);

    const [
      phResult, pdResult, rhResult, rdResult,
      ptResult, rpResult, hdResult, pcResult, sumResult, orSummaryResult, orSeriesResult,
      pathHashWidthsResult, multibyteSummaryResult,
    ] = await Promise.all([
      query(`
        WITH buckets AS (
          SELECT generate_series(
            date_trunc('minute', NOW() - INTERVAL '24 hours'),
            date_trunc('minute', NOW()),
            INTERVAL '5 minutes'
          ) AS bucket
        )
        SELECT b.bucket AS hour, COALESCE(c.count, 0) AS count
        FROM buckets b
        LEFT JOIN LATERAL (
          SELECT COUNT(*)::int AS count
          FROM packets p
          WHERE p.time > b.bucket - INTERVAL '1 hour'
            AND p.time <= b.bucket
            ${filters.packetsAlias('p')}
        ) c ON TRUE
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
            date_trunc('minute', NOW() - INTERVAL '24 hours'),
            date_trunc('minute', NOW()),
            INTERVAL '5 minutes'
          ) AS bucket
        )
        SELECT b.bucket AS hour, COALESCE(c.count, 0) AS count
        FROM buckets b
        LEFT JOIN LATERAL (
          SELECT COUNT(DISTINCT p.src_node_id)::int AS count
          FROM packets p
          WHERE p.time > b.bucket - INTERVAL '1 hour'
            AND p.time <= b.bucket
            AND p.src_node_id IS NOT NULL
            ${filters.packetsAlias('p')}
        ) c ON TRUE
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
        WITH hours AS (
          SELECT generate_series(
            date_trunc('hour', NOW() - INTERVAL '7 days'),
            date_trunc('hour', NOW()),
            INTERVAL '1 hour'
          ) AS hour
        )
        SELECT h.hour,
          (SELECT COUNT(*)
           FROM nodes n
           WHERE (n.role IS NULL OR n.role = 2)
             AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
             ${filters.nodesAlias('n')}
             AND n.created_at <= h.hour + INTERVAL '1 hour') AS count
        FROM hours h
        ORDER BY h.hour
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
          SELECT LEFT(n.node_id, 2) AS prefix, COUNT(*)::int AS node_count
          FROM nodes n
          WHERE n.node_id ~ '^[0-9A-Fa-f]{64}$'
            AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
            AND (n.role IS NULL OR n.role = 2)
            ${filters.nodesAlias('n')}
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
          (SELECT COUNT(DISTINCT src_node_id) FROM packets WHERE time > NOW() - INTERVAL '24 hours' AND src_node_id IS NOT NULL ${filters.packets}) AS unique_radios_24h,
          (SELECT COUNT(*) FROM nodes n WHERE (n.role IS NULL OR n.role = 2) AND n.last_seen > NOW() - INTERVAL '7 days' ${filters.nodesAlias('n')}) AS active_repeaters,
          (SELECT COUNT(*) FROM nodes n WHERE (n.role IS NULL OR n.role = 2) AND n.last_seen <= NOW() - INTERVAL '7 days' AND n.last_seen > NOW() - INTERVAL '14 days' ${filters.nodesAlias('n')}) AS stale_repeaters
      `, filters.params),
      query(`
        SELECT
          COALESCE(NULLIF(TRIM(UPPER(n.iata)), ''), 'UNK') AS iata,
          COUNT(DISTINCT p.packet_hash) FILTER (WHERE p.time > NOW() - INTERVAL '24 hours') AS packets_24h,
          COUNT(DISTINCT p.packet_hash) AS packets_7d,
          COUNT(DISTINCT p.rx_node_id) FILTER (WHERE n.last_seen > NOW() - INTERVAL '1 minute') AS active_observers,
          COUNT(DISTINCT p.rx_node_id) AS observers,
          MAX(p.time)::text AS last_packet_at
        FROM packets p
        LEFT JOIN nodes n ON n.node_id = p.rx_node_id
        WHERE p.time > NOW() - INTERVAL '7 days'
          AND p.rx_node_id IS NOT NULL
          AND p.rx_node_id <> ''
          AND p.rx_node_id ~ '^[0-9A-Fa-f]{64}$'
          ${filters.packetsAlias('p')}
        GROUP BY 1
        ORDER BY packets_7d DESC, iata ASC
      `, filters.params),
      query(`
        SELECT
          COALESCE(NULLIF(TRIM(UPPER(n.iata)), ''), 'UNK') AS iata,
          time_bucket('1 day', p.time) AS day,
          COUNT(DISTINCT p.packet_hash) AS count
        FROM packets p
        LEFT JOIN nodes n ON n.node_id = p.rx_node_id
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
         FROM packets p
         CROSS JOIN LATERAL unnest(p.path_hashes) AS h
         WHERE p.time > NOW() - INTERVAL '24 hours'
           ${filters.packetsAlias('p')}
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
           SELECT DISTINCT ON (p.packet_hash)
             p.packet_hash,
             p.time,
             p.path_hashes
           FROM packets p
           WHERE p.time > NOW() - INTERVAL '24 hours'
             AND p.path_hash_size_bytes > 1
             AND COALESCE(array_length(p.path_hashes, 1), 0) > 0
             ${filters.packetsAlias('p')}
           ORDER BY p.packet_hash,
                    COALESCE(array_length(p.path_hashes, 1), 0) DESC,
                    COALESCE(p.hop_count, 0) DESC,
                    p.time DESC
         ),
         hop_matches AS (
           SELECT m.packet_hash, h.hash, h.ord,
             COUNT(DISTINCT n.node_id)::int AS match_count,
             MIN(n.node_id) FILTER (WHERE n.node_id IS NOT NULL) AS matched_node_id
           FROM multibyte m
           CROSS JOIN LATERAL unnest(m.path_hashes) WITH ORDINALITY AS h(hash, ord)
           LEFT JOIN nodes n
             ON UPPER(LEFT(n.node_id, LENGTH(h.hash))) = UPPER(h.hash)
            AND (n.role IS NULL OR n.role = 2)
            AND n.lat IS NOT NULL
            AND n.lon IS NOT NULL
           GROUP BY 1, 2, 3
         ),
         packet_eval AS (
           SELECT m.packet_hash,
             MAX(m.time)::text AS packet_time,
             BOOL_AND(hm.match_count = 1) AS all_unique,
             COUNT(hm.ord)::int AS hop_count,
             COUNT(DISTINCT hm.matched_node_id) FILTER (WHERE hm.matched_node_id IS NOT NULL)::int AS unique_nodes,
             string_agg(COALESCE(n.name, hm.matched_node_id, hm.hash), ' -> ' ORDER BY hm.ord) AS decoded_path,
             jsonb_agg(jsonb_build_object('ord', hm.ord, 'node_id', hm.matched_node_id, 'name', n.name, 'lat', n.lat, 'lon', n.lon, 'last_seen', n.last_seen::text) ORDER BY hm.ord) AS decoded_nodes
           FROM multibyte m
           JOIN hop_matches hm ON hm.packet_hash = m.packet_hash
           LEFT JOIN nodes n ON n.node_id = hm.matched_node_id
           GROUP BY m.packet_hash
         ),
         latest_multibyte AS (
           SELECT m.packet_hash, m.time FROM multibyte m ORDER BY m.time DESC, m.packet_hash DESC LIMIT 1
         ),
         latest_fully_decoded AS (
           SELECT pe.packet_hash, pe.packet_time, pe.hop_count, pe.decoded_path, pe.decoded_nodes
           FROM packet_eval pe
           WHERE pe.all_unique AND pe.unique_nodes = pe.hop_count
           ORDER BY pe.packet_time DESC, pe.packet_hash DESC
           LIMIT 1
         ),
         longest_fully_decoded AS (
           SELECT pe.packet_hash, pe.packet_time, pe.hop_count, pe.decoded_path, pe.decoded_nodes
           FROM packet_eval pe
           WHERE pe.all_unique AND pe.unique_nodes = pe.hop_count
           ORDER BY pe.hop_count DESC, pe.packet_time DESC, pe.packet_hash DESC
           LIMIT 1
         )
         SELECT
           (SELECT MAX(time)::text FROM multibyte) AS latest_multibyte_at,
           (SELECT packet_hash FROM latest_multibyte) AS latest_multibyte_hash,
           (SELECT COUNT(*)::text FROM multibyte) AS multibyte_packets_24h,
           (SELECT COUNT(*)::text FROM packet_eval WHERE all_unique AND unique_nodes = hop_count) AS fully_decoded_multibyte_24h,
           (SELECT packet_time FROM latest_fully_decoded) AS latest_fully_decoded_at,
           (SELECT packet_hash FROM latest_fully_decoded) AS latest_fully_decoded_hash,
           (SELECT hop_count::text FROM latest_fully_decoded) AS latest_fully_decoded_hops,
           (SELECT decoded_path FROM latest_fully_decoded) AS latest_fully_decoded_path,
           (SELECT decoded_nodes FROM latest_fully_decoded) AS latest_fully_decoded_nodes,
           (SELECT packet_time FROM longest_fully_decoded) AS longest_fully_decoded_at,
           (SELECT packet_hash FROM longest_fully_decoded) AS longest_fully_decoded_hash,
           (SELECT hop_count::text FROM longest_fully_decoded) AS longest_fully_decoded_hops,
           (SELECT decoded_path FROM longest_fully_decoded) AS longest_fully_decoded_path,
           (SELECT decoded_nodes FROM longest_fully_decoded) AS longest_fully_decoded_nodes`,
        filters.params,
      ),
    ]);

    const peakRow = phResult.rows.reduce(
      (best: any, r: any) => (Number(r.count) > Number(best?.count ?? 0) ? r : best),
      null,
    );

    const observerRegionsByIata = new Map<string, {
      iata: string;
      activeObservers: number;
      observers: number;
      packets24h: number;
      packets7d: number;
      lastPacketAt: string | null;
      series: { day: string; count: number }[];
    }>();

    for (const row of orSummaryResult.rows) {
      const iata = String(row.iata ?? 'UNK');
      observerRegionsByIata.set(iata, {
        iata,
        activeObservers: Number(row.active_observers ?? 0),
        observers: Number(row.observers ?? 0),
        packets24h: Number(row.packets_24h ?? 0),
        packets7d: Number(row.packets_7d ?? 0),
        lastPacketAt: row.last_packet_at ?? null,
        series: [],
      });
    }

    for (const row of orSeriesResult.rows) {
      const iata = String(row.iata ?? 'UNK');
      const region = observerRegionsByIata.get(iata);
      if (!region) continue;
      region.series.push({ day: fmtDay(row.day), count: Number(row.count ?? 0) });
    }

    const widthToBucket: Record<number, 'one_byte' | 'two_byte' | 'three_byte'> = { 2: 'one_byte', 4: 'two_byte', 6: 'three_byte' };
    const pathHashStats = { one_byte: 0, two_byte: 0, three_byte: 0 };
    for (const row of pathHashWidthsResult.rows) {
      const width = Number(row.hash_hex_len ?? 0);
      const bucket = widthToBucket[width];
      if (!bucket) continue;
      pathHashStats[bucket] += Number(row.hop_count ?? 0);
    }

    const multibyteRow = multibyteSummaryResult.rows[0];
    const latestFullyDecodedNodes = maskDecodedPathNodes(multibyteRow?.latest_fully_decoded_nodes);
    const longestFullyDecodedNodes = maskDecodedPathNodes(multibyteRow?.longest_fully_decoded_nodes);

    return {
      packetsPerHour: phResult.rows.map(r => ({ hour: fmtHourMinute((r as any).hour), count: Number((r as any).count) })),
      packetsPerDay: pdResult.rows.map(r => ({ day: fmtDay((r as any).day), count: Number((r as any).count) })),
      radiosPerHour: rhResult.rows.map(r => ({ hour: fmtHourMinute((r as any).hour), count: Number((r as any).count) })),
      radiosPerDay: rdResult.rows.map(r => ({ day: fmtDay((r as any).day), count: Number((r as any).count) })),
      packetTypes: ptResult.rows.map(r => ({ label: PAYLOAD_LABELS[Number((r as any).packet_type)] ?? `Type${(r as any).packet_type}`, count: Number((r as any).count) })),
      repeatersPerDay: rpResult.rows.map(r => ({ hour: fmtDay((r as any).hour), count: Number((r as any).count ?? 0) })),
      hopDistribution: hdResult.rows.map(r => ({ hops: Number((r as any).hops), count: Number((r as any).count) })),
      topChatters: [],
      prefixCollisions: pcResult.rows.map(r => ({ prefix: String((r as any).prefix ?? '').toUpperCase(), repeats: Number((r as any).repeats) })),
      observerRegions: Array.from(observerRegionsByIata.values()),
      pathHashes: {
        last24hHops: pathHashStats,
        multibytePackets24h: Number(multibyteRow?.multibyte_packets_24h ?? 0),
        fullyDecodedMultibyte24h: Number(multibyteRow?.fully_decoded_multibyte_24h ?? 0),
        latestMultibyteAt: multibyteRow?.latest_multibyte_at ?? null,
        latestMultibyteHash: multibyteRow?.latest_multibyte_hash ?? null,
        latestFullyDecodedAt: multibyteRow?.latest_fully_decoded_at ?? null,
        latestFullyDecodedHash: multibyteRow?.latest_fully_decoded_hash ?? null,
        latestFullyDecodedHops: Number(multibyteRow?.latest_fully_decoded_hops ?? 0) || null,
        latestFullyDecodedPath: multibyteRow?.latest_fully_decoded_path ?? null,
        latestFullyDecodedNodes,
        longestFullyDecodedAt: multibyteRow?.longest_fully_decoded_at ?? null,
        longestFullyDecodedHash: multibyteRow?.longest_fully_decoded_hash ?? null,
        longestFullyDecodedHops: Number(multibyteRow?.longest_fully_decoded_hops ?? 0) || null,
        longestFullyDecodedPath: multibyteRow?.longest_fully_decoded_path ?? null,
        longestFullyDecodedNodes,
      },
      summary: {
        totalPackets24h: Number((sumResult.rows[0] as any).total_24h),
        totalPackets7d: Number((sumResult.rows[0] as any).total_7d),
        uniqueRadios24h: Number((sumResult.rows[0] as any).unique_radios_24h),
        activeRepeaters: Number((sumResult.rows[0] as any).active_repeaters ?? 0),
        staleRepeaters: Number((sumResult.rows[0] as any).stale_repeaters ?? 0),
        peakHour: peakRow ? fmtHour((peakRow as any).hour) : null,
        peakHourCount: peakRow ? Number((peakRow as any).count) : 0,
      },
    };
  }

  async function getCharts(network: string | undefined, observer: string | undefined): Promise<unknown> {
    const key = `${network ?? 'all'}:${observer ?? ''}`;
    const cached = chartsCache.get(key);
    if (cached && Date.now() - cached.ts < chartsCacheTtlMs) return cached.data;

    const inflight = chartsInflight.get(key);
    if (inflight) return inflight;

    const promise = computeChartsData(network, observer).then((data) => {
      chartsCache.set(key, { ts: Date.now(), data });
      chartsInflight.delete(key);
      return data;
    }).catch((err) => {
      chartsInflight.delete(key);
      throw err;
    });

    chartsInflight.set(key, promise);
    return promise;
  }

  function startChartsWarmup(): void {
    const warmupNetworks = (process.env['WARMUP_NETWORKS'] ?? 'teesside,ukmesh')
      .split(',').map((s: string) => s.trim()).filter(Boolean);

    const warmCharts = () => {
      for (const net of warmupNetworks) {
        getCharts(net, undefined).catch(() => { /* best-effort */ });
      }
    };

    setTimeout(warmCharts, 5_000);
    setInterval(warmCharts, chartsCacheTtlMs);
  }

  async function getStatsSummary(network: string | undefined, observer: string | undefined): Promise<unknown> {
    const filters = networkFilters(network, observer);
    const statsCacheKey = `${network ?? 'all'}:${observer ?? ''}`;
    const statsCached = statsCache.get(statsCacheKey);
    if (statsCached && Date.now() - statsCached.ts < statsCacheTtlMs) {
      return statsCached.data;
    }

    const [mqttCount, packetCount, staleCount, mapNodeCount, totalNodeCount, longestHopCount, nodesDayCount] = await Promise.all([
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
             WHERE lat IS NOT NULL AND lon IS NOT NULL
               AND (role IS NULL OR role = 2)
               AND (name IS NULL OR name NOT LIKE '%🚫%')
               AND last_seen <= NOW() - INTERVAL '7 days'
               AND last_seen >  NOW() - INTERVAL '14 days'
               ${filters.nodes}`, filters.params),
      query(`SELECT COUNT(*) AS count FROM nodes
             WHERE lat IS NOT NULL AND lon IS NOT NULL
               AND (role IS NULL OR role = 2)
               AND (name IS NULL OR name NOT LIKE '%🚫%')
               AND last_seen > NOW() - INTERVAL '14 days'
               ${filters.nodes}`, filters.params),
      query(`SELECT COUNT(*) AS count FROM nodes
             WHERE (name IS NULL OR name NOT LIKE '%🚫%')
               AND (role IS NULL OR role != 4)
               ${filters.nodes}`, filters.params),
      query(`SELECT hop_count AS count, payload->>'hash' AS hash
             FROM packets
             WHERE hop_count IS NOT NULL
               AND time > NOW() - INTERVAL '30 days'
               ${filters.packets}
             ORDER BY hop_count DESC LIMIT 1`, filters.params),
      query(`SELECT COUNT(DISTINCT src_node_id) AS count
             FROM packets
             WHERE time > NOW() - INTERVAL '24 hours'
               AND src_node_id IS NOT NULL
               ${filters.packets}`, filters.params),
    ]);

    const statsData = {
      mqttNodes: Number((mqttCount.rows[0] as any)?.count ?? 0),
      staleNodes: Number((staleCount.rows[0] as any)?.count ?? 0),
      packetsDay: Number((packetCount.rows[0] as any)?.count ?? 0),
      mapNodes: Number((mapNodeCount.rows[0] as any)?.count ?? 0),
      nodesDay: Number((nodesDayCount.rows[0] as any)?.count ?? 0),
      totalNodes: Number((totalNodeCount.rows[0] as any)?.count ?? 0),
      longestHop: Number((longestHopCount.rows[0] as any)?.count ?? 0),
      longestHopHash: ((longestHopCount.rows[0] as any)?.hash as string | undefined) ?? null,
    };
    statsCache.set(statsCacheKey, { ts: Date.now(), data: statsData });
    return statsData;
  }

  async function getObserverActivity(network: string | undefined): Promise<unknown> {
    const params: unknown[] = [];
    const conditions: string[] = [`p.time > NOW() - INTERVAL '24 hours'`];
    if (network) {
      params.push(network);
      conditions.push(`n.network = $${params.length}`);
    }
    const where = conditions.join(' AND ');
    const result = await query<{ node_id: string; name: string | null; rx_24h: string; tx_24h: string; last_tx: string | null; last_rx: string | null }>(
      `SELECT
         n.node_id,
         n.name,
         COUNT(p.packet_hash) FILTER (WHERE p.rx_node_id  = n.node_id) AS rx_24h,
         COUNT(p.packet_hash) FILTER (WHERE p.src_node_id = n.node_id) AS tx_24h,
         MAX(p.time)          FILTER (WHERE p.src_node_id = n.node_id)::text AS last_tx,
         MAX(p.time)          FILTER (WHERE p.rx_node_id  = n.node_id)::text AS last_rx
       FROM nodes n
       JOIN packets p ON (p.rx_node_id = n.node_id OR p.src_node_id = n.node_id)
       WHERE ${where}
       GROUP BY n.node_id, n.name
       HAVING COUNT(p.packet_hash) FILTER (WHERE p.rx_node_id = n.node_id) > 0
       ORDER BY rx_24h DESC`,
      params,
    );
    return result.rows.map((r) => ({ ...r, rx_24h: Number(r.rx_24h), tx_24h: Number(r.tx_24h) }));
  }

  async function getCrossNetworkConnectivity(): Promise<unknown> {
    const cacheKey = 'teesside';
    const cached = crossNetworkCache.get(cacheKey);
    if (cached && Date.now() - cached.ts < crossNetworkCacheTtlMs) {
      return cached.data;
    }

    const [result, historyResult] = await Promise.all([
      query<{ last_inbound: string | null; last_outbound: string | null }>(`
        WITH mme_observers AS (
          SELECT DISTINCT rx_node_id AS node_id
          FROM packets
          WHERE time > NOW() - INTERVAL '24 hours'
            AND network = 'teesside'
            AND rx_node_id IS NOT NULL
        ),
        cross_heard AS (
          SELECT
            p.packet_hash,
            MIN(p.hop_count) FILTER (WHERE p.rx_node_id IN (SELECT node_id FROM mme_observers)) AS mme_min_hops,
            MIN(p.hop_count) FILTER (WHERE p.rx_node_id NOT IN (SELECT node_id FROM mme_observers)) AS other_min_hops,
            MIN(p.time) FILTER (WHERE p.rx_node_id IN (SELECT node_id FROM mme_observers)) AS mme_first_seen,
            MIN(p.time) FILTER (WHERE p.rx_node_id NOT IN (SELECT node_id FROM mme_observers)) AS other_first_seen
          FROM packets p
          WHERE p.time > NOW() - INTERVAL '2 hours'
            AND p.hop_count IS NOT NULL
            AND p.rx_node_id IS NOT NULL
            AND p.packet_hash IS NOT NULL
          GROUP BY p.packet_hash
          HAVING MIN(p.hop_count) FILTER (WHERE p.rx_node_id IN (SELECT node_id FROM mme_observers)) IS NOT NULL
            AND MIN(p.hop_count) FILTER (WHERE p.rx_node_id NOT IN (SELECT node_id FROM mme_observers)) IS NOT NULL
            AND ABS(EXTRACT(EPOCH FROM (
              MIN(p.time) FILTER (WHERE p.rx_node_id IN (SELECT node_id FROM mme_observers)) -
              MIN(p.time) FILTER (WHERE p.rx_node_id NOT IN (SELECT node_id FROM mme_observers))
            ))) <= 120
        )
        SELECT
          MAX(mme_first_seen) FILTER (WHERE other_min_hops < mme_min_hops) AS last_inbound,
          MAX(other_first_seen) FILTER (WHERE mme_min_hops < other_min_hops) AS last_outbound
        FROM cross_heard
        WHERE mme_min_hops != other_min_hops
      `),
      query<{ bucket: string; inbound_count: string; outbound_count: string }>(`
        WITH mme_observers AS (
          SELECT DISTINCT rx_node_id AS node_id
          FROM packets
          WHERE time > NOW() - INTERVAL '24 hours'
            AND network = 'teesside'
            AND rx_node_id IS NOT NULL
        ),
        cross_heard AS (
          SELECT
            p.packet_hash,
            MIN(p.hop_count) FILTER (WHERE p.rx_node_id IN (SELECT node_id FROM mme_observers)) AS mme_min_hops,
            MIN(p.hop_count) FILTER (WHERE p.rx_node_id NOT IN (SELECT node_id FROM mme_observers)) AS other_min_hops,
            MIN(p.time) FILTER (WHERE p.rx_node_id IN (SELECT node_id FROM mme_observers)) AS mme_first_seen,
            MIN(p.time) FILTER (WHERE p.rx_node_id NOT IN (SELECT node_id FROM mme_observers)) AS other_first_seen
          FROM packets p
          WHERE p.time > NOW() - INTERVAL '7 days'
            AND p.hop_count IS NOT NULL
            AND p.rx_node_id IS NOT NULL
            AND p.packet_hash IS NOT NULL
          GROUP BY p.packet_hash
          HAVING MIN(p.hop_count) FILTER (WHERE p.rx_node_id IN (SELECT node_id FROM mme_observers)) IS NOT NULL
            AND MIN(p.hop_count) FILTER (WHERE p.rx_node_id NOT IN (SELECT node_id FROM mme_observers)) IS NOT NULL
            AND ABS(EXTRACT(EPOCH FROM (
              MIN(p.time) FILTER (WHERE p.rx_node_id IN (SELECT node_id FROM mme_observers)) -
              MIN(p.time) FILTER (WHERE p.rx_node_id NOT IN (SELECT node_id FROM mme_observers))
            ))) <= 120
        ),
        classified AS (
          SELECT
            date_trunc('hour', mme_first_seen) AS bucket,
            CASE WHEN other_min_hops < mme_min_hops THEN 1 ELSE 0 END AS inbound_count,
            CASE WHEN mme_min_hops < other_min_hops THEN 1 ELSE 0 END AS outbound_count
          FROM cross_heard
          WHERE mme_min_hops != other_min_hops
            AND mme_first_seen IS NOT NULL
        ),
        buckets AS (
          SELECT generate_series(
            date_trunc('hour', NOW() - INTERVAL '7 days'),
            date_trunc('hour', NOW()),
            INTERVAL '1 hour'
          ) AS bucket
        )
        SELECT
          to_char(b.bucket, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS bucket,
          COALESCE(SUM(c.inbound_count), 0)::text AS inbound_count,
          COALESCE(SUM(c.outbound_count), 0)::text AS outbound_count
        FROM buckets b
        LEFT JOIN classified c ON c.bucket = b.bucket
        GROUP BY b.bucket
        ORDER BY b.bucket
      `),
    ]);

    const lastInbound = result.rows[0]?.last_inbound ?? null;
    const lastOutbound = result.rows[0]?.last_outbound ?? null;
    const responseData = {
      inbound: !!lastInbound,
      outbound: !!lastOutbound,
      lastInbound,
      lastOutbound,
      windowHours: 2,
      historyWindowHours: 7 * 24,
      history: historyResult.rows.map((row) => ({
        bucket: row.bucket,
        inboundCount: Number(row.inbound_count ?? 0),
        outboundCount: Number(row.outbound_count ?? 0),
      })),
      checkedAt: new Date().toISOString(),
    };
    crossNetworkCache.set(cacheKey, { ts: Date.now(), data: responseData });
    return responseData;
  }

  return {
    startChartsWarmup,
    getCharts,
    getStatsSummary,
    getObserverActivity,
    getCrossNetworkConnectivity,
  };
}
