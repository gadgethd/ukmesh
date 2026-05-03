import type { StatsRepository } from './statsRepository.js';

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
  repository: StatsRepository;
  maskDecodedPathNodes: MaskDecodedPathNodesFn;
};

export type StatsService = ReturnType<typeof createStatsService>;

const CHANNEL_TRAFFIC_CACHE_TTL_MS = 60 * 60_000;

type CachedChannelTraffic = Array<{
  channel: string;
  count: number;
  pct: number;
}>;

function countryFromLatLon(lat: number, lon: number): string | null {
  // Ordered smallest/most-specific first to reduce overlap ambiguity
  if (lat >= 50.75 && lat <= 53.60 && lon >= 3.35 && lon <= 7.22) return 'Netherlands';
  if (lat >= 49.50 && lat <= 51.51 && lon >= 2.54 && lon <= 6.41) return 'Belgium';
  if (lat >= 47.27 && lat <= 55.07 && lon >= 5.87 && lon <= 15.04) return 'Germany';
  if (lat >= 54.56 && lat <= 57.75 && lon >= 8.07 && lon <= 15.20) return 'Denmark';
  if (lat >= 51.44 && lat <= 55.39 && lon >= -10.48 && lon <= -5.34) return 'Ireland';
  if (lat >= 41.33 && lat <= 51.12 && lon >= -5.14 && lon <= 9.56) return 'France';
  if (lat >= 35.92 && lat <= 43.79 && lon >= -9.30 && lon <= 4.29) return 'Spain';
  if (lat >= 36.62 && lat <= 47.10 && lon >= 6.61 && lon <= 18.52) return 'Italy';
  if (lat >= 55.34 && lat <= 69.06 && lon >= 11.12 && lon <= 24.17) return 'Sweden';
  if (lat >= 57.98 && lat <= 71.19 && lon >= 4.50 && lon <= 31.10) return 'Norway';
  if (lat >= 59.81 && lat <= 70.09 && lon >= 19.09 && lon <= 31.59) return 'Finland';
  if (lat >= 46.37 && lat <= 54.84 && lon >= 14.12 && lon <= 24.15) return 'Poland';
  return null;
}

export function createStatsService(deps: StatsServiceDeps) {
  const {
    statsCache,
    statsCacheTtlMs,
    chartsCache,
    chartsCacheTtlMs,
    chartsInflight,
    crossNetworkCache,
    crossNetworkCacheTtlMs,
    repository,
    maskDecodedPathNodes,
  } = deps;

  const PAYLOAD_LABELS: Record<number, string> = {
    0: 'Request', 1: 'Response', 2: 'DM', 3: 'Ack',
    4: 'Advert', 5: 'GroupText', 6: 'GroupData',
    7: 'AnonReq', 8: 'Path', 9: 'Trace', 11: 'Control',
  };
  const channelTrafficCache = new Map<string, { ts: number; data: CachedChannelTraffic }>();
  const statsInflight = new Map<string, Promise<unknown>>();

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

  function mergeObserverRegionSummary(
    data: unknown,
    summaryRows: Array<{
      iata?: string | null;
      active_observers?: string | number | null;
      observers?: string | number | null;
      packets_24h?: string | number | null;
      packets_7d?: string | number | null;
      last_packet_at?: string | null;
    }>,
  ): unknown {
    if (!data || typeof data !== 'object') return data;
    const current = data as {
      observerRegions?: Array<{
        iata: string;
        series?: { day: string; count: number }[];
      }>;
    };
    const seriesByIata = new Map(
      Array.isArray(current.observerRegions)
        ? current.observerRegions.map((region) => [region.iata, Array.isArray(region.series) ? region.series : []] as const)
        : [],
    );
    return {
      ...current,
      observerRegions: summaryRows.map((row) => {
        const iata = String(row.iata ?? 'UNK');
        return {
          iata,
          activeObservers: Number(row.active_observers ?? 0),
          observers: Number(row.observers ?? 0),
          packets24h: Number(row.packets_24h ?? 0),
          packets7d: Number(row.packets_7d ?? 0),
          lastPacketAt: row.last_packet_at ?? null,
          series: seriesByIata.get(iata) ?? [],
        };
      }),
    };
  }

  async function getChannelTraffic(network: string | undefined, observer: string | undefined): Promise<CachedChannelTraffic> {
    const key = `${network ?? 'all'}:${observer ?? ''}`;
    const cached = channelTrafficCache.get(key);
    if (cached && Date.now() - cached.ts < CHANNEL_TRAFFIC_CACHE_TTL_MS) return cached.data;

    const result = await repository.fetchChannelTraffic(network, observer);
    const data = result.rows.map((r) => {
      const count = Number(r.count ?? 0);
      const total = Number(r.total_count ?? 0);
      return {
        channel: String(r.channel ?? 'Unknown'),
        count,
        pct: total > 0 ? Number(((count / total) * 100).toFixed(1)) : 0,
      };
    });
    channelTrafficCache.set(key, { ts: Date.now(), data });
    return data;
  }

  async function computeChartsData(network: string | undefined, observer: string | undefined): Promise<unknown> {
    const {
      phResult, pdResult, rhResult, rdResult,
      ptResult, rpResult, hdResult, pcResult, sumResult, orSummaryResult, orSeriesResult,
      pathHashWidthsResult, multibyteSummaryResult,
    } = await repository.fetchChartsData(network, observer);

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
    const totalPackets24h = Number((sumResult.rows[0] as any).total_24h ?? 0);
    const channelTrafficRows = await getChannelTraffic(network, observer);

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
      channelTraffic: channelTrafficRows.map((row) => ({
        ...row,
        allPct: totalPackets24h > 0 ? Number(((row.count / totalPackets24h) * 100).toFixed(1)) : 0,
      })),
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
        totalPackets24h,
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
    if (cached && Date.now() - cached.ts < chartsCacheTtlMs) {
      const observerRegionSummary = await repository.fetchObserverRegionSummary(network, observer);
      const refreshed = mergeObserverRegionSummary(
        cached.data,
        observerRegionSummary.rows as Array<{
          iata?: string | null;
          active_observers?: string | number | null;
          observers?: string | number | null;
          packets_24h?: string | number | null;
          packets_7d?: string | number | null;
          last_packet_at?: string | null;
        }>,
      );
      chartsCache.set(key, { ts: cached.ts, data: refreshed });
      return refreshed;
    }

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

    const warmCharts = async () => {
      for (const net of warmupNetworks) {
        await getCharts(net, undefined).catch(() => { /* best-effort */ });
      }
    };

    setTimeout(warmCharts, 5_000);
    setInterval(warmCharts, chartsCacheTtlMs);

    const warmStats = async () => {
      for (const net of warmupNetworks) {
        await getStatsSummary(net, undefined).catch(() => { /* best-effort */ });
      }
    };

    setTimeout(warmStats, 6_000);
    setInterval(warmStats, statsCacheTtlMs);
  }

  async function computeStatsSummary(network: string | undefined, observer: string | undefined): Promise<unknown> {
    const {
      mqttCount,
      packetCount,
      staleCount,
      mapNodeCount,
      totalNodeCount,
      longestHopCount,
      nodesDayCount,
      internationalCount,
    } = await repository.fetchStatsSummary(network, observer);

    const intlRow = (internationalCount.rows[0] as any);
    const intlTotalAdverts = Number(intlRow?.total_adverts ?? 0);
    const intlConfirmed = intlTotalAdverts >= 20;
    const intlLat = intlRow?.last_lat != null ? Number(intlRow.last_lat) : null;
    const intlLon = intlRow?.last_lon != null ? Number(intlRow.last_lon) : null;
    const intlCountry = intlConfirmed && intlLat != null && intlLon != null
      ? countryFromLatLon(intlLat, intlLon)
      : null;

    return {
      mqttNodes: Number((mqttCount.rows[0] as any)?.count ?? 0),
      staleNodes: Number((staleCount.rows[0] as any)?.count ?? 0),
      packetsDay: Number((packetCount.rows[0] as any)?.count ?? 0),
      mapNodes: Number((mapNodeCount.rows[0] as any)?.count ?? 0),
      nodesDay: Number((nodesDayCount.rows[0] as any)?.count ?? 0),
      totalNodes: Number((totalNodeCount.rows[0] as any)?.count ?? 0),
      longestHop: Number((longestHopCount.rows[0] as any)?.count ?? 0),
      longestHopHash: ((longestHopCount.rows[0] as any)?.hash as string | undefined) ?? null,
      internationalNodes: intlConfirmed ? Number(intlRow?.count_connected ?? 0) : 0,
      internationalLastSeen: intlConfirmed ? ((intlRow?.last_seen_at as string | null) ?? null) : null,
      internationalLastCountry: intlCountry,
    };
  }

  async function getStatsSummary(network: string | undefined, observer: string | undefined): Promise<unknown> {
    const key = `${network ?? 'all'}:${observer ?? ''}`;
    const cached = statsCache.get(key);
    if (cached && Date.now() - cached.ts < statsCacheTtlMs) {
      return cached.data;
    }

    const inflight = statsInflight.get(key);
    if (inflight) return inflight;

    const promise = computeStatsSummary(network, observer).then((data) => {
      statsCache.set(key, { ts: Date.now(), data });
      statsInflight.delete(key);
      return data;
    }).catch((err) => {
      statsInflight.delete(key);
      throw err;
    });

    statsInflight.set(key, promise);
    return promise;
  }

  async function getObserverActivity(network: string | undefined): Promise<unknown> {
    const result = await repository.fetchObserverActivity(network);
    return result.rows.map((r) => ({ ...r, rx_24h: Number(r.rx_24h), tx_24h: Number(r.tx_24h) }));
  }

  async function getCrossNetworkConnectivity(): Promise<unknown> {
    const cacheKey = 'teesside';
    const cached = crossNetworkCache.get(cacheKey);
    if (cached && Date.now() - cached.ts < crossNetworkCacheTtlMs) {
      return cached.data;
    }

    const { result, historyResult } = await repository.fetchCrossNetworkConnectivity();

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
