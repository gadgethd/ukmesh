import type { StatsRepository } from './statsRepository.js';
import { statsRecomputeDuration, statsRecomputeTotal } from '../metrics.js';
import { BoundedTtlMap } from '../cache/boundedTtlMap.js';
import {
  CHART_SNAPSHOT_MAX_FUTURE_SKEW_MS,
  validateChartSnapshotPayload,
} from './chartSnapshot.js';

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
  chartsSnapshotStaleTtlMs?: number;
  chartsInflight: Map<string, Promise<unknown>>;
  repository: StatsRepository;
  getPublicVisibilityGeneration: () => Promise<number>;
  maskDecodedPathNodes: MaskDecodedPathNodesFn;
};

const CHANNEL_TRAFFIC_CACHE_TTL_MS = 60 * 60_000;
const MAX_UNIQUE_STATS_INFLIGHT = Math.min(
  128,
  Math.max(1, Number(process.env['STATS_UNIQUE_INFLIGHT_MAX'] ?? 32) || 32),
);

export class StatsWorkOverloadedError extends Error {
  constructor() {
    super('STATS_WORK_OVERLOADED');
    this.name = 'StatsWorkOverloadedError';
  }
}

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

export function computeRegionHealth(input: {
  activeObservers: number;
  observers: number;
  packets24h: number;
  lastPacketAt: string | null;
}, nowMs = Date.now()): { score: number; status: 'healthy' | 'watch' | 'poor'; factors: Record<string, number> } {
  const parsedLastPacket = input.lastPacketAt ? Date.parse(input.lastPacketAt) : Number.NaN;
  const ageMinutes = Number.isFinite(parsedLastPacket) ? Math.max(0, (nowMs - parsedLastPacket) / 60_000) : Number.POSITIVE_INFINITY;
  const freshness = ageMinutes <= 5 ? 100 : ageMinutes >= 60 ? 0 : Math.round(100 * (1 - ((ageMinutes - 5) / 55)));
  const observerAvailability = input.activeObservers <= 0 ? 0 : input.activeObservers === 1 ? 60 : 100;
  const traffic = Math.min(100, Math.round((Math.log10(Math.max(0, input.packets24h) + 1) / 4) * 100));
  const observerDiversity = Math.min(100, Math.round((input.observers / 3) * 100));
  const score = Math.round(freshness * 0.4 + observerAvailability * 0.3 + traffic * 0.2 + observerDiversity * 0.1);
  return {
    score,
    status: score >= 75 ? 'healthy' : score >= 45 ? 'watch' : 'poor',
    factors: { freshness, observerAvailability, traffic, observerDiversity },
  };
}

export function createStatsService(deps: StatsServiceDeps) {
  const {
    statsCache,
    statsCacheTtlMs,
    chartsCache,
    chartsCacheTtlMs,
    chartsSnapshotStaleTtlMs = Math.max(chartsCacheTtlMs, 6 * 60 * 60_000),
    chartsInflight,
    repository,
    getPublicVisibilityGeneration,
    maskDecodedPathNodes,
  } = deps;

  const PAYLOAD_LABELS: Record<number, string> = {
    0: 'Request', 1: 'Response', 2: 'DM', 3: 'Ack',
    4: 'Advert', 5: 'GroupText', 6: 'GroupData',
    7: 'AnonReq', 8: 'Path', 9: 'Trace', 11: 'Control',
  };
  const ROUTE_LABELS: Record<string, { label: string; description: string }> = {
    '0': {
      label: 'Transport Flood',
      description: 'Flood packet with region/scope transport codes',
    },
    '1': {
      label: 'Flood',
      description: 'Broadcast flood packet without transport codes',
    },
    '2': {
      label: 'Direct',
      description: 'Directed route packet without transport codes',
    },
    '3': {
      label: 'Transport Direct',
      description: 'Directed route packet with region/scope transport codes',
    },
    Unknown: {
      label: 'Unknown',
      description: 'Route type was not decoded from the raw packet',
    },
  };
  const channelTrafficCache = new BoundedTtlMap<string, {
    ts: number;
    data: CachedChannelTraffic;
  }>({
    name: 'stats_channel',
    maxEntries: 64,
    maxWeight: 2 * 1024 * 1024,
    ttlMs: CHANNEL_TRAFFIC_CACHE_TTL_MS,
  });
  const channelTrafficInflight = new Map<string, Promise<CachedChannelTraffic>>();
  // Coordination-only maps have a hard admission cap below and entries are
  // removed in both resolve and reject paths.
  const statsInflight = new Map<string, Promise<unknown>>();
  // Observer activity runs an expensive per-packet aggregation; cache + single-
  // flight it (mirroring getStatsSummary) so concurrent/rapid polls can't pile
  // up identical queries and saturate the database CPU.
  const observerActivityCache = new BoundedTtlMap<string, {
    ts: number;
    data: unknown;
  }>({
    name: 'stats_observer',
    maxEntries: 64,
    maxWeight: 16 * 1024 * 1024,
    ttlMs: Math.max(statsCacheTtlMs * 5, 5 * 60_000),
  });
  const observerActivityInflight = new Map<string, Promise<unknown>>();
  const chartSnapshotLoads = new Map<
    string,
    Promise<{ ts: number; data: unknown } | undefined>
  >();
  let initialStatsWarmup: Promise<void> | undefined;
  let activeObserverWork = 0;

  const fmtHour = (ts: Date | string) => {
    // Machine-readable ISO for the client: axis labels are formatted in the
    // viewer's local timezone (never format display strings server-side).
    const d = new Date(ts);
    return Number.isFinite(d.getTime()) ? d.toISOString() : String(ts);
  };
  const fmtHourMinute = (ts: Date | string) => {
    const d = new Date(ts);
    return Number.isFinite(d.getTime()) ? d.toISOString() : String(ts);
  };
  const fmtDay = (ts: Date | string) => {
    const d = new Date(ts);
    return Number.isFinite(d.getTime()) ? d.toISOString() : String(ts);
  };
  const decodeTransportCodes = (raw: unknown) => {
    const hex = String(raw ?? '').trim().toUpperCase();
    if (!/^[0-9A-F]{8}$/.test(hex)) {
      return {
        raw: hex || 'Unknown',
        scopeCode: null,
        scopeCodeHex: null,
        returnCode: null,
        returnCodeHex: null,
      };
    }
    const scopeCode = Number.parseInt(`${hex.slice(2, 4)}${hex.slice(0, 2)}`, 16);
    const returnCode = Number.parseInt(`${hex.slice(6, 8)}${hex.slice(4, 6)}`, 16);
    return {
      raw: hex,
      scopeCode,
      scopeCodeHex: `0x${scopeCode.toString(16).toUpperCase().padStart(4, '0')}`,
      returnCode,
      returnCodeHex: `0x${returnCode.toString(16).toUpperCase().padStart(4, '0')}`,
    };
  };

  function refreshCachedRegionHealth(data: unknown): unknown {
    if (!data || typeof data !== 'object') return data;
    const current = data as {
      observerRegions?: Array<{
        activeObservers: number;
        observers: number;
        packets24h: number;
        lastPacketAt: string | null;
        [key: string]: unknown;
      }>;
    };
    if (!Array.isArray(current.observerRegions)) return data;
    return {
      ...current,
      observerRegions: current.observerRegions.map((region) => ({
        ...region,
        health: computeRegionHealth({
          activeObservers: region.activeObservers,
          observers: region.observers,
          packets24h: region.packets24h,
          lastPacketAt: region.lastPacketAt,
        }),
      })),
    };
  }

  async function getChannelTraffic(
    network: string | undefined,
    observer: string | undefined,
    visibilityGeneration: number,
  ): Promise<CachedChannelTraffic> {
    const key = `${network ?? 'all'}:${observer ?? ''}:v${visibilityGeneration}`;
    const cached = observer ? undefined : channelTrafficCache.get(key);
    if (cached && Date.now() - cached.ts < CHANNEL_TRAFFIC_CACHE_TTL_MS) return cached.data;
    const existing = channelTrafficInflight.get(key);
    if (existing) return existing;
    if (channelTrafficInflight.size >= MAX_UNIQUE_STATS_INFLIGHT) {
      throw new StatsWorkOverloadedError();
    }
    const tracked = repository.fetchChannelTraffic(network, observer)
      .then((result) => {
        const data = result.rows.map((r) => {
          const count = Number(r.count ?? 0);
          const total = Number(r.total_count ?? 0);
          return {
            channel: String(r.channel ?? 'Unknown'),
            count,
            pct: total > 0 ? Number(((count / total) * 100).toFixed(1)) : 0,
          };
        });
        if (!observer) channelTrafficCache.set(key, { ts: Date.now(), data });
        return data;
      })
      .finally(() => {
        if (channelTrafficInflight.get(key) === tracked) {
          channelTrafficInflight.delete(key);
        }
      });
    channelTrafficInflight.set(key, tracked);
    return tracked;
  }

  async function computeChartsData(
    network: string | undefined,
    observer: string | undefined,
    visibilityGeneration: number,
  ): Promise<unknown> {
    const {
      phResult, pdResult, rhResult, rdResult,
      ptResult, hdResult, pcResult, sumResult, orSummaryResult, orSeriesResult,
      pathHashWidthsResult, multibyteSummaryResult, observerDiversityResult, signalSummaryResult,
      routeTypesResult, transportCodesResult, pathDecodeTrendResult,
    } = await repository.fetchChartsData(network, observer, visibilityGeneration);

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
      health: ReturnType<typeof computeRegionHealth>;
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
        health: computeRegionHealth({
          activeObservers: Number(row.active_observers ?? 0),
          observers: Number(row.observers ?? 0),
          packets24h: Number(row.packets_24h ?? 0),
          lastPacketAt: row.last_packet_at ?? null,
        }),
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
    const channelTrafficRows = await getChannelTraffic(
      network,
      observer,
      visibilityGeneration,
    );
    const observerDiversityRow = observerDiversityResult.rows[0] as any;
    const signalSummaryRow = signalSummaryResult.rows[0] as any;
    const totalDiversityPackets = Number(observerDiversityRow?.total_packets ?? 0);
    const singleObserverPackets = Number(observerDiversityRow?.single_observer_packets ?? 0);

    return {
      snapshot: {
        status: 'complete',
        generatedAt: new Date().toISOString(),
        scope: network ?? 'ukmesh',
        visibilityGeneration,
      },
      packetsPerHour: phResult.rows.map(r => ({ hour: fmtHourMinute((r as any).hour), count: Number((r as any).count) })),
      packetsPerDay: pdResult.rows.map(r => ({ day: fmtDay((r as any).day), count: Number((r as any).count) })),
      radiosPerHour: rhResult.rows.map(r => ({ hour: fmtHourMinute((r as any).hour), count: Number((r as any).count) })),
      radiosPerDay: rdResult.rows.map(r => ({ day: fmtDay((r as any).day), count: Number((r as any).count) })),
      packetTypes: ptResult.rows.map(r => ({ label: PAYLOAD_LABELS[Number((r as any).packet_type)] ?? `Type${(r as any).packet_type}`, count: Number((r as any).count) })),
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
      observerDiversity: {
        averageObserversPerPacket: Number(Number(observerDiversityRow?.avg_observers ?? 0).toFixed(2)),
        maxObserversPerPacket: Number(observerDiversityRow?.max_observers ?? 0),
        totalPackets24h: totalDiversityPackets,
        singleObserverPackets24h: singleObserverPackets,
        singleObserverPct24h: totalDiversityPackets > 0
          ? Number(((singleObserverPackets / totalDiversityPackets) * 100).toFixed(1))
          : 0,
      },
      signalSummary: {
        avgRssi: signalSummaryRow?.avg_rssi == null ? null : Number(Number(signalSummaryRow.avg_rssi).toFixed(1)),
        medianRssi: signalSummaryRow?.median_rssi == null ? null : Number(Number(signalSummaryRow.median_rssi).toFixed(1)),
        avgSnr: signalSummaryRow?.avg_snr == null ? null : Number(Number(signalSummaryRow.avg_snr).toFixed(1)),
        medianSnr: signalSummaryRow?.median_snr == null ? null : Number(Number(signalSummaryRow.median_snr).toFixed(1)),
        rssiSamples24h: Number(signalSummaryRow?.rssi_samples ?? 0),
        snrSamples24h: Number(signalSummaryRow?.snr_samples ?? 0),
      },
      routeTypes: routeTypesResult.rows.map((r: any) => ({
        label: ROUTE_LABELS[String(r.route_type ?? 'Unknown')]?.label ?? `Route ${r.route_type}`,
        description: ROUTE_LABELS[String(r.route_type ?? 'Unknown')]?.description ?? 'Unrecognised route type',
        routeType: r.route_type,
        count: Number(r.count ?? 0),
      })),
      transportCodes: transportCodesResult.rows.map((r: any) => {
        const decoded = decodeTransportCodes(r.transport_code);
        const regionScope = r.region_scope == null ? null : String(r.region_scope);
        return {
          ...decoded,
          regionScope,
          label: regionScope
            ? `${regionScope} scope`
            : decoded.scopeCodeHex
              ? `Scope ${decoded.scopeCodeHex}`
              : decoded.raw,
          description: decoded.returnCode && decoded.returnCode > 0
            ? `Return/home ${decoded.returnCodeHex}`
            : 'No return/home region code',
          count: Number(r.count ?? 0),
        };
      }),
      pathDecodeTrend: pathDecodeTrendResult.rows.map((r: any) => {
        const multibyte = Number(r.multibyte_count ?? 0);
        const fullyDecoded = Number(r.fully_decoded_count ?? 0);
        return {
          day: fmtDay(r.day),
          multibyte,
          fullyDecoded,
          decodedPct: multibyte > 0 ? Number(((fullyDecoded / multibyte) * 100).toFixed(1)) : 0,
        };
      }),
      summary: {
        totalPackets24h,
        totalPackets7d: Number((sumResult.rows[0] as any).total_7d),
        uniqueRadios24h: Number((sumResult.rows[0] as any).unique_radios_24h),
        peakHour: peakRow ? fmtHour((peakRow as any).hour) : null,
        peakHourCount: peakRow ? Number((peakRow as any).count) : 0,
      },
    };
  }

  function getCachedCharts(key: string): { ts: number; data: unknown } | undefined {
    const cached = chartsCache.get(key);
    if (!cached) return undefined;
    const ageMs = Date.now() - cached.ts;
    if (
      !Number.isFinite(cached.ts)
      || ageMs < -CHART_SNAPSHOT_MAX_FUTURE_SKEW_MS
    ) {
      chartsCache.delete(key);
      return undefined;
    }
    return cached;
  }

  async function loadPersistedCharts(
    scope: string,
    key: string,
    visibilityGeneration: number,
  ): Promise<{ ts: number; data: unknown } | undefined> {
    const existing = chartSnapshotLoads.get(key);
    if (existing) return existing;
    let load!: Promise<{ ts: number; data: unknown } | undefined>;
    load = repository.loadChartSnapshot(scope, visibilityGeneration)
      .then((row) => {
        if (
          !row
          || row.scope_key !== scope
        ) {
          return undefined;
        }
        const validated = validateChartSnapshotPayload(
          row.payload,
          scope,
          chartsSnapshotStaleTtlMs,
          Date.now(),
          undefined,
          { allowExpired: true },
        );
        const storedGeneratedAtMs = new Date(row.generated_at).getTime();
        if (
          !validated
          || !Number.isFinite(storedGeneratedAtMs)
          || Math.abs(storedGeneratedAtMs - validated.generatedAtMs) > 1_000
        ) {
          console.warn(`[stats] ignored invalid persisted chart snapshot scope=${scope}`);
          return undefined;
        }
        const cached = { ts: validated.generatedAtMs, data: validated.payload };
        chartsCache.set(key, cached);
        return cached;
      })
      .catch((error: unknown) => {
        console.warn(
          `[stats] persisted chart snapshot load failed scope=${scope}:`,
          error instanceof Error ? error.message : 'unknown error',
        );
        return undefined;
      })
      .finally(() => {
        if (chartSnapshotLoads.get(key) === load) chartSnapshotLoads.delete(key);
      });
    chartSnapshotLoads.set(key, load);
    return load;
  }

  function beginCanonicalChartRefresh(
    network: string | undefined,
    scope: string,
    key: string,
    visibilityGeneration: number,
  ): Promise<unknown> {
    const existing = chartsInflight.get(scope);
    if (existing) return existing;
    if (chartsInflight.size >= MAX_UNIQUE_STATS_INFLIGHT) {
      throw new StatsWorkOverloadedError();
    }
    let refresh!: Promise<unknown>;
    refresh = (async () => {
      if (initialStatsWarmup) {
        await initialStatsWarmup.catch(() => {
          // Chart refresh can still proceed if the lightweight warmup failed.
        });
      }
      const data = await computeChartsData(
        network,
        undefined,
        visibilityGeneration,
      );
      const validated = validateChartSnapshotPayload(
        data,
        scope,
        chartsSnapshotStaleTtlMs,
        Date.now(),
        visibilityGeneration,
      );
      if (!validated) {
        throw new Error('computed chart snapshot failed completeness validation');
      }
      const published = await repository.saveChartSnapshot(
        scope,
        validated.payload,
        visibilityGeneration,
        chartsSnapshotStaleTtlMs,
      ).catch((error: unknown) => {
        // A transient persistence failure must not discard a newly completed
        // in-memory response, but it is visible for operators and retried on
        // the next successful refresh.
        console.warn(
          `[stats] chart snapshot persistence failed scope=${scope}:`,
          error instanceof Error ? error.message : 'unknown error',
        );
        return false;
      });
      if (!published) {
        throw new Error('chart snapshot privacy generation changed before publication');
      }
      chartsCache.set(key, { ts: validated.generatedAtMs, data: validated.payload });
      return validated.payload;
    })().finally(() => {
      if (chartsInflight.get(scope) === refresh) chartsInflight.delete(scope);
    });
    chartsInflight.set(scope, refresh);
    return refresh;
  }

  async function getCharts(network: string | undefined, observer: string | undefined): Promise<unknown> {
    if (observer) {
      if (activeObserverWork >= MAX_UNIQUE_STATS_INFLIGHT) throw new StatsWorkOverloadedError();
      activeObserverWork += 1;
      try {
        for (let attempt = 0; attempt < 2; attempt += 1) {
          const visibilityGeneration = await getPublicVisibilityGeneration();
          const data = await computeChartsData(network, observer, visibilityGeneration);
          if (await getPublicVisibilityGeneration() === visibilityGeneration) return data;
        }
        throw new Error('chart privacy generation changed during observer computation');
      } finally {
        activeObserverWork -= 1;
      }
    }
    const scope = `${network ?? 'ukmesh'}`;
    const visibilityGeneration = await getPublicVisibilityGeneration();
    const key = `${scope}:v${visibilityGeneration}`;
    let cached = getCachedCharts(key);
    if (!cached) {
      // Durable complete snapshots are loaded before any analytical query. A
      // cold process can therefore answer immediately and refresh in the
      // background, while observer-scoped data never crosses this boundary.
      cached = await loadPersistedCharts(scope, key, visibilityGeneration);
    }
    const cachedVisibilityGeneration = Number(
      (cached?.data as { snapshot?: { visibilityGeneration?: unknown } } | undefined)
        ?.snapshot?.visibilityGeneration,
    );
    if (
      cached
      && cachedVisibilityGeneration === visibilityGeneration
      && Date.now() - cached.ts <= chartsSnapshotStaleTtlMs
    ) {
      // A cold process must treat the persisted snapshot's durable max-age as
      // authoritative. The shorter in-memory cadence is only how often we
      // check freshness; it must not force a multi-minute rebuild after every
      // restart. Only the time-dependent region health score changes here.
      const refreshed = refreshCachedRegionHealth(cached.data);
      chartsCache.set(key, { ts: cached.ts, data: refreshed });
      return refreshed;
    }

    let promise: Promise<unknown>;
    try {
      promise = beginCanonicalChartRefresh(
        network,
        scope,
        key,
        visibilityGeneration,
      );
    } catch (error) {
      if (cached) return refreshCachedRegionHealth(cached.data);
      throw error;
    }
    if (cached) {
      // Even an expired canonical snapshot is complete and privacy-filtered.
      // Serve it while one refresh runs, and retain it if that refresh fails.
      void promise.catch(() => { /* retain the last successful value */ });
      return refreshCachedRegionHealth(cached.data);
    }
    return promise;
  }

  function startChartsWarmup(): void {
    const warmupNetworks = (process.env['WARMUP_NETWORKS'] ?? 'ukmesh,test')
      .split(',').map((s: string) => s.trim()).filter(Boolean);

    const warmCharts = async () => {
      for (const net of warmupNetworks) {
        await getCharts(net, undefined).catch(() => { /* best-effort */ });
      }
    };

    const warmStats = async () => {
      for (const net of warmupNetworks) {
        await getStatsSummary(net, undefined).catch(() => { /* best-effort */ });
      }
    };

    const coldRegenDeferMs = Math.max(
      0,
      Math.min(15 * 60_000, Number(process.env['CHARTS_COLD_REGEN_DEFER_MS'] ?? 60_000) || 0),
    );

    // Populate the lightweight summary first, then defer the persisted-snapshot
    // check. Fresh durable rows never regenerate; missing or genuinely stale
    // rows can regenerate after WS initial-state has had time to warm.
    initialStatsWarmup = new Promise<void>((resolve) => {
      setTimeout(resolve, 5_000);
    }).then(warmStats);
    void initialStatsWarmup.finally(() => {
      setTimeout(() => void warmCharts(), coldRegenDeferMs).unref();
    });
    setInterval(warmStats, statsCacheTtlMs);
    setInterval(warmCharts, chartsCacheTtlMs);
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
    if (observer) {
      if (activeObserverWork >= MAX_UNIQUE_STATS_INFLIGHT) throw new StatsWorkOverloadedError();
      activeObserverWork += 1;
      try {
        for (let attempt = 0; attempt < 2; attempt += 1) {
          const visibilityGeneration = await getPublicVisibilityGeneration();
          const data = await computeStatsSummary(network, observer);
          if (await getPublicVisibilityGeneration() === visibilityGeneration) return data;
        }
        throw new Error('statistics privacy generation changed during observer computation');
      } finally {
        activeObserverWork -= 1;
      }
    }
    const scope = `${network ?? 'ukmesh'}`;
    const visibilityGeneration = await getPublicVisibilityGeneration();
    const key = `${scope}:v${visibilityGeneration}`;
    const cached = statsCache.get(key);
    if (cached && Date.now() - cached.ts < statsCacheTtlMs) {
      return cached.data;
    }

    const inflight = statsInflight.get(key);
    if (inflight) return cached ? cached.data : inflight;
    if (statsInflight.size >= MAX_UNIQUE_STATS_INFLIGHT) {
      if (cached) return cached.data;
      throw new StatsWorkOverloadedError();
    }

    const metricNetwork = scope;
    const stopTimer = statsRecomputeDuration.startTimer({ network: metricNetwork });
    const promise = computeStatsSummary(network, observer).then(async (data) => {
      if (await getPublicVisibilityGeneration() !== visibilityGeneration) {
        throw new Error('statistics privacy generation changed before cache publication');
      }
      stopTimer({ status: 'success' });
      statsRecomputeTotal.inc({ network: metricNetwork, status: 'success' });
      statsCache.set(key, { ts: Date.now(), data });
      statsInflight.delete(key);
      return data;
    }).catch((err) => {
      stopTimer({ status: 'failed' });
      statsRecomputeTotal.inc({ network: metricNetwork, status: 'failed' });
      statsInflight.delete(key);
      throw err;
    });

    statsInflight.set(key, promise);
    if (cached) {
      void promise.catch(() => { /* retain the last successful value */ });
      return cached.data;
    }
    return promise;
  }

  async function getObserverActivity(network: string | undefined): Promise<unknown> {
    const key = `${network ?? 'ukmesh'}`;
    const cached = observerActivityCache.get(key);
    if (cached && Date.now() - cached.ts < statsCacheTtlMs) {
      return cached.data;
    }

    const inflight = observerActivityInflight.get(key);
    if (inflight) return cached ? cached.data : inflight;
    if (observerActivityInflight.size >= MAX_UNIQUE_STATS_INFLIGHT) {
      if (cached) return cached.data;
      throw new StatsWorkOverloadedError();
    }

    const promise = repository.fetchObserverActivity(network)
      .then((result) => {
        const data = result.rows.map((r) => ({ ...r, rx_24h: Number(r.rx_24h), tx_24h: Number(r.tx_24h) }));
        observerActivityCache.set(key, { ts: Date.now(), data });
        observerActivityInflight.delete(key);
        return data;
      })
      .catch((err) => {
        observerActivityInflight.delete(key);
        throw err;
      });

    observerActivityInflight.set(key, promise);
    if (cached) {
      void promise.catch(() => { /* retain the last successful value */ });
      return cached.data;
    }
    return promise;
  }

  return {
    startChartsWarmup,
    getCharts,
    getStatsSummary,
    getObserverActivity,
  };
}
