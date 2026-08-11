import assert from 'node:assert/strict';
import test from 'node:test';
import { BoundedTtlMap } from '../cache/boundedTtlMap.js';
import { computeRegionHealth, createStatsService } from './statsService.js';
import type { StatsRepository } from './statsRepository.js';

const NOW = Date.parse('2026-07-11T16:00:00Z');

test('region health rewards fresh traffic and observer redundancy', () => {
  assert.deepEqual(computeRegionHealth({
    activeObservers: 3,
    observers: 4,
    packets24h: 10_000,
    lastPacketAt: '2026-07-11T15:59:00Z',
  }, NOW), {
    score: 100,
    status: 'healthy',
    factors: { freshness: 100, observerAvailability: 100, traffic: 100, observerDiversity: 100 },
  });
});

test('region health identifies stale regions without active observers', () => {
  const result = computeRegionHealth({ activeObservers: 0, observers: 1, packets24h: 10, lastPacketAt: '2026-07-11T12:00:00Z' }, NOW);
  assert.equal(result.status, 'poor');
  assert.ok(result.score < 30);
  assert.equal(result.factors.freshness, 0);
});

function emptyChartsData() {
  const result = { rows: [] };
  return {
    phResult: result, pdResult: result, rhResult: result, rdResult: result,
    ptResult: result, hdResult: result, pcResult: result, sumResult: { rows: [{}] },
    orSummaryResult: result, orSeriesResult: result, pathHashWidthsResult: result,
    multibyteSummaryResult: result, observerDiversityResult: result, signalSummaryResult: result,
    routeTypesResult: result, transportCodesResult: result, pathDecodeTrendResult: result,
  };
}

test('completed canonical charts are reused while observer-scoped charts are never persisted', async () => {
  let chartCalls = 0;
  let regionSummaryCalls = 0;
  let snapshotLoads = 0;
  let snapshotSaves = 0;
  let admittedChartRefreshes = 0;
  const repository = {
    loadChartSnapshot: async () => {
      snapshotLoads += 1;
      return null;
    },
    saveChartSnapshot: async () => {
      snapshotSaves += 1;
      return true;
    },
    fetchChartsData: async () => {
      chartCalls += 1;
      return emptyChartsData();
    },
    fetchChannelTraffic: async () => ({ rows: [] }),
    fetchObserverRegionSummary: async () => {
      regionSummaryCalls += 1;
      return { rows: [] };
    },
  } as unknown as StatsRepository;
  const chartsCache = new Map<string, { ts: number; data: unknown }>();
  const service = createStatsService({
    statsCache: new Map(),
    statsCacheTtlMs: 60_000,
    chartsCache,
    chartsCacheTtlMs: 60_000,
    chartsInflight: new Map(),
    repository,
    getPublicVisibilityGeneration: async () => 1,
    maskDecodedPathNodes: () => [],
    runHeavyWork: async (workload, task) => {
      assert.equal(workload, 'chart-refresh:ukmesh');
      admittedChartRefreshes += 1;
      return task();
    },
  });

  await service.getCharts('ukmesh', undefined);
  await service.getCharts('ukmesh', undefined);
  assert.equal(chartCalls, 1);
  assert.equal(regionSummaryCalls, 0);
  assert.equal(chartsCache.size, 1);
  assert.equal(snapshotLoads, 1);
  assert.equal(snapshotSaves, 1);
  assert.equal(admittedChartRefreshes, 1);

  await service.getCharts('ukmesh', 'A'.repeat(64));
  await service.getCharts('ukmesh', 'A'.repeat(64));
  assert.equal(chartCalls, 3);
  assert.equal(chartsCache.size, 1);
  assert.equal(snapshotLoads, 1);
  assert.equal(snapshotSaves, 1);
  assert.equal(admittedChartRefreshes, 1);
});

test('a valid durable chart snapshot serves a cold process without analytical queries', async () => {
  // Older than the 30-minute in-memory cadence, but still inside the durable
  // six-hour max age: this is the exact backend-restart regression guard.
  const generatedAt = new Date(Date.now() - 45 * 60_000).toISOString();
  const durable = {
    snapshot: {
      status: 'complete',
      generatedAt,
      scope: 'ukmesh',
      visibilityGeneration: 1,
    },
    packetsPerHour: [{ hour: '11:00', count: 3 }],
  };
  let chartCalls = 0;
  let saves = 0;
  const repository = {
    loadChartSnapshot: async () => ({
      scope_key: 'ukmesh',
      schema_version: 2,
      visibility_generation: '1',
      generated_at: generatedAt,
      payload: durable,
    }),
    saveChartSnapshot: async () => {
      saves += 1;
      return true;
    },
    fetchChartsData: async () => {
      chartCalls += 1;
      return emptyChartsData();
    },
    fetchChannelTraffic: async () => ({ rows: [] }),
  } as unknown as StatsRepository;
  const service = createStatsService({
    statsCache: new Map(),
    statsCacheTtlMs: 60_000,
    chartsCache: new Map(),
    chartsCacheTtlMs: 30 * 60_000,
    chartsSnapshotStaleTtlMs: 6 * 60 * 60_000,
    chartsInflight: new Map(),
    repository,
    getPublicVisibilityGeneration: async () => 1,
    maskDecodedPathNodes: () => [],
  });

  assert.deepEqual(await service.getCharts('ukmesh', undefined), durable);
  assert.equal(chartCalls, 0);
  assert.equal(saves, 0);
});

test('a complete older-generation snapshot is served while one scope refresh runs', async () => {
  const generatedAt = new Date(Date.now() - 60_000).toISOString();
  const oldPayload = {
    snapshot: {
      status: 'complete',
      generatedAt,
      scope: 'ukmesh',
      visibilityGeneration: 1,
    },
    marker: 'old-generation',
  };
  let resolveRefresh!: (value: ReturnType<typeof emptyChartsData>) => void;
  const refresh = new Promise<ReturnType<typeof emptyChartsData>>((resolve) => {
    resolveRefresh = resolve;
  });
  let chartCalls = 0;
  const repository = {
    loadChartSnapshot: async () => ({
      scope_key: 'ukmesh',
      schema_version: 2,
      visibility_generation: '1',
      generated_at: generatedAt,
      payload: oldPayload,
    }),
    saveChartSnapshot: async () => true,
    fetchChartsData: async () => {
      chartCalls += 1;
      return refresh;
    },
    fetchChannelTraffic: async () => ({ rows: [] }),
  } as unknown as StatsRepository;
  const service = createStatsService({
    statsCache: new Map(),
    statsCacheTtlMs: 60_000,
    chartsCache: new Map(),
    chartsCacheTtlMs: 30 * 60_000,
    chartsSnapshotStaleTtlMs: 6 * 60 * 60_000,
    chartsInflight: new Map(),
    repository,
    getPublicVisibilityGeneration: async () => 2,
    maskDecodedPathNodes: () => [],
  });

  const first = await service.getCharts('ukmesh', undefined) as {
    snapshot: { visibilityGeneration: number };
    marker?: string;
  };
  const second = await service.getCharts('ukmesh', undefined) as typeof first;
  assert.equal(chartCalls, 1);
  assert.equal(first.snapshot.visibilityGeneration, 1);
  assert.equal(first.marker, 'old-generation');
  assert.deepEqual(second, first);
  resolveRefresh(emptyChartsData());
});

test('a detached chart refresh reports its actual failure while retaining the old snapshot', async () => {
  const generatedAt = new Date(Date.now() - 60_000).toISOString();
  const warnings: unknown[][] = [];
  const originalWarn = console.warn;
  console.warn = (...args: unknown[]) => { warnings.push(args); };
  try {
    const repository = {
      loadChartSnapshot: async () => ({
        scope_key: 'ukmesh',
        schema_version: 2,
        visibility_generation: '1',
        generated_at: generatedAt,
        payload: {
          snapshot: { status: 'complete', generatedAt, scope: 'ukmesh', visibilityGeneration: 1 },
        },
      }),
      fetchChartsData: async () => { throw new Error('captured database failure'); },
      fetchChannelTraffic: async () => ({ rows: [] }),
    } as unknown as StatsRepository;
    const service = createStatsService({
      statsCache: new Map(),
      statsCacheTtlMs: 60_000,
      chartsCache: new Map(),
      chartsCacheTtlMs: 30 * 60_000,
      chartsSnapshotStaleTtlMs: 6 * 60 * 60_000,
      chartsInflight: new Map(),
      repository,
      getPublicVisibilityGeneration: async () => 2,
      maskDecodedPathNodes: () => [],
    });

    const served = await service.getCharts('ukmesh', undefined) as {
      snapshot: { visibilityGeneration: number };
    };
    assert.equal(served.snapshot.visibilityGeneration, 1);
    await new Promise((resolve) => setImmediate(resolve));
    assert.ok(warnings.some((args) => args.join(' ').includes('captured database failure')));
  } finally {
    console.warn = originalWarn;
  }
});

test('a visibility change at chart publication fails closed without caching the result', async () => {
  const chartsCache = new Map<string, { ts: number; data: unknown }>();
  const repository = {
    loadChartSnapshot: async () => null,
    saveChartSnapshot: async () => false,
    fetchChartsData: async () => emptyChartsData(),
    fetchChannelTraffic: async () => ({ rows: [] }),
  } as unknown as StatsRepository;
  const service = createStatsService({
    statsCache: new Map(),
    statsCacheTtlMs: 60_000,
    chartsCache,
    chartsCacheTtlMs: 30 * 60_000,
    chartsInflight: new Map(),
    repository,
    getPublicVisibilityGeneration: async () => 1,
    maskDecodedPathNodes: () => [],
  });

  await assert.rejects(
    service.getCharts('ukmesh', undefined),
    /privacy generation changed before publication/,
  );
  assert.equal(chartsCache.size, 0);
});

test('expired persisted canonical charts are served while one refresh runs in the background', async () => {
  let resolveRefresh!: (value: ReturnType<typeof emptyChartsData>) => void;
  const refresh = new Promise<ReturnType<typeof emptyChartsData>>((resolve) => {
    resolveRefresh = resolve;
  });
  let chartCalls = 0;
  const generatedAt = new Date(Date.now() - 2 * 60_000).toISOString();
  const stale = {
    snapshot: {
      status: 'complete',
      generatedAt,
      scope: 'ukmesh',
      visibilityGeneration: 1,
    },
    marker: 'last-complete',
  };
  const repository = {
    loadChartSnapshot: async () => ({
      scope_key: 'ukmesh',
      schema_version: 2,
      visibility_generation: '1',
      generated_at: generatedAt,
      payload: stale,
    }),
    saveChartSnapshot: async () => true,
    fetchChartsData: async () => {
      chartCalls += 1;
      return refresh;
    },
    fetchChannelTraffic: async () => ({ rows: [] }),
  } as unknown as StatsRepository;
  const chartsCache = new BoundedTtlMap<string, { ts: number; data: unknown }>({
    maxEntries: 2,
    maxWeight: 1024 * 1024,
    ttlMs: 60_000,
  });
  const chartsInflight = new Map<string, Promise<unknown>>();
  const service = createStatsService({
    statsCache: new Map(),
    statsCacheTtlMs: 60_000,
    chartsCache,
    chartsCacheTtlMs: 1_000,
    chartsSnapshotStaleTtlMs: 60_000,
    chartsInflight,
    repository,
    getPublicVisibilityGeneration: async () => 1,
    maskDecodedPathNodes: () => [],
  });

  assert.equal(await service.getCharts('ukmesh', undefined), stale);
  assert.equal(await service.getCharts('ukmesh', undefined), stale);
  assert.equal(chartCalls, 1);

  resolveRefresh(emptyChartsData());
  await chartsInflight.get('ukmesh');
  assert.notEqual(chartsCache.get('ukmesh:v1')?.data, stale);
  chartsCache.shutdown();
});

test('startup chart work waits for the lightweight summary warmup', async () => {
  let resolveSummary!: (value: Awaited<ReturnType<StatsRepository['fetchStatsSummary']>>) => void;
  const summary = new Promise<Awaited<ReturnType<StatsRepository['fetchStatsSummary']>>>((resolve) => {
    resolveSummary = resolve;
  });
  let chartCalls = 0;
  const repository = {
    loadChartSnapshot: async () => null,
    saveChartSnapshot: async () => true,
    fetchStatsSummary: async () => summary,
    fetchChartsData: async () => {
      chartCalls += 1;
      return emptyChartsData();
    },
    fetchChannelTraffic: async () => ({ rows: [] }),
  } as unknown as StatsRepository;
  const service = createStatsService({
    statsCache: new Map(),
    statsCacheTtlMs: 60_000,
    chartsCache: new Map(),
    chartsCacheTtlMs: 60_000,
    chartsInflight: new Map(),
    repository,
    getPublicVisibilityGeneration: async () => 1,
    maskDecodedPathNodes: () => [],
  });

  const originalSetTimeout = globalThis.setTimeout;
  const originalSetInterval = globalThis.setInterval;
  globalThis.setTimeout = ((callback: (...args: unknown[]) => void) => {
    queueMicrotask(callback);
    return { unref() {} } as unknown as NodeJS.Timeout;
  }) as typeof setTimeout;
  globalThis.setInterval = (() => ({ unref() {} } as unknown as NodeJS.Timeout)) as typeof setInterval;
  try {
    service.startChartsWarmup();
    const charts = service.getCharts('ukmesh', undefined);
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(chartCalls, 0);

    const empty = { rows: [] };
    resolveSummary({
      mqttCount: empty,
      packetCount: empty,
      staleCount: empty,
      mapNodeCount: empty,
      totalNodeCount: empty,
      longestHopCount: empty,
      nodesDayCount: empty,
      internationalCount: empty,
    });
    await charts;
    assert.ok(chartCalls >= 1);
  } finally {
    globalThis.setTimeout = originalSetTimeout;
    globalThis.setInterval = originalSetInterval;
  }
});

test('expired canonical stats are served while one refresh runs in the background', async () => {
  let resolveRefresh!: (value: Awaited<ReturnType<StatsRepository['fetchStatsSummary']>>) => void;
  const refresh = new Promise<Awaited<ReturnType<StatsRepository['fetchStatsSummary']>>>((resolve) => {
    resolveRefresh = resolve;
  });
  let summaryCalls = 0;
  const repository = {
    fetchStatsSummary: async () => {
      summaryCalls += 1;
      return refresh;
    },
  } as unknown as StatsRepository;
  const stale = { totalNodes: 6 };
  const statsCache = new BoundedTtlMap<string, { ts: number; data: unknown }>({
    maxEntries: 2,
    maxWeight: 1024,
    ttlMs: 60_000,
  });
  statsCache.set('ukmesh:v1', { ts: 0, data: stale });
  const service = createStatsService({
    statsCache,
    statsCacheTtlMs: 1,
    chartsCache: new Map(),
    chartsCacheTtlMs: 60_000,
    chartsInflight: new Map(),
    repository,
    getPublicVisibilityGeneration: async () => 1,
    maskDecodedPathNodes: () => [],
  });

  assert.equal(await service.getStatsSummary('ukmesh', undefined), stale);
  assert.equal(await service.getStatsSummary('ukmesh', undefined), stale);
  assert.equal(summaryCalls, 1);

  const empty = { rows: [] };
  resolveRefresh({
    mqttCount: empty,
    packetCount: empty,
    staleCount: empty,
    mapNodeCount: empty,
    totalNodeCount: { rows: [{ count: '7' }] },
    longestHopCount: empty,
    nodesDayCount: empty,
    internationalCount: empty,
  });
  await refresh;
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal((statsCache.get('ukmesh:v1')?.data as { totalNodes: number }).totalNodes, 7);
  statsCache.shutdown();
});

test('canonical summary cache entries cannot cross a privacy generation change', async () => {
  let visibilityGeneration = 1;
  let summaryCalls = 0;
  const empty = { rows: [] };
  const repository = {
    fetchStatsSummary: async () => {
      summaryCalls += 1;
      return {
        mqttCount: empty,
        packetCount: empty,
        staleCount: empty,
        mapNodeCount: empty,
        totalNodeCount: { rows: [{ count: String(summaryCalls) }] },
        longestHopCount: empty,
        nodesDayCount: empty,
        internationalCount: empty,
      };
    },
  } as unknown as StatsRepository;
  const statsCache = new Map<string, { ts: number; data: unknown }>();
  const service = createStatsService({
    statsCache,
    statsCacheTtlMs: 60_000,
    chartsCache: new Map(),
    chartsCacheTtlMs: 60_000,
    chartsInflight: new Map(),
    repository,
    getPublicVisibilityGeneration: async () => visibilityGeneration,
    maskDecodedPathNodes: () => [],
  });

  assert.equal(
    (await service.getStatsSummary('ukmesh', undefined) as { totalNodes: number }).totalNodes,
    1,
  );
  visibilityGeneration = 2;
  assert.equal(
    (await service.getStatsSummary('ukmesh', undefined) as { totalNodes: number }).totalNodes,
    2,
  );
  assert.equal(summaryCalls, 2);
  assert.ok(statsCache.has('ukmesh:v1'));
  assert.ok(statsCache.has('ukmesh:v2'));
});
