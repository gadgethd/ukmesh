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
  const repository = {
    fetchChartsData: async () => {
      chartCalls += 1;
      return emptyChartsData();
    },
    fetchChannelTraffic: async () => ({ rows: [] }),
    fetchObserverRegionSummary: async () => ({ rows: [] }),
  } as unknown as StatsRepository;
  const chartsCache = new Map<string, { ts: number; data: unknown }>();
  const service = createStatsService({
    statsCache: new Map(),
    statsCacheTtlMs: 60_000,
    chartsCache,
    chartsCacheTtlMs: 60_000,
    chartsInflight: new Map(),
    repository,
    maskDecodedPathNodes: () => [],
  });

  await service.getCharts('ukmesh', undefined);
  await service.getCharts('ukmesh', undefined);
  assert.equal(chartCalls, 1);
  assert.equal(chartsCache.size, 1);

  await service.getCharts('ukmesh', 'A'.repeat(64));
  await service.getCharts('ukmesh', 'A'.repeat(64));
  assert.equal(chartCalls, 3);
  assert.equal(chartsCache.size, 1);
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
  statsCache.set('ukmesh', { ts: 0, data: stale });
  const service = createStatsService({
    statsCache,
    statsCacheTtlMs: 1,
    chartsCache: new Map(),
    chartsCacheTtlMs: 60_000,
    chartsInflight: new Map(),
    repository,
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
  assert.equal((statsCache.get('ukmesh')?.data as { totalNodes: number }).totalNodes, 7);
  statsCache.shutdown();
});
