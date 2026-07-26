import assert from 'node:assert/strict';
import test from 'node:test';
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
