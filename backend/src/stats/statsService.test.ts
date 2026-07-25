import assert from 'node:assert/strict';
import test from 'node:test';
import { computeRegionHealth, createStatsService } from './statsService.js';

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

test('identity-bearing chart responses are recomputed after privacy changes', async () => {
  let chartFetches = 0;
  const emptyResult = () => ({ rows: [] });
  const service = createStatsService({
    statsCache: new Map(),
    statsCacheTtlMs: 60_000,
    chartsCache: new Map(),
    chartsCacheTtlMs: 60_000,
    chartsInflight: new Map(),
    repository: {
      fetchChartsData: async () => {
        chartFetches += 1;
        return {
          phResult: emptyResult(),
          pdResult: emptyResult(),
          rhResult: emptyResult(),
          rdResult: emptyResult(),
          ptResult: emptyResult(),
          hdResult: emptyResult(),
          pcResult: emptyResult(),
          sumResult: { rows: [{}] },
          orSummaryResult: emptyResult(),
          orSeriesResult: emptyResult(),
          pathHashWidthsResult: emptyResult(),
          multibyteSummaryResult: emptyResult(),
          observerDiversityResult: emptyResult(),
          signalSummaryResult: emptyResult(),
          routeTypesResult: emptyResult(),
          transportCodesResult: emptyResult(),
          pathDecodeTrendResult: emptyResult(),
        };
      },
      fetchChannelTraffic: async () => emptyResult(),
    } as any,
    maskDecodedPathNodes: () => [],
  });

  await service.getCharts('ukmesh', undefined);
  await service.getCharts('ukmesh', undefined);
  assert.equal(chartFetches, 2);
});
