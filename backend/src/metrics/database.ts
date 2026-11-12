import { Counter, Gauge, Histogram } from 'prom-client';
import { metricsRegistry } from './registry.js';

export const dbPoolTotal = new Gauge({
  name: 'meshcore_db_pool_connections',
  help: 'PostgreSQL pool connections by state.',
  labelNames: ['state'] as const,
  registers: [metricsRegistry],
});

export const dbQueriesTotal = new Counter({
  name: 'meshcore_db_queries_total',
  help: 'Database queries by pool and bounded outcome.',
  labelNames: ['pool', 'outcome'] as const,
  registers: [metricsRegistry],
});

export const dbQueryDuration = new Histogram({
  name: 'meshcore_db_query_duration_seconds',
  help: 'Database query duration by pool and bounded outcome.',
  labelNames: ['pool', 'outcome'] as const,
  buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15, 60, 300],
  registers: [metricsRegistry],
});

export function updateDbPoolMetrics(
  poolName: 'oltp' | 'analytics' | 'owner_auth' | 'ingest',
  snapshot: { totalCount: number; idleCount: number; waitingCount: number },
): void {
  dbPoolTotal.set({ state: `${poolName}_total` }, snapshot.totalCount);
  dbPoolTotal.set({ state: `${poolName}_idle` }, snapshot.idleCount);
  dbPoolTotal.set({ state: `${poolName}_waiting` }, snapshot.waitingCount);
}
