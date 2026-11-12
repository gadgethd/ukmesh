import { Counter, Gauge, Histogram } from 'prom-client';
import { metricsRegistry } from './registry.js';

export const statsRecomputeTotal = new Counter({
  name: 'meshcore_stats_recompute_total',
  help: 'Cold statistics recomputations.',
  labelNames: ['network', 'status'] as const,
  registers: [metricsRegistry],
});

export const statsRecomputeDuration = new Histogram({
  name: 'meshcore_stats_recompute_duration_seconds',
  help: 'Cold statistics recomputation duration.',
  labelNames: ['network', 'status'] as const,
  buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120],
  registers: [metricsRegistry],
});

export const activityTimelineRefreshInflight = new Gauge({
  name: 'meshcore_activity_timeline_refresh_inflight',
  help: 'Coalesced or background timeline fills currently running (maximum four per API router).',
  registers: [metricsRegistry],
});
