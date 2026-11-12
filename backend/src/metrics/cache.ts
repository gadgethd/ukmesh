import { Counter, Gauge } from 'prom-client';
import { metricsRegistry } from './registry.js';

export const cacheOperationsTotal = new Counter({
  name: 'meshcore_cache_operations_total',
  help: 'Cache operations by registered cache and bounded outcome.',
  labelNames: ['cache', 'outcome'] as const,
  registers: [metricsRegistry],
});

export const cacheEntries = new Gauge({
  name: 'meshcore_cache_entries',
  help: 'Current entries by registered bounded cache.',
  labelNames: ['cache'] as const,
  registers: [metricsRegistry],
});

export const cacheBytes = new Gauge({
  name: 'meshcore_cache_bytes',
  help: 'Current estimated bytes by registered bounded cache.',
  labelNames: ['cache'] as const,
  registers: [metricsRegistry],
});
