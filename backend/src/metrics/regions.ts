import { Gauge } from 'prom-client';
import { metricsRegistry } from './registry.js';

export const regionSnapshotHealthy = new Gauge({
  name: 'meshcore_region_snapshot_healthy',
  help: 'Latest background poll found a fresh region snapshot certified for the ready privacy generation.',
  registers: [metricsRegistry],
});

export const regionSnapshotPublishedSeconds = new Gauge({
  name: 'meshcore_region_snapshot_published_seconds',
  help: 'Unix time of the most recently observed region publication.',
  registers: [metricsRegistry],
});

export const regionSnapshotRunning = new Gauge({
  name: 'meshcore_region_snapshot_running',
  help: 'Whether this worker is running one bounded region rebuild child.',
  registers: [metricsRegistry],
});
