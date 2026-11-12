import { Counter, Gauge } from 'prom-client';
import { metricsRegistry } from './registry.js';

export const analysisLeaseEventsTotal = new Counter({
  name: 'meshcore_analysis_lease_events_total',
  help: 'Analysis lease lifecycle events by workload and bounded outcome.',
  labelNames: ['workload', 'outcome'] as const,
  registers: [metricsRegistry],
});

export const analysisActiveLeases = new Gauge({
  name: 'meshcore_analysis_active_leases',
  help: 'Locally observed unexpired analysis leases, bounded to 4096 entries; SQL is authoritative.',
  labelNames: ['workload'] as const,
  registers: [metricsRegistry],
});

export const analysisLeaseTrackingDropsTotal = new Counter({
  name: 'meshcore_analysis_lease_tracking_drops_total',
  help: 'Local analysis lease observations removed by expiry or capacity, not database lease transitions.',
  labelNames: ['workload', 'outcome'] as const,
  registers: [metricsRegistry],
});
