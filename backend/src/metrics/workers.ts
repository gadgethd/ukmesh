import { createCollectorSlot } from './collector.js';
import { Counter, Gauge } from 'prom-client';
import { metricsRegistry } from './registry.js';
import { boundedLabel } from './labels.js';

const collector = createCollectorSlot<(() => Promise<void>) | null>(null);

export function setWorkerHeartbeatCollector(read: (() => Promise<void>) | null): () => void {
  return collector.register(read);
}

export const workerHeartbeatAgeSeconds = new Gauge({
  name: 'meshcore_worker_heartbeat_age_seconds',
  help: 'Age of the last worker heartbeat by bounded worker role.',
  labelNames: ['worker'] as const,
  registers: [metricsRegistry],
  async collect() {
    await collector.read()?.();
  },
});

export const workerOutcomesTotal = new Counter({
  name: 'meshcore_worker_outcomes_total',
  help: 'Worker task outcomes by bounded worker role and phase.',
  labelNames: ['worker', 'phase', 'outcome'] as const,
  registers: [metricsRegistry],
});

const METRIC_WORKERS = new Set([
  'forwarding_outbox',
  'health',
  'privacy_remat',
  'region_scope',
  'path_learning',
  'synthetic',
  'link_backfill',
  'link',
]);

const METRIC_WORKER_PHASES = new Set([
  'handoff',
  'poll',
  'path_capacity',
  'core_capacity',
  'snapshot',
  'pass',
  'chunk',
  'cleanup',
  'rebuild',
  'refresh',
  'check',
  'backfill',
  'job',
  'terrain',
  'rf_compute',
]);

const METRIC_WORKER_OUTCOMES = new Set([
  'success',
  'failure',
  'skipped',
  'retry',
  'dead',
  'recovered',
]);

export function observeWorkerOutcome(worker: string, phase: string, outcome: string): void {
  workerOutcomesTotal.inc({
    worker: boundedLabel(worker, METRIC_WORKERS),
    phase: boundedLabel(phase, METRIC_WORKER_PHASES),
    outcome: boundedLabel(outcome, METRIC_WORKER_OUTCOMES),
  });
}
