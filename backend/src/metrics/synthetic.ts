import { Counter, Gauge } from 'prom-client';
import { metricsRegistry } from './registry.js';
import { boundedLabel } from './labels.js';
import { observeWorkerOutcome } from './workers.js';

export const syntheticCheckSuccess = new Gauge({
  name: 'meshcore_synthetic_check_success',
  help: 'Latest synthetic check result (1 for success, 0 for failure).',
  labelNames: ['check'] as const,
  registers: [metricsRegistry],
});

export const syntheticCheckOutcomesTotal = new Counter({
  name: 'meshcore_synthetic_check_outcomes_total',
  help: 'Synthetic check executions by bounded check and outcome.',
  labelNames: ['check', 'outcome'] as const,
  registers: [metricsRegistry],
});

const METRIC_SYNTHETIC_CHECKS = new Set([
  'http_liveness',
  'dependency_readiness',
  'stats_api',
  'websocket_ping',
  'websocket_initial_state',
]);

export function observeSyntheticCheck(check: string, succeeded: boolean): void {
  const boundedCheck = boundedLabel(check, METRIC_SYNTHETIC_CHECKS);
  syntheticCheckSuccess.set({ check: boundedCheck }, succeeded ? 1 : 0);
  syntheticCheckOutcomesTotal.inc({
    check: boundedCheck,
    outcome: succeeded ? 'success' : 'failure',
  });
  observeWorkerOutcome('synthetic', 'check', succeeded ? 'success' : 'failure');
}
