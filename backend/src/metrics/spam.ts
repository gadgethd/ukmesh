import { createCollectorSlot } from './collector.js';
import { Counter, Gauge } from 'prom-client';
import { metricsRegistry } from './registry.js';

export const spamDetectorSnapshotEntries = new Gauge({
  name: 'meshcore_spam_detector_snapshot_entries',
  help: 'Retained spam evidence entries in complete bounded snapshots.',
  labelNames: ['dataset'] as const, registers: [metricsRegistry],
});

export const spamDetectorSnapshotUpdatedSeconds = new Gauge({
  name: 'meshcore_spam_detector_snapshot_updated_seconds',
  help: 'Unix time of the last complete spam evidence snapshot; missing before first success.',
  labelNames: ['dataset'] as const, registers: [metricsRegistry],
});

export const spamDetectorRefreshHealthy = new Gauge({
  name: 'meshcore_spam_detector_refresh_healthy',
  help: 'Whether the most recent spam snapshot refresh succeeded.',
  labelNames: ['dataset'] as const, registers: [metricsRegistry],
});

export const spamDetectorRefreshesTotal = new Counter({
  name: 'meshcore_spam_detector_refreshes_total',
  help: 'Complete spam snapshot refreshes by bounded dataset and outcome.',
  labelNames: ['dataset', 'outcome'] as const, registers: [metricsRegistry],
});

type RuntimeCounts = { trackers: number; arrivals: number; names: number };
const collector = createCollectorSlot<() => RuntimeCounts>(() => ({ trackers: 0, arrivals: 0, names: 0 }));

export function setSpamDetectorRuntimeCollector(read: () => RuntimeCounts): () => void {
  return collector.register(read);
}

export const spamDetectorRuntimeEntries = new Gauge({
  name: 'meshcore_spam_detector_runtime_entries',
  help: 'Runtime spam tracker, arrival and name counts; bounded at 10000, 310000 and 640000.',
  labelNames: ['kind'] as const, registers: [metricsRegistry],
  collect() {
    for (const [kind, count] of Object.entries(collector.read()())) this.set({ kind }, count);
  },
});

export const spamAnalysisTruncationsTotal = new Counter({
  name: 'meshcore_spam_analysis_truncations_total',
  help: 'Spam analysis passes that exceeded a bounded candidate, message or incident limit.',
  labelNames: ['phase'] as const,
  registers: [metricsRegistry],
});
