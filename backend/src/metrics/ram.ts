import { Counter, Gauge, Histogram } from 'prom-client';
import { metricsRegistry } from './registry.js';

export const ramQueueDepth = new Gauge({
  name: 'meshcore_ram_ingest_queue_depth',
  help: 'All entries still resident in the RAM ingest stream, including retained committed overlap.',
  registers: [metricsRegistry],
});

export const ramPendingDepth = new Gauge({
  name: 'meshcore_ram_ingest_pending_depth',
  help: 'Uncommitted observations in the bounded RAM ingest stream sample.',
  registers: [metricsRegistry],
});

export const ramRetainedDepth = new Gauge({
  name: 'meshcore_ram_ingest_retained_depth',
  help: 'Committed observations retained for public-read handoff.',
  registers: [metricsRegistry],
});

export const ramPelDepth = new Gauge({
  name: 'meshcore_ram_ingest_pel_depth',
  help: 'Consumer-group pending-entry-list size.',
  registers: [metricsRegistry],
});

export const ramOldestPelAge = new Gauge({
  name: 'meshcore_ram_ingest_oldest_pel_age_seconds',
  help: 'Age of the oldest consumer-group pending stream ID.',
  registers: [metricsRegistry],
});

export const ramQueueBytes = new Gauge({
  name: 'meshcore_ram_ingest_queue_bytes',
  help: 'Redis memory used by the RAM ingest stream (MEMORY USAGE estimate).',
  registers: [metricsRegistry],
});

export const ramQueueReservedBytes = new Gauge({
  name: 'meshcore_ram_ingest_queue_reserved_bytes',
  help: 'Conservative admission byte budget reserved by pending and retained RAM entries.',
  registers: [metricsRegistry],
});

export const ramEarliestPendingAge = new Gauge({
  name: 'meshcore_ram_ingest_oldest_pending_age_seconds',
  help: 'Age of the oldest packet still pending in the RAM ingest stream.',
  registers: [metricsRegistry],
});

export const ramAdmissionsTotal = new Counter({
  name: 'meshcore_ram_ingest_admissions_total',
  help: 'RAM ingest admissions by outcome (admitted | queue_full | queue_bytes | redis_error).',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const ramAdmissionDuration = new Histogram({
  name: 'meshcore_ram_ingest_admission_duration_seconds',
  help: 'RAM ingest admission (classify + XADD) duration.',
  buckets: [0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5],
  registers: [metricsRegistry],
});

export const ramCommitTotal = new Counter({
  name: 'meshcore_ram_ingest_commit_total',
  help: 'RAM ingest batch commits by outcome.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const ramCommitDuration = new Histogram({
  name: 'meshcore_ram_ingest_commit_duration_seconds',
  help: 'RAM ingest batch commit duration.',
  labelNames: ['outcome'] as const,
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15],
  registers: [metricsRegistry],
});

export const ramCommitBatchSize = new Histogram({
  name: 'meshcore_ram_ingest_commit_batch_size',
  help: 'New packet rows inserted by each successful RAM ingest batch (replays count zero).',
  buckets: [1, 2, 5, 10, 20, 35, 50, 100, 250, 500],
  registers: [metricsRegistry],
});

export const ramVisibilityLatency = new Histogram({
  name: 'meshcore_ram_ingest_visibility_latency_seconds',
  help: 'RAM ingest admission to durable commit latency.',
  buckets: [0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300],
  registers: [metricsRegistry],
});

export const ramRetryTotal = new Counter({
  name: 'meshcore_ram_ingest_retries_total',
  help: 'Transient database retry attempts for RAM ingest batches.',
  registers: [metricsRegistry],
});

export const ramBatchDedupeTotal = new Counter({
  name: 'meshcore_ram_ingest_batch_dedupe_total',
  help: 'Replay-duplicate entries collapsed within a RAM ingest flush.',
  registers: [metricsRegistry],
});

export const ramQuarantinedTotal = new Counter({
  name: 'meshcore_ram_ingest_quarantined_total',
  help: 'Undecodable stream entries moved to the bounded quarantine stream.',
  registers: [metricsRegistry],
});

export const ramSweepDeletedTotal = new Counter({
  name: 'meshcore_ram_ingest_sweep_deleted_total',
  help: 'Committed, acknowledged stream entries reclaimed by the retention sweep.',
  registers: [metricsRegistry],
});

export const ramReclaimedTotal = new Counter({
  name: 'meshcore_ram_ingest_reclaimed_total',
  help: 'Un-acked stream entries reclaimed by a (re)started writer.',
  registers: [metricsRegistry],
});
