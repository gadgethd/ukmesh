import { Counter, Gauge } from 'prom-client';
import { metricsRegistry } from './registry.js';

export const srtmRequestsTotal = new Counter({
  name: 'meshcore_srtm_requests_total',
  help: 'SRTM tile request outcomes.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const backupAgeSeconds = new Gauge({
  name: 'meshcore_backup_age_seconds',
  help: 'Age of the latest verified backup receipt.',
  labelNames: ['dataset'] as const,
  registers: [metricsRegistry],
});

export const packetPathsRows30d = new Gauge({
  name: 'meshcore_packet_paths_rows_30d',
  help: 'Durable packet path rows written in the trailing 30 days.',
  registers: [metricsRegistry],
});

export const packetPathsBytesPerRow = new Gauge({
  name: 'meshcore_packet_paths_bytes_per_row',
  help: 'Approximate total packet_paths bytes per retained row.',
  registers: [metricsRegistry],
});

export const packetPathsOverdueUncompressedChunks = new Gauge({
  name: 'meshcore_packet_paths_overdue_uncompressed_chunks',
  help: 'Packet path chunks older than the 14-day policy plus one-day scheduling allowance that remain uncompressed.',
  registers: [metricsRegistry],
});

export const coreTelemetryHypertableBytes = new Gauge({
  name: 'meshcore_core_telemetry_hypertable_bytes',
  help: 'Total chunk bytes for core high-volume telemetry hypertables.',
  labelNames: ['table'] as const,
  registers: [metricsRegistry],
});

export const coreTelemetryHypertableChunks = new Gauge({
  name: 'meshcore_core_telemetry_hypertable_chunks',
  help: 'Chunk count for core high-volume telemetry hypertables.',
  labelNames: ['table'] as const,
  registers: [metricsRegistry],
});

export const coreTelemetryExpiredChunks = new Gauge({
  name: 'meshcore_core_telemetry_expired_chunks',
  help: 'Fully expired core telemetry chunks still present beyond the reviewed retention window.',
  labelNames: ['table'] as const,
  registers: [metricsRegistry],
});

export const coreTelemetryUncompressedChunks = new Gauge({
  name: 'meshcore_core_telemetry_overdue_uncompressed_chunks',
  help: 'Core telemetry chunks still uncompressed after the configured age plus a one-day schedule allowance.',
  labelNames: ['table'] as const,
  registers: [metricsRegistry],
});
