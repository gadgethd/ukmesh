import {
  Counter,
  Gauge,
  Histogram,
  Registry,
  collectDefaultMetrics,
} from 'prom-client';
import type { NextFunction, Request, Response } from 'express';

export const metricsRegistry = new Registry();
collectDefaultMetrics({ register: metricsRegistry, prefix: 'meshcore_process_' });

export const mqttMessagesTotal = new Counter({
  name: 'meshcore_mqtt_messages_total',
  help: 'MQTT messages accepted by the ingest handler.',
  labelNames: ['network'] as const,
  registers: [metricsRegistry],
});

export const mqttDeniedPublishesTotal = new Counter({
  name: 'meshcore_mqtt_denied_publishes_total',
  help: 'Denied PUBLISH broker-log observations by bounded topic suffix and scan phase; history may replay at startup.',
  labelNames: ['kind', 'phase'] as const,
  registers: [metricsRegistry],
});

export const ownerAclReloadTotal = new Counter({
  name: 'meshcore_owner_acl_reload_total',
  help: 'Authenticated Mosquitto ACL reload requests by bounded outcome.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const websocketClients = new Gauge({
  name: 'meshcore_websocket_clients',
  help: 'Currently connected WebSocket clients.',
  registers: [metricsRegistry],
});

export const publicLiveOutcomesTotal = new Counter({
  name: 'meshcore_public_live_outcomes_total',
  help: 'Public live events authorized or suppressed at a bounded delivery batch.',
  labelNames: ['phase', 'outcome'] as const,
  registers: [metricsRegistry],
});

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

export const dbPoolTotal = new Gauge({
  name: 'meshcore_db_pool_connections',
  help: 'PostgreSQL pool connections by state.',
  labelNames: ['state'] as const,
  registers: [metricsRegistry],
});

export const webhookDeliveriesTotal = new Counter({
  name: 'meshcore_owner_webhook_deliveries_total',
  help: 'Owner webhook delivery attempts by bounded outcome.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const linkQueueDepth = new Gauge({
  name: 'meshcore_link_queue_active_jobs',
  help: 'Queued and leased durable link jobs.',
  registers: [metricsRegistry],
});

export const linkQueueBytes = new Gauge({
  name: 'meshcore_link_queue_active_bytes',
  help: 'Serialized bytes retained by active durable link jobs.',
  registers: [metricsRegistry],
});

export const linkQueueDeadJobs = new Gauge({
  name: 'meshcore_link_queue_dead_jobs',
  help: 'Retained dead-letter link jobs.',
  registers: [metricsRegistry],
});

export const linkQueueDeadBytes = new Gauge({
  name: 'meshcore_link_queue_dead_bytes',
  help: 'Serialized bytes retained by dead-letter link jobs.',
  registers: [metricsRegistry],
});

export const linkQueueLeases = new Gauge({
  name: 'meshcore_link_queue_active_leases',
  help: 'Currently leased link jobs.',
  registers: [metricsRegistry],
});

export const linkQueueRetries = new Gauge({
  name: 'meshcore_link_queue_retries',
  help: 'Retry attempts retained in active and dead link jobs.',
  registers: [metricsRegistry],
});

export const linkQueueOldestAgeSeconds = new Gauge({
  name: 'meshcore_link_queue_oldest_age_seconds',
  help: 'Age of the oldest active link job.',
  registers: [metricsRegistry],
});

export const httpRequestsTotal = new Counter({
  name: 'meshcore_http_requests_total',
  help: 'HTTP requests by method and bounded status class.',
  labelNames: ['method', 'status_class'] as const,
  registers: [metricsRegistry],
});

export const httpRequestDuration = new Histogram({
  name: 'meshcore_http_request_duration_seconds',
  help: 'HTTP response latency by method and bounded status class.',
  labelNames: ['method', 'status_class'] as const,
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30],
  registers: [metricsRegistry],
});

export const mqttIngestOutcomesTotal = new Counter({
  name: 'meshcore_mqtt_ingest_outcomes_total',
  help: 'MQTT ingest messages by durable handler outcome.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const mqttIngestActive = new Gauge({
  name: 'meshcore_mqtt_ingest_active',
  help: 'MQTT messages currently being processed before acknowledgement.',
  registers: [metricsRegistry],
});

export const packetShareOutcomesTotal = new Counter({
  name: 'meshcore_owner_packet_share_outcomes_total',
  help: 'Owner packet sharing decisions and delivery outcomes.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const packetShareQueueDepth = new Gauge({
  name: 'meshcore_owner_packet_share_queue_depth',
  help: 'Owner packet sharing deliveries currently claimed for processing.',
  registers: [metricsRegistry],
});

export const publicFeedOutcomesTotal = new Counter({
  name: 'meshcore_public_feed_outcomes_total',
  help: 'Public MeshCore feed decisions and delivery outcomes.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const publicFeedQueueDepth = new Gauge({
  name: 'meshcore_public_feed_queue_depth',
  help: 'Public MeshCore feed deliveries currently claimed for processing.',
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

export const mqttConnectionMonitorHealthy = new Gauge({
  name: 'meshcore_mqtt_connection_monitor_healthy',
  help: 'Whether the broker-log monitor has completed a read without outstanding read or audit-write failures.',
  registers: [metricsRegistry],
});

export const mqttConnectionMonitorErrorsTotal = new Counter({
  name: 'meshcore_mqtt_connection_monitor_errors_total',
  help: 'Broker-log monitor failures by bounded phase.',
  labelNames: ['phase'] as const,
  registers: [metricsRegistry],
});

export const packetBatchFlushTotal = new Counter({
  name: 'meshcore_packet_batch_flush_total',
  help: 'Atomic packet batch flushes by bounded outcome.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const packetBatchRetryTotal = new Counter({
  name: 'meshcore_packet_batch_retries_total',
  help: 'Transient database retry attempts for packet batches.',
  registers: [metricsRegistry],
});

export const packetBatchSize = new Histogram({
  name: 'meshcore_packet_batch_size',
  help: 'Packets committed per atomic batch.',
  buckets: [1, 2, 5, 10, 20, 35, 50],
  registers: [metricsRegistry],
});

export const packetBatchDuration = new Histogram({
  name: 'meshcore_packet_batch_duration_seconds',
  help: 'Atomic packet batch flush duration.',
  labelNames: ['outcome'] as const,
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15],
  registers: [metricsRegistry],
});

export const packetBatchQueueDepth = new Gauge({
  name: 'meshcore_packet_batch_queue_depth',
  help: 'Packet batch entries waiting to be persisted.',
  registers: [metricsRegistry],
});

export const packetBatchQueueWait = new Histogram({
  name: 'meshcore_packet_batch_queue_wait_seconds',
  help: 'Time from packet batch enqueue to commit.',
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15],
  registers: [metricsRegistry],
});

export const packetBatchDedupeTotal = new Counter({
  name: 'meshcore_packet_batch_dedupe_total',
  help: 'Duplicate packets collapsed inside a batch before persistence.',
  registers: [metricsRegistry],
});

export const mqttIngestAckDuration = new Histogram({
  name: 'meshcore_mqtt_ingest_ack_duration_seconds',
  help: 'MQTT ingest processing time from receipt to acknowledgement.',
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15, 30],
  registers: [metricsRegistry],
});

export const websocketAdmissionsTotal = new Counter({
  name: 'meshcore_websocket_admissions_total',
  help: 'WebSocket handshake decisions by bounded outcome.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

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

export const analysisLeaseEventsTotal = new Counter({
  name: 'meshcore_analysis_lease_events_total',
  help: 'Analysis lease lifecycle events by workload and bounded outcome.',
  labelNames: ['workload', 'outcome'] as const,
  registers: [metricsRegistry],
});

export const analysisActiveLeases = new Gauge({
  name: 'meshcore_analysis_active_leases',
  help: 'Active analysis leases by bounded workload.',
  labelNames: ['workload'] as const,
  registers: [metricsRegistry],
});

export const spamAnalysisTruncationsTotal = new Counter({
  name: 'meshcore_spam_analysis_truncations_total',
  help: 'Spam analysis passes that exceeded a bounded candidate, message or incident limit.',
  labelNames: ['phase'] as const,
  registers: [metricsRegistry],
});

let workerHeartbeatCollector: (() => Promise<void>) | null = null;

export function setWorkerHeartbeatCollector(collector: (() => Promise<void>) | null): void {
  workerHeartbeatCollector = collector;
}

export const workerHeartbeatAgeSeconds = new Gauge({
  name: 'meshcore_worker_heartbeat_age_seconds',
  help: 'Age of the last worker heartbeat by bounded worker role.',
  labelNames: ['worker'] as const,
  registers: [metricsRegistry],
  async collect() {
    await workerHeartbeatCollector?.();
  },
});

export const workerOutcomesTotal = new Counter({
  name: 'meshcore_worker_outcomes_total',
  help: 'Worker task outcomes by bounded worker role and phase.',
  labelNames: ['worker', 'phase', 'outcome'] as const,
  registers: [metricsRegistry],
});

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

export const srtmRequestsTotal = new Counter({
  name: 'meshcore_srtm_requests_total',
  help: 'SRTM tile request outcomes.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const privacyFilterTotal = new Counter({
  name: 'meshcore_privacy_filter_total',
  help: 'Privacy-filter decisions by bounded operation and outcome.',
  labelNames: ['operation', 'outcome'] as const,
  registers: [metricsRegistry],
});

let privacyGenerationHealthCollector: (() => Promise<boolean>) | null = null;

export function setPrivacyGenerationHealthCollector(collector: () => Promise<boolean>): void {
  privacyGenerationHealthCollector = collector;
}

export const privacyGenerationHealthy = new Gauge({
  name: 'meshcore_privacy_generation_healthy',
  help: 'One when desired, materialized and globally admissible privacy generations agree; zero on drift or an unavailable observation.',
  registers: [metricsRegistry],
  async collect() {
    this.set(0);
    if (privacyGenerationHealthCollector) {
      try { this.set(await privacyGenerationHealthCollector() ? 1 : 0); }
      catch { this.set(0); }
    }
  },
});

export const gracefulShutdownTotal = new Counter({
  name: 'meshcore_graceful_shutdown_total',
  help: 'Graceful shutdown attempts by bounded outcome.',
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

const HTTP_METHODS = new Set(['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS', 'HEAD']);
const METRIC_NETWORKS = new Set(['ukmesh', 'teesside', 'test']);
const METRIC_ANALYSIS_WORKLOADS = new Set(['spam-analysis', 'path-learning']);
const METRIC_WORKERS = new Set([
  'health',
  'privacy_remat',
  'path_learning',
  'synthetic',
  'link_backfill',
  'link',
]);
const METRIC_WORKER_PHASES = new Set([
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
const METRIC_SYNTHETIC_CHECKS = new Set([
  'http_liveness',
  'dependency_readiness',
  'stats_api',
  'websocket_ping',
  'websocket_initial_state',
]);
const METRIC_CACHES = new Set([
  'api_stats',
  'api_inferred_nodes',
  'api_node_links',
  'api_charts',
  'api_owner_live',
  'mqtt_channels',
  'owner_auth',
  'owner_nodes',
  'owner_dashboard',
  'owner_last_hop',
  'path_context',
  'path_resolution',
  'path_invalidation',
  'path_sticky_nodes',
  'stats_channel',
  'stats_observer',
  'ws_initial_state',
  'ws_viable_links',
]);

export function boundedNetworkMetricLabel(network: string | null | undefined): string {
  const normalized = String(network ?? '').trim().toLowerCase();
  return METRIC_NETWORKS.has(normalized) ? normalized : 'other';
}

export function boundedAnalysisWorkloadLabel(workload: string): string {
  const normalized = workload.trim().toLowerCase();
  return METRIC_ANALYSIS_WORKLOADS.has(normalized) ? normalized : 'other';
}

export function boundedCacheMetricLabel(cache: string | undefined): string {
  const normalized = String(cache ?? '').trim().toLowerCase();
  return METRIC_CACHES.has(normalized) ? normalized : 'other';
}

function boundedLabel(value: string, allowed: Set<string>): string {
  const normalized = value.trim().toLowerCase();
  return allowed.has(normalized) ? normalized : 'other';
}

export function observeWorkerOutcome(worker: string, phase: string, outcome: string): void {
  workerOutcomesTotal.inc({
    worker: boundedLabel(worker, METRIC_WORKERS),
    phase: boundedLabel(phase, METRIC_WORKER_PHASES),
    outcome: boundedLabel(outcome, METRIC_WORKER_OUTCOMES),
  });
}

export function observeSyntheticCheck(check: string, succeeded: boolean): void {
  const boundedCheck = boundedLabel(check, METRIC_SYNTHETIC_CHECKS);
  syntheticCheckSuccess.set({ check: boundedCheck }, succeeded ? 1 : 0);
  syntheticCheckOutcomesTotal.inc({
    check: boundedCheck,
    outcome: succeeded ? 'success' : 'failure',
  });
  observeWorkerOutcome('synthetic', 'check', succeeded ? 'success' : 'failure');
}

function boundedHttpMethod(method: string): string {
  const normalized = method.toUpperCase();
  return HTTP_METHODS.has(normalized) ? normalized : 'OTHER';
}

function statusClass(statusCode: number): string {
  if (statusCode >= 100 && statusCode <= 599) return `${Math.floor(statusCode / 100)}xx`;
  return 'other';
}

export function observeHttpRequest(req: Request, res: Response, next: NextFunction): void {
  const startedAt = process.hrtime.bigint();
  let observed = false;
  const observe = () => {
    if (observed) return;
    observed = true;
    const labels = {
      method: boundedHttpMethod(req.method),
      status_class: statusClass(res.statusCode),
    };
    const durationSeconds = Number(process.hrtime.bigint() - startedAt) / 1e9;
    httpRequestsTotal.inc(labels);
    httpRequestDuration.observe(labels, durationSeconds);
  };
  res.once('finish', observe);
  res.once('close', observe);
  next();
}

export function updateDbPoolMetrics(
  poolName: 'oltp' | 'analytics' | 'owner_auth' | 'ingest',
  snapshot: { totalCount: number; idleCount: number; waitingCount: number },
): void {
  dbPoolTotal.set({ state: `${poolName}_total` }, snapshot.totalCount);
  dbPoolTotal.set({ state: `${poolName}_idle` }, snapshot.idleCount);
  dbPoolTotal.set({ state: `${poolName}_waiting` }, snapshot.waitingCount);
}

export const postPersistInFlightGauge = new Gauge({
  name: 'meshcore_mqtt_post_persist_inflight',
  help: 'Best-effort post-persist follow-ups currently in flight (bounded).',
  registers: [metricsRegistry],
});

export const postPersistDroppedTotal = new Counter({
  name: 'meshcore_mqtt_post_persist_dropped_total',
  help: 'Best-effort post-persist follow-ups dropped because the in-flight bound was reached.',
  registers: [metricsRegistry],
});

// ---------------------------------------------------------------- RAM ingest
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
