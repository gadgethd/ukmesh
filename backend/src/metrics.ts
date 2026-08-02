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

export const websocketClients = new Gauge({
  name: 'meshcore_websocket_clients',
  help: 'Currently connected WebSocket clients.',
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

export const viewshedQueueAdmissionsTotal = new Counter({
  name: 'meshcore_viewshed_queue_admissions_total',
  help: 'Viewshed queue admission decisions.',
  labelNames: ['status'] as const,
  registers: [metricsRegistry],
});

export const viewshedQueueDepth = new Gauge({
  name: 'meshcore_viewshed_queue_depth',
  help: 'Current number of queued viewshed jobs.',
  registers: [metricsRegistry],
});

export const viewshedQueueBytes = new Gauge({
  name: 'meshcore_viewshed_queue_payload_bytes',
  help: 'Current serialized payload bytes retained by the viewshed queue.',
  registers: [metricsRegistry],
});

export const viewshedQueueDeadJobs = new Gauge({
  name: 'meshcore_viewshed_queue_dead_jobs',
  help: 'Retained dead-letter coverage jobs.',
  registers: [metricsRegistry],
});

export const viewshedQueueLeases = new Gauge({
  name: 'meshcore_viewshed_queue_active_leases',
  help: 'Currently leased coverage jobs.',
  registers: [metricsRegistry],
});

export const viewshedQueueRetries = new Gauge({
  name: 'meshcore_viewshed_queue_retries',
  help: 'Retry attempts retained in active and dead coverage jobs.',
  registers: [metricsRegistry],
});

export const viewshedQueueOldestAgeSeconds = new Gauge({
  name: 'meshcore_viewshed_queue_oldest_age_seconds',
  help: 'Age of the oldest active coverage job.',
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
  help: 'MQTT ingest tasks by bounded outcome.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const mqttIngestQueueDepth = new Gauge({
  name: 'meshcore_mqtt_ingest_queue_depth',
  help: 'MQTT messages waiting in the bounded ingest queue.',
  registers: [metricsRegistry],
});

export const mqttIngestActive = new Gauge({
  name: 'meshcore_mqtt_ingest_active',
  help: 'MQTT ingest tasks currently executing.',
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

export const packetBatchFlushTotal = new Counter({
  name: 'meshcore_packet_batch_flush_total',
  help: 'Atomic packet batch flushes by bounded outcome.',
  labelNames: ['outcome'] as const,
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

export const workerHeartbeatAgeSeconds = new Gauge({
  name: 'meshcore_worker_heartbeat_age_seconds',
  help: 'Age of the last worker heartbeat by bounded worker role.',
  labelNames: ['worker'] as const,
  registers: [metricsRegistry],
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

const HTTP_METHODS = new Set(['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS', 'HEAD']);
const METRIC_NETWORKS = new Set(['ukmesh', 'teesside', 'test']);
const METRIC_ANALYSIS_WORKLOADS = new Set(['spam-analysis', 'path-learning', 'path-history']);
const METRIC_WORKERS = new Set([
  'health',
  'path_learning',
  'path_history',
  'synthetic',
  'link_backfill',
  'link',
  'viewshed',
]);
const METRIC_WORKER_PHASES = new Set([
  'snapshot',
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
  'websocket_initial_state',
]);
const METRIC_CACHES = new Set([
  'api_stats',
  'api_inferred_nodes',
  'api_node_links',
  'api_path_history',
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
  poolName: 'oltp' | 'analytics' | 'owner_auth',
  snapshot: { totalCount: number; idleCount: number; waitingCount: number },
): void {
  dbPoolTotal.set({ state: `${poolName}_total` }, snapshot.totalCount);
  dbPoolTotal.set({ state: `${poolName}_idle` }, snapshot.idleCount);
  dbPoolTotal.set({ state: `${poolName}_waiting` }, snapshot.waitingCount);
}
