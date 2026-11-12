import { createHealthRepository, CORE_CAPACITY_TABLES } from './repository.js';
import type { WorkerSnapshot, OperationalCheckRow } from './types.js';
import { createSystemProbe } from './resources.js';
import { RETENTION_TARGETS, deleteExpiredRows } from './retention.js';
import { privacyGenerationProblem, databaseVacuumBacklogProblem, privacyRematerializationBacklogProblem, summarizeAuthoritativeHealth, type HealthProblem } from './projections.js';
import { LINK_V3_KEYS } from '../queue/linkQueueV3.js';
import { Redis } from 'ioredis';
import { query as defaultQuery } from '../db/runtime.js';
import { getRedisConnectionOptions, getRedisUrl } from '../platform/config/redis.js';
import {
  configuredLifecycleTargets,
  dataLifecycleConfigurationStatus,
} from '../db/dataLifecycle.js';
import {
  createPrivacyGenerationReconciler,
  PRIVACY_GENERATION_HEALTH_SQL,
  type PrivacyGenerationSnapshot,
} from '../privacy/generationHealth.js';
import { linkWorkerHeartbeatCollector, readLinkWorkerHeartbeatAge } from './workerHeartbeat.js';
import {
  loadVerifiedRestoreReceipt,
  REQUIRED_BACKUP_DATASETS,
} from '../backup/receipt.js';
import { backupAgeSeconds, coreTelemetryExpiredChunks, coreTelemetryHypertableBytes, coreTelemetryHypertableChunks, coreTelemetryUncompressedChunks, packetPathsBytesPerRow, packetPathsOverdueUncompressedChunks, packetPathsRows30d } from '../metrics/health.js';
import { linkQueueBytes, linkQueueDeadBytes, linkQueueDeadJobs, linkQueueDepth as linkQueueDepthMetric, linkQueueLeases, linkQueueOldestAgeSeconds, linkQueueRetries } from '../metrics/queue.js';
import { workerHeartbeatAgeSeconds, setWorkerHeartbeatCollector } from '../metrics/workers.js';
import { setPrivacyGenerationHealthCollector } from '../metrics/privacy.js';

export function createHealthRuntime(deps: { execute?: typeof defaultQuery; createRedis?: () => Redis } = {}) {
  const query = deps.execute ?? defaultQuery;
  const systemStats = createSystemProbe();
  const repository = createHealthRepository(query);
  const reconcilePrivacyGenerations = createPrivacyGenerationReconciler(
    signal => query<PrivacyGenerationSnapshot>(PRIVACY_GENERATION_HEALTH_SQL, undefined, signal),
  );
  let closed = false;
  let closePrivacyCollector: (() => void) | undefined;
  let closeHeartbeatCollector: (() => void) | undefined;
  function refreshBackupAgeMetrics(now = new Date()): void {
    backupAgeSeconds.reset();
    try {
      const receipt = loadVerifiedRestoreReceipt({
        receiptPath: process.env['DATA_LIFECYCLE_RESTORE_RECEIPT_PATH'],
        signaturePath: process.env['DATA_LIFECYCLE_RESTORE_RECEIPT_SIGNATURE_PATH'],
        verifyKeyPath: process.env['DATA_LIFECYCLE_RECEIPT_VERIFY_KEY_PATH'],
      });
      const ageSeconds = Math.max(
        0,
        (now.getTime() - Date.parse(receipt.backup_completed_at)) / 1_000,
      );
      for (const dataset of REQUIRED_BACKUP_DATASETS) {
        backupAgeSeconds.set({ dataset }, ageSeconds);
      }
    } catch {
      // Absence is intentional signal: BackupReceiptMissing fires when no
      // signature-verified receipt can be loaded.
    }
  }

  async function refreshPacketPathCapacityMetrics(): Promise<void> {
    const result = await repository.loadPacketPathCapacity();
    const row = result.rows[0];
    const approximateRows = Number(row?.approximate_rows ?? 0);
    const totalBytes = Number(row?.total_bytes ?? 0);
    packetPathsRows30d.set(Number(row?.rows_30d ?? 0));
    packetPathsBytesPerRow.set(approximateRows > 0 ? totalBytes / approximateRows : 0);
    packetPathsOverdueUncompressedChunks.set(Number(row?.overdue_uncompressed_chunks ?? 0));
  }

  async function refreshCoreTelemetryCapacityMetrics(): Promise<void> {
    for (const table of CORE_CAPACITY_TABLES) {
      coreTelemetryHypertableBytes.set({ table }, 0);
      coreTelemetryHypertableChunks.set({ table }, 0);
      coreTelemetryExpiredChunks.set({ table }, 0);
      coreTelemetryUncompressedChunks.set({ table }, 0);
    }
    const result = await repository.loadCoreCapacity();
    for (const row of result.rows) {
      const labels = { table: row.table_name };
      coreTelemetryHypertableBytes.set(labels, Number(row.total_bytes ?? 0));
      coreTelemetryHypertableChunks.set(labels, Number(row.chunk_count ?? 0));
      coreTelemetryExpiredChunks.set(labels, Number(row.expired_chunks ?? 0));
      coreTelemetryUncompressedChunks.set(
        labels,
        Number(row.overdue_uncompressed_chunks ?? 0),
      );
    }
  }
  const OPERATIONAL_RETENTION_ENABLED =
    process.env['DATA_LIFECYCLE_RETENTION_ENABLED'] === 'true';
  const OPERATIONAL_RETENTION_TARGETS = configuredLifecycleTargets(
    process.env['DATA_LIFECYCLE_RETENTION_TARGETS'],
  );
  const DATA_LIFECYCLE_CONFIGURATION = dataLifecycleConfigurationStatus();
  if (!DATA_LIFECYCLE_CONFIGURATION.ready) {
    const missing = [
      ...(DATA_LIFECYCLE_CONFIGURATION.retentionEnabled
        ? DATA_LIFECYCLE_CONFIGURATION.missingRetentionTargets.map((target) => `retention:${target}`)
        : []),
      ...(DATA_LIFECYCLE_CONFIGURATION.compressionEnabled
        ? DATA_LIFECYCLE_CONFIGURATION.missingCompressionTargets.map((target) => `compression:${target}`)
        : []),
    ];
    throw new Error(`DATA_LIFECYCLE_REQUIRED_TARGETS_MISSING:${missing.join(',')}`);
  }
  const SYNTHETIC_SUCCESS_TTL_MS = Math.max(
    16 * 60_000,
    Number(process.env['SYNTHETIC_SUCCESS_TTL_MS'] ?? 16 * 60_000) || 16 * 60_000,
  );

  let redisClient: Redis | null = null;

  function redis(): Redis {
    if (!redisClient) {
      if (closed) throw new Error('HEALTH_RUNTIME_CLOSED');
      redisClient = deps.createRedis?.() ?? new Redis(getRedisUrl(), getRedisConnectionOptions());
      redisClient.on('error', (err) => console.error('[health] redis error', err.message));
    }
    return redisClient;
  }

  // Collect on every scrape, not only the five-minute health snapshot cadence.
  // DB write recency remains last_activity_at; it cannot stand in for liveness.

  async function currentWorkers(precomputedStats?: ReturnType<typeof systemStats>): Promise<WorkerSnapshot[]> {
    refreshBackupAgeMetrics();
    const r = redis();
    const [
      linkQueue,
      linkRecent,
      linkLast,
      linkHeartbeatAge,
      learning,
      healthActivity,
      backfillState,
    ] = await Promise.all([
      Promise.all([
        r.llen('meshcore:link_jobs'),
        r.hmget(LINK_V3_KEYS.counters, 'count', 'bytes', 'dead_count', 'dead_bytes'),
        r.zcard(LINK_V3_KEYS.leases),
        r.zrange(LINK_V3_KEYS.enqueued, 0, '0', 'WITHSCORES'),
        r.hvals(LINK_V3_KEYS.attempts),
      ]).then(([legacy, counters, leases, oldest, attempts]) => {
        const [count, bytes, deadCount, deadBytes] = counters;
        const oldestScore = Number(oldest[1] ?? 0);
        return {
          depth: Number(legacy ?? 0) + Number(count ?? 0),
          count: Number(count ?? 0),
          bytes: Number(bytes ?? 0),
          deadCount: Number(deadCount ?? 0),
          deadBytes: Number(deadBytes ?? 0),
          leases: Number(leases ?? 0),
          oldestAgeSeconds: oldestScore > 0 ? Math.max(0, (Date.now() - oldestScore) / 1_000) : 0,
          retries: attempts.reduce((total, value) => total + Math.max(0, Number(value) - 1), 0),
        };
      }),
      repository.loadRecentLinkCount(),
      repository.loadLatestLinkComputation(),
      readLinkWorkerHeartbeatAge(r),
      repository.loadLatestLearningCalibration(),
      repository.loadRecentHealthActivity(),
      repository.loadBackfillActivity(),
    ]);

    const stats = precomputedStats ?? systemStats();
    const load = stats.cpu.load_1m;
    const memPct = stats.memory.used_pct ?? -1;
    const diskPct = stats.disk.used_pct ?? -1;

    const linkProcessed = Number(linkRecent.rows[0]?.count ?? 0);
    const healthProcessed = Number(healthActivity.rows[0]?.count ?? 0);
    const healthLastTs = healthActivity.rows[0]?.ts ?? null;
    const learningLast = learning.rows[0]?.ts ?? null;
    const learningRecent = learningLast ? (Date.now() - Date.parse(learningLast)) <= 60 * 60_000 : false;
    const backfillLinks = Number(backfillState.rows[0]?.links ?? 0);
    const backfillLast = backfillState.rows[0]?.last_observed ?? null;
    linkQueueDepthMetric.set(linkQueue.count);
    linkQueueBytes.set(linkQueue.bytes);
    linkQueueDeadJobs.set(linkQueue.deadCount);
    linkQueueDeadBytes.set(linkQueue.deadBytes);
    linkQueueLeases.set(linkQueue.leases);
    linkQueueRetries.set(linkQueue.retries);
    linkQueueOldestAgeSeconds.set(linkQueue.oldestAgeSeconds);
    const setHeartbeatAge = (worker: string, timestamp: string | null | undefined) => {
      const age = timestamp
        ? Math.max(0, (Date.now() - Date.parse(timestamp)) / 1_000)
        : -1;
      workerHeartbeatAgeSeconds.set({ worker }, Number.isFinite(age) ? age : -1);
    };
    workerHeartbeatAgeSeconds.set({ worker: 'link' }, linkHeartbeatAge);
    setHeartbeatAge('path_learning', learningLast);
    setHeartbeatAge('health', healthLastTs);
    setHeartbeatAge('link_backfill', backfillLast);
    return [
      {
        worker_name: 'link-worker',
        status: linkQueue.depth > 0 || linkProcessed > 0 ? 'running' : 'idle',
        queue_depth: linkQueue.depth,
        processed_1h: linkProcessed,
        last_activity_at: linkLast.rows[0]?.ts ?? null,
        cpu_load_1m: load,
        cpu_usage_pct: stats.cpu.usage_pct,
        mem_used_pct: memPct,
        disk_used_pct: diskPct,
        queue_bytes: linkQueue.bytes,
        dead_jobs: linkQueue.deadCount,
        retries: linkQueue.retries,
        active_leases: linkQueue.leases,
        oldest_age_s: linkQueue.oldestAgeSeconds,
      },
      {
        worker_name: 'path-learning',
        status: learningRecent ? 'running' : 'idle',
        queue_depth: 0,
        processed_1h: learningRecent ? 1 : 0,
        last_activity_at: learningLast,
        cpu_load_1m: load,
        cpu_usage_pct: stats.cpu.usage_pct,
        mem_used_pct: memPct,
        disk_used_pct: diskPct,
      },
      {
        worker_name: 'health-worker',
        status: healthLastTs ? 'running' : 'idle',
        queue_depth: 0,
        processed_1h: healthProcessed,
        last_activity_at: healthLastTs,
        cpu_load_1m: load,
        cpu_usage_pct: stats.cpu.usage_pct,
        mem_used_pct: memPct,
        disk_used_pct: diskPct,
      },
      {
        worker_name: 'link-backfill-worker',
        status: backfillLinks > 0 ? 'completed' : 'pending',
        queue_depth: 0,
        processed_1h: 0,
        last_activity_at: backfillLast,
        cpu_load_1m: load,
        cpu_usage_pct: stats.cpu.usage_pct,
        mem_used_pct: memPct,
        disk_used_pct: diskPct,
      },
    ];
  }

  async function currentWorkerStatuses(precomputedStats: ReturnType<typeof systemStats>): Promise<WorkerSnapshot[]> {
    const result = await repository.loadCurrentWorkers();
    if (result.rows.length === 0) return currentWorkers(precomputedStats);
    return result.rows.map((row) => ({
      worker_name: row.worker_name,
      status: row.status,
      queue_depth: Number(row.queue_depth ?? 0),
      processed_1h: Number(row.processed_1h ?? 0),
      last_activity_at: row.last_activity_at,
      cpu_load_1m: Number(row.cpu_load_1m ?? 0),
      cpu_usage_pct: Number(row.cpu_usage_pct ?? 0),
      mem_used_pct: Number(row.mem_used_pct ?? -1),
      disk_used_pct: Number(row.disk_used_pct ?? -1),
      queue_bytes: row.queue_bytes == null ? undefined : Number(row.queue_bytes),
      dead_jobs: row.dead_jobs == null ? undefined : Number(row.dead_jobs),
      retries: row.retries == null ? undefined : Number(row.retries),
      active_leases: row.active_leases == null ? undefined : Number(row.active_leases),
      oldest_age_s: row.oldest_age_s == null ? undefined : Number(row.oldest_age_s),
    }));
  }

  async function redisDurabilityState(): Promise<{
    maxmemory_policy: string;
    appendonly: string;
  }> {
    const r = redis();
    try {
      const [policy, appendonly] = await Promise.all([
        r.config('GET', 'maxmemory-policy') as Promise<string[]>,
        r.config('GET', 'appendonly') as Promise<string[]>,
      ]);
      return {
        maxmemory_policy: policy[1] ?? 'unknown',
        appendonly: appendonly[1] ?? 'unknown',
      };
    } catch (error) {
      console.error('[health] unable to verify Redis durability config', error);
      return { maxmemory_policy: 'unknown', appendonly: 'unknown' };
    }
  }

  async function captureWorkerHealthSnapshot(): Promise<void> {
    if (closed) throw new Error('HEALTH_RUNTIME_CLOSED');
    await refreshPacketPathCapacityMetrics();
    await refreshCoreTelemetryCapacityMetrics();
    const capturedAt = new Date().toISOString();
    const rows = (await currentWorkers()).map((row) => row.worker_name === 'health-worker'
      ? {
          ...row,
          status: 'running',
          processed_1h: row.processed_1h + 1,
          last_activity_at: capturedAt,
        }
      : row);
    await repository.saveWorkerSnapshot(capturedAt, rows);

    if (OPERATIONAL_RETENTION_ENABLED) {
      for (const target of RETENTION_TARGETS) {
        if (!OPERATIONAL_RETENTION_TARGETS.has(target.table)) continue;
        await deleteExpiredRows(target, query);
      }
    }
  }

  const OPERATIONAL_CHECKS_TTL_MS = 30_000;
  const OPERATIONAL_CHECKS_STALE_TTL_MS = 5 * 60_000;
  let operationalChecksSnapshot: { ts: number; rows: OperationalCheckRow[] } | null = null;
  let operationalChecksInflight: Promise<OperationalCheckRow[]> | null = null;

  async function getOperationalChecksCached(): Promise<{ rows: OperationalCheckRow[] }> {
    const now = Date.now();
    if (operationalChecksSnapshot && now - operationalChecksSnapshot.ts < OPERATIONAL_CHECKS_TTL_MS) {
      return { rows: operationalChecksSnapshot.rows };
    }
    if (!operationalChecksInflight) {
      const tracked = repository.loadOperationalChecks()
        .then((result) => {
          if (!closed) operationalChecksSnapshot = { ts: Date.now(), rows: result.rows };
          return result.rows;
        })
        .finally(() => {
          if (operationalChecksInflight === tracked) operationalChecksInflight = null;
        });
      operationalChecksInflight = tracked;
    }
    if (operationalChecksSnapshot && now - operationalChecksSnapshot.ts < OPERATIONAL_CHECKS_STALE_TTL_MS) {
      return { rows: operationalChecksSnapshot.rows };
    }
    return { rows: await operationalChecksInflight! };
  }

  async function getWorkerHealthOverview() {
    if (closed) throw new Error('HEALTH_RUNTIME_CLOSED');
    // Compute system stats once — cpuUsagePct() diffs against lastCpuSample,
    // so calling it twice in one request gives a garbage near-zero second reading.
    const sysStats = systemStats();
    const [workers, history, errors1h, ingest, pathDiagnostics, operationalChecks, databaseMaintenance, databaseRuntime, redisDurability, privacyRemat, privacyGenerations] = await Promise.all([
      currentWorkerStatuses(sysStats),
      repository.loadHistory(),
      repository.loadFrontendErrors(),
      repository.loadIngest(),
      repository.loadPathDiagnostics(),
      getOperationalChecksCached(),
      repository.loadDatabaseMaintenance(),
      repository.loadDatabaseRuntime(),
      redisDurabilityState(),
      repository.loadPrivacyBacklog(Math.min(100, Math.max(1, Number(process.env['PRIVACY_REMAT_MAX_ATTEMPTS'] ?? 8) || 8))),
      reconcilePrivacyGenerations(),
    ]);

    const ingestRow = ingest.rows[0];
    const staleNodes = Number(ingestRow?.stale_nodes ?? 0);
    const activeNodes = Number(ingestRow?.active_nodes ?? 0);
    const maxStaleMinutes = Number(ingestRow?.max_stale_minutes ?? 0);
    const staleThresholdMinutes = Number(ingestRow?.stale_threshold_minutes ?? 15);
    const multibyteRow = pathDiagnostics.rows[0];
    const pathHashStats = {
      one_byte: Number(multibyteRow?.one_byte ?? 0),
      two_byte: Number(multibyteRow?.two_byte ?? 0),
      three_byte: Number(multibyteRow?.three_byte ?? 0),
    };
    const problems: HealthProblem[] = [];
    const lastPacketAt = ingestRow?.global_last_packet_at ? Date.parse(ingestRow.global_last_packet_at) : Number.NaN;
    const packetAgeMinutes = Number.isFinite(lastPacketAt) ? Math.floor((Date.now() - lastPacketAt) / 60_000) : null;
    if (packetAgeMinutes == null || packetAgeMinutes > 30) {
      problems.push({
        code: 'ingest_stale',
        severity: packetAgeMinutes == null || packetAgeMinutes > 60 ? 'critical' : 'warning',
        message: packetAgeMinutes == null ? 'No public packet ingest timestamp is available' : `Public ingest is ${packetAgeMinutes} minutes stale`,
      });
    }
    if (sysStats.disk.used_pct != null && sysStats.disk.used_pct >= 90) {
      problems.push({ code: 'disk_pressure', severity: 'critical', message: `Disk usage is ${sysStats.disk.used_pct}%` });
    } else if (sysStats.disk.used_pct != null && sysStats.disk.used_pct >= 80) {
      problems.push({ code: 'disk_pressure', severity: 'warning', message: `Disk usage is ${sysStats.disk.used_pct}%` });
    }
    for (const worker of workers) {
      if (worker.queue_depth >= 1_000) {
        problems.push({
          code: 'worker_queue_backlog',
          severity: worker.queue_depth >= 5_000 ? 'critical' : 'warning',
          message: `${worker.worker_name} queue contains ${worker.queue_depth} jobs`,
        });
      }
      if ((worker.dead_jobs ?? 0) > 0) {
        problems.push({
          code: 'worker_dead_letter_jobs',
          severity: (worker.dead_jobs ?? 0) >= 10 ? 'critical' : 'warning',
          message: `${worker.worker_name} retains ${worker.dead_jobs} dead-letter job(s)`,
        });
      }
      if ((worker.oldest_age_s ?? 0) > 3_600) {
        problems.push({
          code: 'worker_oldest_job_stale',
          severity: (worker.oldest_age_s ?? 0) > 21_600 ? 'critical' : 'warning',
          message: `${worker.worker_name} oldest job is ${Math.floor((worker.oldest_age_s ?? 0) / 60)} minutes old`,
        });
      }
    }
    // Browser reports are anonymous, untrusted diagnostics. They are retained as
    // an operator trend but can never set authoritative public health severity.
    const frontendErrors = Number(errors1h.rows[0]?.count ?? 0);
    const latestChecks = operationalChecks.rows;
    for (const check of latestChecks) {
      const ageMs = Date.now() - Date.parse(check.ts);
      const ageMinutes = Math.floor(ageMs / 60_000);
      if (check.status !== 'ok' || ageMs > SYNTHETIC_SUCCESS_TTL_MS) {
        problems.push({
          code: check.status !== 'ok' ? 'synthetic_check_failed' : 'synthetic_check_stale',
          severity: check.status !== 'ok' || ageMs > SYNTHETIC_SUCCESS_TTL_MS * 2 ? 'critical' : 'warning',
          message: `${check.check_name}: ${check.status !== 'ok' ? check.detail ?? 'failed' : `last result is ${ageMinutes} minutes old`}`,
        });
      }
    }
    const maintenance = databaseMaintenance.rows[0];
    const tablesNeedingVacuum = Number(maintenance?.tables_needing_vacuum ?? 0);
    const vacuumProblem = databaseVacuumBacklogProblem(tablesNeedingVacuum);
    if (vacuumProblem) problems.push(vacuumProblem);
    const privacyRematRow = privacyRemat.rows[0];
    const privacyRematProblem = privacyRematerializationBacklogProblem({
      pending: Number(privacyRematRow?.pending ?? 0),
      processing: Number(privacyRematRow?.processing ?? 0),
      failed: Number(privacyRematRow?.failed ?? 0),
      failedRetryable: Number(privacyRematRow?.failed_retryable ?? 0),
      failedExhausted: Number(privacyRematRow?.failed_exhausted ?? 0),
      oldestPendingAgeSeconds: privacyRematRow?.oldest_pending_age_seconds == null
        ? null : Number(privacyRematRow.oldest_pending_age_seconds),
    });
    if (privacyRematProblem) problems.push(privacyRematProblem);
    const generationProblem = privacyGenerationProblem(privacyGenerations);
    if (generationProblem) problems.push(generationProblem);

    if (redisDurability.maxmemory_policy !== 'noeviction') {
      problems.push({
        code: 'redis_eviction_policy_unsafe',
        severity: 'critical',
        message: `Redis maxmemory policy is ${redisDurability.maxmemory_policy}; durable queues require noeviction`,
      });
    }
    if (redisDurability.appendonly !== 'yes') {
      problems.push({
        code: 'redis_persistence_disabled',
        severity: 'warning',
        message: `Redis append-only persistence is ${redisDurability.appendonly}`,
      });
    }

    const healthSummary = summarizeAuthoritativeHealth(problems, frontendErrors);
    return {
      status: healthSummary.status,
      problems,
      maintenance: {
        active: process.env['MAINTENANCE_ACTIVE'] === '1',
        message: String(process.env['MAINTENANCE_MESSAGE'] ?? '').slice(0, 300) || null,
      },
      operational_checks: latestChecks,
      database: {
        size_bytes: Number(maintenance?.database_size_bytes ?? 0),
        dead_rows: Number(maintenance?.dead_rows ?? 0),
        oldest_vacuum_at: maintenance?.oldest_vacuum_at ?? null,
        tables_needing_vacuum: tablesNeedingVacuum,
        connection_count: Number(databaseRuntime.rows[0]?.connection_count ?? 0),
        max_connections: Number(databaseRuntime.rows[0]?.max_connections ?? 0),
        cache_hit_ratio: Number(databaseRuntime.rows[0]?.cache_hit_ratio ?? 0),
      },
      redis: redisDurability,
      privacy_visibility: privacyGenerations,
      system: sysStats,
      workers,
      history: history.rows,
      frontend_errors_1h: healthSummary.frontendErrors,
      ingest: {
        stale_nodes: staleNodes,
        active_nodes: activeNodes,
        max_stale_minutes: staleNodes > 0 ? maxStaleMinutes : 0,
        stale_threshold_minutes: staleThresholdMinutes,
        global_last_packet_at: ingestRow?.global_last_packet_at ?? null,
        packet_age_minutes: packetAgeMinutes,
      },
      path_hashes: {
        last_24h_hops: pathHashStats,
        multibyte_packets_24h: Number(multibyteRow?.multibyte_packets_24h ?? 0),
        latest_multibyte_at: multibyteRow?.latest_multibyte_at ?? null,
      },
    };
  }


  return {
    getWorkerHealthOverview,
    captureWorkerHealthSnapshot,
    startTelemetry() {
      if (closed || closePrivacyCollector) return;
      closePrivacyCollector = setPrivacyGenerationHealthCollector(async () => (await reconcilePrivacyGenerations()).healthy);
      closeHeartbeatCollector = setWorkerHeartbeatCollector(linkWorkerHeartbeatCollector(redis, workerHeartbeatAgeSeconds));
    },
    close() {
      if (closed) return;
      closed = true;
      closePrivacyCollector?.(); closeHeartbeatCollector?.();
      redisClient?.disconnect(); redisClient = null;
      operationalChecksSnapshot = null;
    },
  };
}

export type HealthRuntime = ReturnType<typeof createHealthRuntime>;
