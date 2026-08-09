import fs from 'node:fs';
import { Redis } from 'ioredis';
import { query } from '../db/index.js';
import { getRedisConnectionOptions, getRedisUrl } from '../platform/config/redis.js';
import { configuredLifecycleTargets } from '../db/dataLifecycle.js';
import {
  loadVerifiedRestoreReceipt,
  REQUIRED_BACKUP_DATASETS,
} from '../backup/receipt.js';
import {
  backupAgeSeconds,
  linkQueueBytes,
  linkQueueDeadBytes,
  linkQueueDeadJobs,
  linkQueueDepth as linkQueueDepthMetric,
  linkQueueLeases,
  linkQueueOldestAgeSeconds,
  linkQueueRetries,
  workerHeartbeatAgeSeconds,
} from '../metrics.js';

type WorkerSnapshot = {
  worker_name: string;
  status: string;
  queue_depth: number;
  processed_1h: number;
  last_activity_at: string | null;
  cpu_load_1m: number;
  cpu_usage_pct: number;
  mem_used_pct: number;
  disk_used_pct: number;
  queue_bytes?: number;
  dead_jobs?: number;
  retries?: number;
  active_leases?: number;
  oldest_age_s?: number;
};

type RetentionTarget = {
  table: 'worker_health_snapshots' | 'frontend_error_events' | 'owner_alert_deliveries' | 'operational_check_results' | 'observer_region_packet_sightings' | 'observer_region_observer_sightings' | 'link_job_commits' | 'ml_model_variant_packet_results';
  timestampColumn: 'ts' | 'time' | 'created_at' | 'completed_at' | 'last_seen';
  retention: string;
  batchSize: number;
  extraPredicate?: string;
};

const RETENTION_TARGETS: RetentionTarget[] = [
  { table: 'worker_health_snapshots', timestampColumn: 'ts', retention: '7 days', batchSize: 10_000 },
  { table: 'frontend_error_events', timestampColumn: 'time', retention: '30 days', batchSize: 2_000 },
  {
    table: 'owner_alert_deliveries',
    timestampColumn: 'created_at',
    retention: '90 days',
    batchSize: 1_000,
    extraPredicate: `(status = 'succeeded' OR attempts >= 5)`,
  },
  { table: 'operational_check_results', timestampColumn: 'ts', retention: '14 days', batchSize: 2_000 },
  { table: 'observer_region_packet_sightings', timestampColumn: 'last_seen', retention: '8 days', batchSize: 25_000 },
  { table: 'observer_region_observer_sightings', timestampColumn: 'last_seen', retention: '31 days', batchSize: 5_000 },
  { table: 'link_job_commits', timestampColumn: 'completed_at', retention: '180 days', batchSize: 5_000 },
  { table: 'ml_model_variant_packet_results', timestampColumn: 'created_at', retention: '180 days', batchSize: 5_000 },
];

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
const OPERATIONAL_RETENTION_ENABLED =
  process.env['DATA_LIFECYCLE_RETENTION_ENABLED'] === 'true';
const OPERATIONAL_RETENTION_TARGETS = configuredLifecycleTargets(
  process.env['DATA_LIFECYCLE_RETENTION_TARGETS'],
);
const SYNTHETIC_SUCCESS_TTL_MS = Math.max(
  16 * 60_000,
  Number(process.env['SYNTHETIC_SUCCESS_TTL_MS'] ?? 16 * 60_000) || 16 * 60_000,
);

let redisClient: Redis | null = null;

function redis(): Redis {
  if (!redisClient) {
    redisClient = new Redis(getRedisUrl(), getRedisConnectionOptions());
    redisClient.on('error', (err) => console.error('[health] redis error', err.message));
  }
  return redisClient;
}

async function deleteExpiredRows(target: RetentionTarget): Promise<void> {
  // Identifiers come from the closed RetentionTarget union above. PostgreSQL
  // cannot parameterize identifiers, but the retention interval and batch size
  // remain parameters.
  await query(
    `WITH expired AS (
       SELECT ctid
       FROM ${target.table}
       WHERE ${target.timestampColumn} < NOW() - $1::interval
         ${target.extraPredicate ? `AND ${target.extraPredicate}` : ''}
       LIMIT $2
     )
     DELETE FROM ${target.table} AS target
     USING expired
     WHERE target.ctid = expired.ctid`,
    [target.retention, target.batchSize],
  );
}

function toPct(num: number): number {
  return Math.round(num * 1000) / 10;
}

type ProcessCpuSample = {
  usageMicros: number;
  atNanos: bigint;
};

let lastProcessCpuSample: ProcessCpuSample | null = null;

function processCpuUsagePct(): number {
  const usage = process.cpuUsage();
  const current = {
    usageMicros: usage.user + usage.system,
    atNanos: process.hrtime.bigint(),
  };
  const previous = lastProcessCpuSample;
  lastProcessCpuSample = current;
  if (!previous) return 0;
  const elapsedMicros = Number(current.atNanos - previous.atNanos) / 1_000;
  if (elapsedMicros <= 0) return 0;
  return Math.round(
    (Math.max(0, current.usageMicros - previous.usageMicros) / elapsedMicros) * 10_000,
  ) / 100;
}

function readFiniteNumber(path: string): number | null {
  try {
    const value = fs.readFileSync(path, 'utf8').trim();
    if (!/^\d+$/.test(value)) return null;
    const parsed = Number(value);
    return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
  } catch {
    return null;
  }
}

export function measureDirectoryBytes(
  root: string,
  maxEntries = 100_000,
): { bytes: number | null; entries: number; complete: boolean } {
  const pending = [root];
  let bytes = 0;
  let entries = 0;
  try {
    while (pending.length > 0) {
      const directory = pending.pop()!;
      for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
        entries += 1;
        if (entries > maxEntries) return { bytes: null, entries, complete: false };
        const entryPath = `${directory}/${entry.name}`;
        if (entry.isDirectory()) pending.push(entryPath);
        else if (entry.isFile()) bytes += fs.statSync(entryPath).size;
      }
    }
    return { bytes, entries, complete: true };
  } catch {
    return { bytes: null, entries, complete: false };
  }
}

type VolumeConfig = {
  name: string;
  path: string;
  budgetBytes: number;
};

function volumeConfigs(raw = process.env['HEALTH_VOLUME_PATHS'] ?? ''): VolumeConfig[] {
  return raw.split(',')
    .map((entry) => entry.trim())
    .filter(Boolean)
    .flatMap((entry) => {
      const [name, path, budget] = entry.split('=');
      const budgetBytes = Number(budget);
      return name && path && /^[a-z][a-z0-9_-]{0,31}$/i.test(name)
        && path.startsWith('/') && Number.isSafeInteger(budgetBytes) && budgetBytes > 0
        ? [{ name, path, budgetBytes }]
        : [];
    });
}

export function measureConfiguredVolumes(raw: string): Record<string, {
  path: string;
  used_bytes: number | null;
  budget_bytes: number;
  used_pct: number | null;
  complete: boolean;
}> {
  return Object.fromEntries(volumeConfigs(raw).map((config) => {
    const measured = measureDirectoryBytes(config.path);
    return [config.name, {
      path: config.path,
      used_bytes: measured.bytes,
      budget_bytes: config.budgetBytes,
      used_pct: measured.bytes == null
        ? null
        : toPct(measured.bytes / config.budgetBytes),
      complete: measured.complete,
    }];
  }));
}

let cachedVolumes: {
  expiresAt: number;
  value: Record<string, {
    path: string;
    used_bytes: number | null;
    budget_bytes: number;
    used_pct: number | null;
    complete: boolean;
  }>;
} | null = null;

function volumeStats(): Record<string, {
  path: string;
  used_bytes: number | null;
  budget_bytes: number;
  used_pct: number | null;
  complete: boolean;
}> {
  if (cachedVolumes && cachedVolumes.expiresAt > Date.now()) return cachedVolumes.value;
  const value = measureConfiguredVolumes(process.env['HEALTH_VOLUME_PATHS'] ?? '');
  cachedVolumes = { expiresAt: Date.now() + 5 * 60_000, value };
  return value;
}

function systemStats() {
  const usagePct = processCpuUsagePct();
  const cgroupCurrent = readFiniteNumber('/sys/fs/cgroup/memory.current');
  const cgroupMax = readFiniteNumber('/sys/fs/cgroup/memory.max');
  const rss = process.memoryUsage().rss;
  const memoryUsed = cgroupCurrent ?? rss;
  const memoryTotal = cgroupMax;
  const volumes = volumeStats();
  const knownVolumePcts = Object.values(volumes)
    .map((volume) => volume.used_pct)
    .filter((value): value is number => value != null);
  const maxVolumePct = knownVolumePcts.length > 0 ? Math.max(...knownVolumePcts) : null;

  return {
    generated_at: new Date().toISOString(),
    cpu: {
      scope: 'process',
      load_1m: usagePct,
      usage_pct: usagePct,
    },
    memory: {
      scope: cgroupCurrent != null && cgroupMax != null ? 'cgroup' : 'process',
      total_mb: memoryTotal == null ? null : Math.round(memoryTotal / 1_048_576),
      used_mb: Math.round(memoryUsed / 1_048_576),
      used_pct: memoryTotal == null ? null : toPct(memoryUsed / memoryTotal),
    },
    disk: {
      scope: 'configured_volumes',
      used_pct: maxVolumePct,
      volumes,
    },
    runtime: {
      scope: 'process',
      uptime_s: Math.round(process.uptime()),
    },
  };
}

async function currentWorkers(precomputedStats?: ReturnType<typeof systemStats>): Promise<WorkerSnapshot[]> {
  refreshBackupAgeMetrics();
  const r = redis();
  const [
    linkQueue,
    linkRecent,
    linkLast,
    learning,
    healthActivity,
    backfillState,
  ] = await Promise.all([
    Promise.all([
      r.llen('meshcore:link_jobs'),
      r.hmget('meshcore:link:v3:counters', 'count', 'bytes', 'dead_count', 'dead_bytes'),
      r.zcard('meshcore:link:v3:leases'),
      r.zrange('meshcore:link:v3:enqueued', 0, 0, 'WITHSCORES'),
      r.hvals('meshcore:link:v3:attempts'),
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
    query<{ count: string }>(`SELECT COUNT(*) AS count FROM node_links WHERE itm_computed_at > NOW() - INTERVAL '1 hour'`),
    query<{ ts: string | null }>(`SELECT MAX(itm_computed_at)::text AS ts FROM node_links`),
    query<{ ts: string | null }>(`SELECT MAX(updated_at)::text AS ts FROM path_model_calibration`),
    query<{ count: string; ts: string | null }>(
      `SELECT COUNT(*) FILTER (WHERE ts > NOW() - INTERVAL '1 hour') AS count,
              MAX(ts)::text AS ts
       FROM worker_health_snapshots
       WHERE worker_name = 'health-worker'`,
    ),
    query<{ links: string; last_observed: string | null }>(
      `SELECT COUNT(*)::text AS links, MAX(last_observed)::text AS last_observed
       FROM node_links`,
    ),
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
  setHeartbeatAge('link', linkLast.rows[0]?.ts);
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
  const result = await query<WorkerSnapshot & { captured_at: string }>(
    `SELECT worker_name, status, queue_depth, processed_1h,
            last_activity_at::text, cpu_load_1m, cpu_usage_pct,
            mem_used_pct, disk_used_pct, queue_bytes, dead_jobs,
            retries, active_leases, oldest_age_s, captured_at::text
       FROM worker_health_current
      ORDER BY worker_name`,
  );
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

export async function captureWorkerHealthSnapshot(): Promise<void> {
  const capturedAt = new Date().toISOString();
  const rows = (await currentWorkers()).map((row) => row.worker_name === 'health-worker'
    ? {
        ...row,
        status: 'running',
        processed_1h: row.processed_1h + 1,
        last_activity_at: capturedAt,
      }
    : row);
  await query(
    `WITH snapshot AS MATERIALIZED (
       SELECT *
         FROM jsonb_to_recordset($2::jsonb) AS value(
           worker_name text,
           status text,
           queue_depth integer,
           processed_1h integer,
           last_activity_at timestamptz,
           cpu_load_1m double precision,
           cpu_usage_pct double precision,
           mem_used_pct double precision,
           disk_used_pct double precision,
           queue_bytes bigint,
           dead_jobs integer,
           retries integer,
           active_leases integer,
           oldest_age_s double precision
         )
     ), history_write AS (
       INSERT INTO worker_health_snapshots
         (ts, worker_name, status, queue_depth, processed_5m, processed_1h,
          last_activity_at, cpu_load_1m, mem_used_pct, disk_used_pct)
       SELECT $1::timestamptz, worker_name, status, queue_depth, 0, processed_1h,
              last_activity_at, cpu_load_1m, mem_used_pct, disk_used_pct
         FROM snapshot
       RETURNING worker_name
     ), current_cleanup AS (
       DELETE FROM worker_health_current current
        WHERE NOT EXISTS (
          SELECT 1 FROM snapshot WHERE snapshot.worker_name = current.worker_name
        )
       RETURNING worker_name
     )
     INSERT INTO worker_health_current
       (worker_name, captured_at, status, queue_depth, processed_1h,
        last_activity_at, cpu_load_1m, cpu_usage_pct, mem_used_pct,
        disk_used_pct, queue_bytes, dead_jobs, retries, active_leases, oldest_age_s)
     SELECT worker_name, $1::timestamptz, status, queue_depth, processed_1h,
            last_activity_at, cpu_load_1m, cpu_usage_pct, mem_used_pct,
            disk_used_pct, queue_bytes, dead_jobs, retries, active_leases, oldest_age_s
       FROM snapshot
     ON CONFLICT (worker_name) DO UPDATE SET
       captured_at = EXCLUDED.captured_at,
       status = EXCLUDED.status,
       queue_depth = EXCLUDED.queue_depth,
       processed_1h = EXCLUDED.processed_1h,
       last_activity_at = EXCLUDED.last_activity_at,
       cpu_load_1m = EXCLUDED.cpu_load_1m,
       cpu_usage_pct = EXCLUDED.cpu_usage_pct,
       mem_used_pct = EXCLUDED.mem_used_pct,
       disk_used_pct = EXCLUDED.disk_used_pct,
       queue_bytes = EXCLUDED.queue_bytes,
       dead_jobs = EXCLUDED.dead_jobs,
       retries = EXCLUDED.retries,
       active_leases = EXCLUDED.active_leases,
       oldest_age_s = EXCLUDED.oldest_age_s`,
    [capturedAt, JSON.stringify(rows)],
  );

  if (OPERATIONAL_RETENTION_ENABLED) {
    for (const target of RETENTION_TARGETS) {
      if (!OPERATIONAL_RETENTION_TARGETS.has(target.table)) continue;
      await deleteExpiredRows(target);
    }
  }
}

type OperationalCheckRow = {
  check_name: string;
  status: string;
  latency_ms: number;
  detail: string | null;
  ts: string;
};

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
    const tracked = query<OperationalCheckRow>(
      `SELECT DISTINCT ON (check_name)
         check_name, status, latency_ms, detail, ts::text
       FROM operational_check_results
       ORDER BY check_name, ts DESC`,
    )
      .then((result) => {
        operationalChecksSnapshot = { ts: Date.now(), rows: result.rows };
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

export async function getWorkerHealthOverview() {
  // Compute system stats once — cpuUsagePct() diffs against lastCpuSample,
  // so calling it twice in one request gives a garbage near-zero second reading.
  const sysStats = systemStats();
  const [workers, history, errors1h, ingest, pathHashWidths, multibyteSummary, operationalChecks, databaseMaintenance, databaseRuntime, redisDurability] = await Promise.all([
    currentWorkerStatuses(sysStats),
    query<{
      ts: string;
      worker_name: string;
      status: string;
      queue_depth: number;
      processed_1h: number;
      cpu_load_1m: number | null;
      mem_used_pct: number | null;
      disk_used_pct: number | null;
    }>(
      `SELECT ts::text, worker_name, status, queue_depth, processed_1h, cpu_load_1m, mem_used_pct, disk_used_pct
       FROM worker_health_snapshots
       ORDER BY ts DESC
       LIMIT 720`,
    ),
    query<{ count: string }>(`SELECT COUNT(*) AS count FROM frontend_error_events WHERE time > NOW() - INTERVAL '1 hour'`),
    query<{
      stale_nodes: string;
      active_nodes: string;
      max_stale_minutes: string | null;
      stale_threshold_minutes: string;
      global_last_packet_at: string | null;
    }>(
       `WITH latest_rx AS (
         -- Bounded to the active_rx window below; an unbounded scan of the
         -- packets hypertable here takes 20s+ and gives identical results.
         SELECT rx_node_id, MAX(time) AS last_packet_at
      FROM packets
         WHERE time > NOW() - INTERVAL '3 days'
           AND rx_node_id IS NOT NULL
           AND rx_node_id <> ''
           AND network IS DISTINCT FROM 'test'
           AND split_part(topic, '/', 1) <> 'meshcore-test'
         GROUP BY rx_node_id
       ),
       test_active AS (
         SELECT rx_node_id
         FROM packets
         WHERE time > NOW() - INTERVAL '3 days'
           AND rx_node_id IS NOT NULL AND rx_node_id <> ''
         GROUP BY rx_node_id
         HAVING MAX(time) = MAX(time) FILTER (WHERE network = 'test')
       ),
       active_rx AS (
         SELECT rx_node_id, last_packet_at
         FROM latest_rx
         WHERE last_packet_at > NOW() - INTERVAL '3 days'
           AND rx_node_id NOT IN (SELECT rx_node_id FROM test_active)
       )
       SELECT
         COUNT(*) FILTER (WHERE last_packet_at < NOW() - INTERVAL '15 minutes')::text AS stale_nodes,
         COUNT(*)::text AS active_nodes,
         MAX(
           CASE
             WHEN last_packet_at < NOW() - INTERVAL '15 minutes'
             THEN FLOOR(EXTRACT(EPOCH FROM (NOW() - last_packet_at)) / 60)
             ELSE NULL
           END
         )::text AS max_stale_minutes,
         '15'::text AS stale_threshold_minutes,
         (SELECT MAX(time)::text
          FROM packets
          WHERE network IS DISTINCT FROM 'test'
            AND split_part(topic, '/', 1) <> 'meshcore-test') AS global_last_packet_at
       FROM active_rx`,
    ),
    query<{
      hash_hex_len: string;
      hop_count: string;
    }>(
      `SELECT length(h)::text AS hash_hex_len, COUNT(*)::text AS hop_count
       FROM packets p
       CROSS JOIN LATERAL unnest(p.path_hashes) AS h
       WHERE p.time > NOW() - INTERVAL '24 hours'
         AND p.network IS DISTINCT FROM 'test'
       GROUP BY 1`,
    ),
    query<{
      latest_multibyte_at: string | null;
      multibyte_packets_24h: string;
    }>(
      `SELECT
         MAX(time) FILTER (
           WHERE EXISTS (
             SELECT 1
             FROM unnest(path_hashes) AS h
             WHERE length(h) > 2
           )
         )::text AS latest_multibyte_at,
         COUNT(*) FILTER (
           WHERE EXISTS (
             SELECT 1
             FROM unnest(path_hashes) AS h
             WHERE length(h) > 2
           )
         )::text AS multibyte_packets_24h
       FROM packets
       WHERE time > NOW() - INTERVAL '24 hours'
        AND network IS DISTINCT FROM 'test'`,
    ),
    getOperationalChecksCached(),
    query<{
      database_size_bytes: string;
      dead_rows: string;
      oldest_vacuum_at: string | null;
      tables_needing_vacuum: string;
    }>(
      `SELECT
         pg_database_size(current_database())::text AS database_size_bytes,
         COALESCE(SUM(n_dead_tup), 0)::text AS dead_rows,
         MIN(last_autovacuum)::text AS oldest_vacuum_at,
         COUNT(*) FILTER (
           WHERE n_live_tup > 10000
             AND n_dead_tup > GREATEST(10000, n_live_tup * 0.1)
         )::text AS tables_needing_vacuum
       FROM pg_stat_user_tables`,
    ),
    query<{
      connection_count: string;
      max_connections: string;
      cache_hit_ratio: string | null;
    }>(
      `SELECT
         (SELECT COUNT(*)::text FROM pg_stat_activity WHERE datname = current_database()) AS connection_count,
         current_setting('max_connections') AS max_connections,
         ROUND(
           100 * blks_hit::numeric / NULLIF(blks_hit + blks_read, 0),
           2
         )::text AS cache_hit_ratio
       FROM pg_stat_database
       WHERE datname = current_database()`,
    ),
    redisDurabilityState(),
  ]);

  const ingestRow = ingest.rows[0];
  const staleNodes = Number(ingestRow?.stale_nodes ?? 0);
  const activeNodes = Number(ingestRow?.active_nodes ?? 0);
  const maxStaleMinutes = Number(ingestRow?.max_stale_minutes ?? 0);
  const staleThresholdMinutes = Number(ingestRow?.stale_threshold_minutes ?? 15);
  const widthToBucket: Record<number, 'one_byte' | 'two_byte' | 'three_byte'> = {
    2: 'one_byte',
    4: 'two_byte',
    6: 'three_byte',
  };
  const pathHashStats = {
    one_byte: 0,
    two_byte: 0,
    three_byte: 0,
  };

  for (const row of pathHashWidths.rows) {
    const width = Number(row.hash_hex_len ?? 0);
    const bucket = widthToBucket[width];
    if (!bucket) continue;
    pathHashStats[bucket] += Number(row.hop_count ?? 0);
  }

  const multibyteRow = multibyteSummary.rows[0];
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
  if (tablesNeedingVacuum > 0) {
    problems.push({
      code: 'database_vacuum_backlog',
      severity: tablesNeedingVacuum >= 3 ? 'critical' : 'warning',
      message: `${tablesNeedingVacuum} database table(s) exceed the dead-row vacuum threshold`,
    });
  }
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

export type HealthProblem = {
  code: string;
  severity: 'warning' | 'critical';
  message: string;
};

/**
 * Anonymous browser diagnostics are reported to operators but are deliberately
 * absent from the authoritative status calculation.
 */
export function summarizeAuthoritativeHealth(
  problems: readonly HealthProblem[],
  frontendErrors: number,
): {
  status: 'healthy' | 'degraded' | 'critical';
  frontendErrors: number;
} {
  return {
    status: problems.some((problem) => problem.severity === 'critical')
      ? 'critical'
      : problems.length > 0 ? 'degraded' : 'healthy',
    frontendErrors: Math.max(0, Math.floor(frontendErrors) || 0),
  };
}

export function toPublicHealthOverview(
  detail: Awaited<ReturnType<typeof getWorkerHealthOverview>>,
) {
  const storagePct = detail.system.disk.used_pct;
  return {
    status: detail.status,
    generatedAt: detail.system.generated_at,
    maintenance: detail.maintenance,
    incidents: detail.problems.map((problem) => ({
      code: problem.code,
      severity: problem.severity,
    })),
    components: {
      ingest: {
        status: detail.ingest.packet_age_minutes == null
          ? 'unknown'
          : detail.ingest.packet_age_minutes > 30 ? 'stale' : 'ok',
      },
      workers: {
        status: detail.workers.some((worker) => worker.status === 'failed')
          ? 'failed'
          : detail.workers.some((worker) => worker.status === 'running')
            ? 'running'
            : 'idle',
      },
      storage: {
        status: storagePct == null
          ? 'unknown'
          : storagePct >= 90 ? 'critical' : storagePct >= 80 ? 'warning' : 'ok',
      },
    },
  };
}
