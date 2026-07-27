import os from 'node:os';
import fs from 'node:fs';
import { Redis } from 'ioredis';
import { query } from '../db/index.js';
import { isViewshedFeatureEnabled } from '../features.js';
import { getRedisConnectionOptions, getRedisUrl } from '../platform/config/redis.js';

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
};

type RetentionTarget = {
  table: 'worker_health_snapshots' | 'frontend_error_events' | 'operational_check_results' | 'observer_region_packet_sightings' | 'observer_region_observer_sightings';
  timestampColumn: 'ts' | 'time' | 'last_seen';
  retention: string;
  batchSize: number;
};

const RETENTION_TARGETS: RetentionTarget[] = [
  { table: 'worker_health_snapshots', timestampColumn: 'ts', retention: '14 days', batchSize: 10_000 },
  { table: 'frontend_error_events', timestampColumn: 'time', retention: '30 days', batchSize: 2_000 },
  { table: 'operational_check_results', timestampColumn: 'ts', retention: '14 days', batchSize: 2_000 },
  // This rollup is intentionally limited to the 7-day dashboard window. Deleting
  // it in batches avoids long transactions and lets live ingest continue.
  { table: 'observer_region_packet_sightings', timestampColumn: 'last_seen', retention: '8 days', batchSize: 25_000 },
  { table: 'observer_region_observer_sightings', timestampColumn: 'last_seen', retention: '8 days', batchSize: 2_000 },
];

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

type CpuSample = {
  idle: number;
  total: number;
};

let lastCpuSample: CpuSample | null = null;

function readCpuSample(): CpuSample {
  const cpus = os.cpus();
  let idle = 0;
  let total = 0;
  for (const cpu of cpus) {
    idle += cpu.times.idle;
    total += cpu.times.user + cpu.times.nice + cpu.times.sys + cpu.times.idle + cpu.times.irq;
  }
  return { idle, total };
}

function cpuUsagePct(): number {
  const current = readCpuSample();
  const previous = lastCpuSample;
  lastCpuSample = current;
  if (!previous) return 0;

  const idleDelta = Math.max(0, current.idle - previous.idle);
  const totalDelta = Math.max(0, current.total - previous.total);
  if (totalDelta <= 0) return 0;
  return toPct(1 - idleDelta / totalDelta);
}

function systemStats() {
  const load1 = os.loadavg()[0] ?? 0;
  const cpuCount = os.cpus().length;
  const usagePct = cpuUsagePct();
  const totalMem = os.totalmem();
  const freeMem = os.freemem();
  const usedMem = Math.max(0, totalMem - freeMem);

  let diskTotal = 0;
  let diskUsed = 0;
  try {
    const stat = fs.statfsSync('/');
    diskTotal = Number(stat.blocks) * Number(stat.bsize);
    const free = Number(stat.bavail) * Number(stat.bsize);
    diskUsed = Math.max(0, diskTotal - free);
  } catch {
    // Keep zeros if statfs is unavailable
  }

  return {
    generated_at: new Date().toISOString(),
    cpu: {
      load_1m: load1,
      count: cpuCount,
      load_pct: cpuCount > 0 ? toPct(load1 / cpuCount) : 0,
      usage_pct: usagePct,
    },
    memory: {
      total_mb: Math.round(totalMem / 1_048_576),
      used_mb: Math.round(usedMem / 1_048_576),
      used_pct: totalMem > 0 ? toPct(usedMem / totalMem) : 0,
    },
    disk: {
      total_gb: Math.round((diskTotal / 1_073_741_824) * 10) / 10,
      used_gb: Math.round((diskUsed / 1_073_741_824) * 10) / 10,
      used_pct: diskTotal > 0 ? toPct(diskUsed / diskTotal) : 0,
    },
    runtime: {
      uptime_s: Math.round(os.uptime()),
      node_version: process.version,
      platform: process.platform,
      arch: process.arch,
    },
  };
}

async function currentWorkers(precomputedStats?: ReturnType<typeof systemStats>): Promise<WorkerSnapshot[]> {
  const r = redis();
  const viewshedEnabled = isViewshedFeatureEnabled();
  const [
    viewshedDepth,
    linkDepth,
    viewshedRecent,
    linkRecent,
    viewshedLast,
    linkLast,
    learning,
    healthRecent,
    healthLast,
    backfillState,
  ] = await Promise.all([
    r.llen('meshcore:viewshed_jobs'),
    Promise.all([
      r.llen('meshcore:link_jobs'),
      r.hget('meshcore:link:v3:counters', 'count'),
    ]).then(([legacy, v3]) => Number(legacy ?? 0) + Number(v3 ?? 0)),
    query<{ count: string }>(`SELECT COUNT(*) AS count FROM node_coverage WHERE calculated_at > NOW() - INTERVAL '1 hour'`),
    query<{ count: string }>(`SELECT COUNT(*) AS count FROM node_links WHERE itm_computed_at > NOW() - INTERVAL '1 hour'`),
    query<{ ts: string | null }>(`SELECT MAX(calculated_at)::text AS ts FROM node_coverage`),
    query<{ ts: string | null }>(`SELECT MAX(itm_computed_at)::text AS ts FROM node_links`),
    query<{ ts: string | null }>(`SELECT MAX(updated_at)::text AS ts FROM path_model_calibration`),
    query<{ count: string }>(
      `SELECT COUNT(*) AS count
       FROM worker_health_snapshots
       WHERE worker_name = 'health-worker'
         AND ts > NOW() - INTERVAL '1 hour'`,
    ),
    query<{ ts: string | null }>(
      `SELECT MAX(ts)::text AS ts
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
  const memPct = stats.memory.used_pct;
  const diskPct = stats.disk.used_pct;

  const viewshedProcessed = viewshedEnabled ? Number(viewshedRecent.rows[0]?.count ?? 0) : 0;
  const linkProcessed = Number(linkRecent.rows[0]?.count ?? 0);
  const healthProcessed = Number(healthRecent.rows[0]?.count ?? 0);
  const healthLastTs = healthLast.rows[0]?.ts ?? null;
  const learningLast = learning.rows[0]?.ts ?? null;
  const learningRecent = learningLast ? (Date.now() - Date.parse(learningLast)) <= 60 * 60_000 : false;
  const backfillLinks = Number(backfillState.rows[0]?.links ?? 0);
  const backfillLast = backfillState.rows[0]?.last_observed ?? null;
  return [
    {
      worker_name: 'viewshed-worker',
      status: viewshedEnabled ? (viewshedDepth > 0 || viewshedProcessed > 0 ? 'running' : 'idle') : 'disabled',
      queue_depth: viewshedEnabled ? Number(viewshedDepth ?? 0) : 0,
      processed_1h: viewshedProcessed,
      last_activity_at: viewshedEnabled ? (viewshedLast.rows[0]?.ts ?? null) : null,
      cpu_load_1m: load,
      cpu_usage_pct: stats.cpu.usage_pct,
      mem_used_pct: memPct,
      disk_used_pct: diskPct,
    },
    {
      worker_name: 'link-worker',
      status: linkDepth > 0 || linkProcessed > 0 ? 'running' : 'idle',
      queue_depth: Number(linkDepth ?? 0),
      processed_1h: linkProcessed,
      last_activity_at: linkLast.rows[0]?.ts ?? null,
      cpu_load_1m: load,
      cpu_usage_pct: stats.cpu.usage_pct,
      mem_used_pct: memPct,
      disk_used_pct: diskPct,
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

export async function captureWorkerHealthSnapshot(): Promise<void> {
  const rows = await currentWorkers();
  for (const row of rows) {
    await query(
      `INSERT INTO worker_health_snapshots
         (ts, worker_name, status, queue_depth, processed_5m, processed_1h, last_activity_at, cpu_load_1m, mem_used_pct, disk_used_pct)
       VALUES (NOW(), $1, $2, $3, 0, $4, $5, $6, $7, $8)`,
      [
        row.worker_name,
        row.status,
        row.queue_depth,
        row.processed_1h,
        row.last_activity_at,
        row.cpu_load_1m,
        row.mem_used_pct,
        row.disk_used_pct,
      ],
    );
  }

  for (const target of RETENTION_TARGETS) {
    await deleteExpiredRows(target);
  }
}

export async function getWorkerHealthOverview() {
  // Compute system stats once — cpuUsagePct() diffs against lastCpuSample,
  // so calling it twice in one request gives a garbage near-zero second reading.
  const sysStats = systemStats();
  const [workers, history, errors1h, ingest, pathHashWidths, multibyteSummary, operationalChecks, databaseMaintenance] = await Promise.all([
    currentWorkers(sysStats),
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
    query<{
      check_name: string;
      status: string;
      latency_ms: number;
      detail: string | null;
      ts: string;
    }>(
      `SELECT DISTINCT ON (check_name)
         check_name, status, latency_ms, detail, ts::text
       FROM operational_check_results
       ORDER BY check_name, ts DESC`,
    ),
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
  const problems: Array<{ code: string; severity: 'warning' | 'critical'; message: string }> = [];
  const lastPacketAt = ingestRow?.global_last_packet_at ? Date.parse(ingestRow.global_last_packet_at) : Number.NaN;
  const packetAgeMinutes = Number.isFinite(lastPacketAt) ? Math.floor((Date.now() - lastPacketAt) / 60_000) : null;
  if (packetAgeMinutes == null || packetAgeMinutes > 30) {
    problems.push({
      code: 'ingest_stale',
      severity: packetAgeMinutes == null || packetAgeMinutes > 60 ? 'critical' : 'warning',
      message: packetAgeMinutes == null ? 'No public packet ingest timestamp is available' : `Public ingest is ${packetAgeMinutes} minutes stale`,
    });
  }
  if (sysStats.disk.used_pct >= 90) {
    problems.push({ code: 'disk_pressure', severity: 'critical', message: `Disk usage is ${sysStats.disk.used_pct}%` });
  } else if (sysStats.disk.used_pct >= 80) {
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
  }
  const frontendErrors = Number(errors1h.rows[0]?.count ?? 0);
  if (frontendErrors >= 25) {
    problems.push({
      code: 'frontend_error_spike',
      severity: frontendErrors >= 100 ? 'critical' : 'warning',
      message: `${frontendErrors} frontend errors were recorded in the last hour`,
    });
  }
  const latestChecks = operationalChecks.rows;
  for (const check of latestChecks) {
    const ageMinutes = Math.floor((Date.now() - Date.parse(check.ts)) / 60_000);
    if (check.status !== 'ok' || ageMinutes > 5) {
      problems.push({
        code: check.status !== 'ok' ? 'synthetic_check_failed' : 'synthetic_check_stale',
        severity: check.status !== 'ok' || ageMinutes > 15 ? 'critical' : 'warning',
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

  return {
    status: problems.some((problem) => problem.severity === 'critical')
      ? 'critical'
      : problems.length > 0 ? 'degraded' : 'healthy',
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
    },
    system: sysStats,
    workers,
    history: history.rows,
    frontend_errors_1h: frontendErrors,
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
