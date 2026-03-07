import os from 'node:os';
import fs from 'node:fs';
import { Redis } from 'ioredis';
import { query } from '../db/index.js';

type WorkerSnapshot = {
  worker_name: string;
  status: string;
  queue_depth: number;
  processed_1h: number;
  last_activity_at: string | null;
  cpu_load_1m: number;
  mem_used_pct: number;
  disk_used_pct: number;
};

let redisClient: Redis | null = null;

function redis(): Redis {
  if (!redisClient) {
    const redisUrl = process.env['REDIS_URL'] ?? 'redis://redis:6379';
    redisClient = new Redis(redisUrl);
    redisClient.on('error', (err) => console.error('[health] redis error', err.message));
  }
  return redisClient;
}

function toPct(num: number): number {
  return Math.round(num * 1000) / 10;
}

function systemStats() {
  const load1 = os.loadavg()[0] ?? 0;
  const cpuCount = os.cpus().length;
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

async function currentWorkers(): Promise<WorkerSnapshot[]> {
  const r = redis();
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
    pathSimLatest,
  ] = await Promise.all([
    r.llen('meshcore:viewshed_jobs'),
    r.llen('meshcore:link_jobs'),
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
    query<{ completed_at: string | null }>(
      `SELECT MAX(completed_at)::text AS completed_at
       FROM path_simulation_runs`,
    ),
  ]);

  const stats = systemStats();
  const load = stats.cpu.load_1m;
  const memPct = stats.memory.used_pct;
  const diskPct = stats.disk.used_pct;

  const viewshedProcessed = Number(viewshedRecent.rows[0]?.count ?? 0);
  const linkProcessed = Number(linkRecent.rows[0]?.count ?? 0);
  const healthProcessed = Number(healthRecent.rows[0]?.count ?? 0);
  const healthLastTs = healthLast.rows[0]?.ts ?? null;
  const learningLast = learning.rows[0]?.ts ?? null;
  const learningRecent = learningLast ? (Date.now() - Date.parse(learningLast)) <= 60 * 60_000 : false;
  const backfillLinks = Number(backfillState.rows[0]?.links ?? 0);
  const backfillLast = backfillState.rows[0]?.last_observed ?? null;
  const pathSimLast = pathSimLatest.rows[0]?.completed_at ?? null;
  const pathSimRecent = pathSimLast ? (Date.now() - Date.parse(pathSimLast)) <= 6 * 60 * 60_000 : false;

  return [
    {
      worker_name: 'viewshed-worker',
      status: viewshedDepth > 0 || viewshedProcessed > 0 ? 'running' : 'idle',
      queue_depth: Number(viewshedDepth ?? 0),
      processed_1h: viewshedProcessed,
      last_activity_at: viewshedLast.rows[0]?.ts ?? null,
      cpu_load_1m: load,
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
      mem_used_pct: memPct,
      disk_used_pct: diskPct,
    },
    {
      worker_name: 'path-sim-worker',
      status: pathSimRecent ? 'running' : 'idle',
      queue_depth: 0,
      processed_1h: pathSimLast && (Date.now() - Date.parse(pathSimLast)) <= 60 * 60_000 ? 1 : 0,
      last_activity_at: pathSimLast,
      cpu_load_1m: load,
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

  await query(`DELETE FROM worker_health_snapshots WHERE ts < NOW() - INTERVAL '14 days'`);
  await query(`DELETE FROM frontend_error_events WHERE time < NOW() - INTERVAL '30 days'`);
}

export async function getWorkerHealthOverview() {
  const [workers, history, errors1h, ingest, pathHashWidths, multibyteSummary] = await Promise.all([
    currentWorkers(),
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
         SELECT rx_node_id, MAX(time) AS last_packet_at
         FROM packets
         WHERE rx_node_id IS NOT NULL
           AND rx_node_id <> ''
         GROUP BY rx_node_id
       ),
       active_rx AS (
         SELECT rx_node_id, last_packet_at
         FROM latest_rx
         WHERE last_packet_at > NOW() - INTERVAL '7 days'
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
         (SELECT MAX(time)::text FROM packets) AS global_last_packet_at
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
       WHERE time > NOW() - INTERVAL '24 hours'`,
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

  return {
    system: systemStats(),
    workers,
    history: history.rows,
    frontend_errors_1h: Number(errors1h.rows[0]?.count ?? 0),
    ingest: {
      stale_nodes: staleNodes,
      active_nodes: activeNodes,
      max_stale_minutes: staleNodes > 0 ? maxStaleMinutes : 0,
      stale_threshold_minutes: staleThresholdMinutes,
      global_last_packet_at: ingestRow?.global_last_packet_at ?? null,
    },
    path_hashes: {
      last_24h_hops: pathHashStats,
      multibyte_packets_24h: Number(multibyteRow?.multibyte_packets_24h ?? 0),
      latest_multibyte_at: multibyteRow?.latest_multibyte_at ?? null,
    },
  };
}
