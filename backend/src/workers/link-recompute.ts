import 'node:process';
import { randomBytes } from 'node:crypto';
import { Redis } from 'ioredis';
import { initDb, pool, query } from '../db/index.js';
import { backfillHistoricalLinks } from '../mqtt/client.js';
import {
  queueLinkJob,
  queuePhysicalLinkJob,
  closeQueuePublisher,
} from '../queue/publisher.js';
import {
  LINK_V3_KEYS,
  releaseDeferredLinkJobs,
  type LinkQueueAdmission,
} from '../queue/linkQueueV3.js';
import { getRedisConnectionOptions, getRedisUrl } from '../platform/config/redis.js';

const LEGACY_LINK_JOB_QUEUE = 'meshcore:link_jobs';
const DEFAULT_PHYSICAL_RADIUS_KM = 60;
const MIN_PHYSICAL_RADIUS_KM = 20;
const MAX_PHYSICAL_RADIUS_KM = 100;
const PHYSICAL_RADIUS_MARGIN = 1.25;
const ADMISSION_TIMEOUT_MS = Math.max(
  60_000,
  Number(process.env['LINK_REBUILD_ADMISSION_TIMEOUT_MS'] ?? 30 * 60_000) || 30 * 60_000,
);
const COMPLETION_TIMEOUT_MS = Math.max(
  60_000,
  Number(process.env['LINK_REBUILD_COMPLETION_TIMEOUT_MS'] ?? 6 * 60 * 60_000) || 6 * 60 * 60_000,
);
const MAX_REBUILD_NODES = Math.min(
  20_000,
  Math.max(100, Number(process.env['LINK_REBUILD_MAX_NODES'] ?? 5_000) || 5_000),
);
const MAX_REBUILD_PHYSICAL_JOBS = Math.min(
  1_000_000,
  Math.max(1_000, Number(process.env['LINK_REBUILD_MAX_PHYSICAL_JOBS'] ?? 200_000) || 200_000),
);

type PhysicalNodeRow = {
  node_id: string;
  lat: number;
  lon: number;
  radius_m: number | null;
};

function distKm(a: PhysicalNodeRow, b: PhysicalNodeRow): number {
  const cos = Math.cos(((a.lat + b.lat) / 2) * Math.PI / 180);
  const dLat = (a.lat - b.lat) * 111.32;
  const dLon = (a.lon - b.lon) * 111.32 * cos;
  return Math.sqrt(dLat * dLat + dLon * dLon);
}

function candidateRadiusKm(node: PhysicalNodeRow): number {
  const derived = node.radius_m != null
    ? (node.radius_m / 1000) * PHYSICAL_RADIUS_MARGIN
    : DEFAULT_PHYSICAL_RADIUS_KM;
  return Math.min(MAX_PHYSICAL_RADIUS_KM, Math.max(MIN_PHYSICAL_RADIUS_KM, derived));
}

function accepted(admission: LinkQueueAdmission | null): admission is LinkQueueAdmission & { jobId: string } {
  return admission != null
    && admission.jobId != null
    && ['accepted', 'coalesced', 'duplicate'].includes(admission.status);
}

async function waitForAdmission(
  enqueue: () => Promise<LinkQueueAdmission | null>,
  deadline: number,
): Promise<string | null> {
  while (true) {
    const result = await enqueue();
    if (accepted(result)) return result.jobId;
    if (!result) return null;
    if (result.status !== 'full') {
      throw new Error(`LINK_REBUILD_ADMISSION_${result.status.toUpperCase()}`);
    }
    if (Date.now() >= deadline) throw new Error('LINK_REBUILD_ADMISSION_TIMEOUT');
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
}

async function waitForPreexistingDrain(redis: Redis, deadline: number): Promise<void> {
  while (true) {
    const [ready, leased, legacy] = await Promise.all([
      redis.llen(LINK_V3_KEYS.ready),
      redis.zcard(LINK_V3_KEYS.leases),
      redis.llen(LEGACY_LINK_JOB_QUEUE),
    ]);
    if (ready === 0 && leased === 0 && legacy === 0) return;
    if (Date.now() >= deadline) throw new Error('LINK_REBUILD_PREEXISTING_DRAIN_TIMEOUT');
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
}

async function waitForV3DrainAfterFreeze(redis: Redis, deadline: number): Promise<void> {
  while (true) {
    const [ready, leased, legacy] = await Promise.all([
      redis.llen(LINK_V3_KEYS.ready),
      redis.zcard(LINK_V3_KEYS.leases),
      redis.llen(LEGACY_LINK_JOB_QUEUE),
    ]);
    if (legacy > 0) throw new Error('LINK_REBUILD_LEGACY_PRODUCER_ACTIVE');
    if (ready === 0 && leased === 0) return;
    if (Date.now() >= deadline) throw new Error('LINK_REBUILD_PREEXISTING_DRAIN_TIMEOUT');
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
}

async function waitForGeneration(
  redis: Redis,
  generation: string,
  expectedJobs: number,
  deadline: number,
): Promise<void> {
  while (true) {
    const completed = await query<{ count: string }>(
      'SELECT COUNT(*)::text AS count FROM link_job_commits WHERE generation = $1',
      [generation],
    );
    const completedJobs = Number(completed.rows[0]?.count ?? 0);
    await query(
      `UPDATE link_rebuild_runs
          SET completed_jobs = $2, status = 'processing', updated_at = NOW()
        WHERE generation = $1`,
      [generation, completedJobs],
    );
    if (completedJobs >= expectedJobs) return;
    if (await redis.llen(LEGACY_LINK_JOB_QUEUE) > 0) {
      throw new Error('LINK_REBUILD_LEGACY_PRODUCER_ACTIVE');
    }
    if (Date.now() >= deadline) throw new Error('LINK_REBUILD_COMPLETION_TIMEOUT');
    await new Promise((resolve) => setTimeout(resolve, 1_000));
  }
}

async function main(): Promise<void> {
  await initDb();
  const redis = new Redis(getRedisUrl(), getRedisConnectionOptions());
  redis.on('error', (error: Error) => console.error('[link-recompute/redis]', error.message));

  const suffix = randomBytes(8).toString('hex');
  const schemaName = `link_rebuild_${suffix}`;
  const rollbackSchema = `link_rollback_${suffix}`;
  const generation = schemaName;
  let windowEnd = new Date();
  const rebuildValue = JSON.stringify({ generation, startedAt: new Date().toISOString() });

  let published = false;
  let rebuildHeld = false;
  try {
    const heartbeat = await redis.exists(LINK_V3_KEYS.workerHeartbeat);
    if (heartbeat !== 1) throw new Error('LINK_REBUILD_WORKER_UNAVAILABLE');

    await waitForPreexistingDrain(redis, Date.now() + ADMISSION_TIMEOUT_MS);
    const acquired = await redis.set(LINK_V3_KEYS.rebuild, rebuildValue, 'NX');
    if (acquired !== 'OK') throw new Error('LINK_REBUILD_ALREADY_ACTIVE');
    rebuildHeld = true;
    await waitForV3DrainAfterFreeze(redis, Date.now() + ADMISSION_TIMEOUT_MS);
    windowEnd = new Date();
    await query(`CREATE SCHEMA ${schemaName}`);
    await query(`CREATE TABLE ${schemaName}.node_links (LIKE public.node_links INCLUDING ALL)`);
    await query(
      `INSERT INTO link_rebuild_runs (generation, schema_name, status, window_end)
       VALUES ($1, $2, 'preparing', $3)`,
      [generation, schemaName, windowEnd],
    );

    const nodes = await query<PhysicalNodeRow>(
      `SELECT n.node_id, n.lat, n.lon, nc.radius_m
         FROM nodes n
         LEFT JOIN node_coverage nc ON nc.node_id = n.node_id
        WHERE n.lat IS NOT NULL
          AND n.lon IS NOT NULL
          AND n.lat BETWEEN 49.5 AND 61.5
          AND n.lon BETWEEN -8.5 AND 2.5
          AND NOT (ABS(n.lat) < 1e-9 AND ABS(n.lon) < 1e-9)
          AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
          AND (n.role IS NULL OR n.role = 2)
        ORDER BY n.node_id
        LIMIT $1`,
      [MAX_REBUILD_NODES + 1],
    );
    if (nodes.rows.length > MAX_REBUILD_NODES) throw new Error('LINK_REBUILD_NODE_LIMIT');

    const jobIds = new Set<string>();
    const admissionDeadline = Date.now() + ADMISSION_TIMEOUT_MS;
    for (let i = 0; i < nodes.rows.length; i += 1) {
      const a = nodes.rows[i]!;
      const aRadiusKm = candidateRadiusKm(a);
      for (let j = i + 1; j < nodes.rows.length; j += 1) {
        const b = nodes.rows[j]!;
        if (distKm(a, b) > Math.max(aRadiusKm, candidateRadiusKm(b))) continue;
        if (jobIds.size >= MAX_REBUILD_PHYSICAL_JOBS) {
          throw new Error('LINK_REBUILD_PHYSICAL_JOB_LIMIT');
        }
        const jobId = await waitForAdmission(
          () => queuePhysicalLinkJob(a.node_id, b.node_id, generation),
          admissionDeadline,
        );
        if (jobId) jobIds.add(jobId);
      }
    }

    await backfillHistoricalLinks(async (
      packetHash,
      rxNodeId,
      srcNodeId,
      path,
      hopCount,
      pathHashSizeBytes,
    ) => {
      const jobId = await waitForAdmission(
        () => queueLinkJob(
          packetHash,
          rxNodeId,
          srcNodeId,
          path,
          hopCount,
          pathHashSizeBytes,
          generation,
        ),
        admissionDeadline,
      );
      if (jobId) jobIds.add(jobId);
    }, { windowEnd });

    await query(
      `UPDATE link_rebuild_runs
          SET status = 'processing',
              expected_jobs = $2,
              admitted_jobs = $2,
              updated_at = NOW()
        WHERE generation = $1`,
      [generation, jobIds.size],
    );
    await waitForGeneration(redis, generation, jobIds.size, Date.now() + COMPLETION_TIMEOUT_MS);

    if (await redis.llen(LEGACY_LINK_JOB_QUEUE) > 0) {
      throw new Error('LINK_REBUILD_LEGACY_PRODUCER_ACTIVE');
    }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query("SELECT pg_advisory_xact_lock(hashtext('meshcore-link-graph-write'))");
      await client.query(`CREATE SCHEMA ${rollbackSchema}`);
      await client.query(`CREATE TABLE ${rollbackSchema}.node_links (LIKE public.node_links INCLUDING ALL)`);
      await client.query(`INSERT INTO ${rollbackSchema}.node_links SELECT * FROM public.node_links`);
      await client.query('LOCK TABLE public.node_links IN ACCESS EXCLUSIVE MODE');
      await client.query('TRUNCATE public.node_links');
      await client.query(`INSERT INTO public.node_links SELECT * FROM ${schemaName}.node_links`);
      // A live job admitted during the freeze may represent a packet already
      // incorporated into this generation. Record its stable logical ID before
      // releasing deferred work so replay becomes an idempotent ACK.
      await client.query(
        `INSERT INTO link_job_commits
           (job_id, job_type, generation, logical_job_id, payload_hash)
         SELECT logical_job_id, job_type, NULL, logical_job_id, payload_hash
           FROM link_job_commits
          WHERE generation = $1
            AND logical_job_id IS NOT NULL
         ON CONFLICT (job_id) DO NOTHING`,
        [generation],
      );
      await client.query(
        `INSERT INTO public.node_links
         SELECT * FROM ${rollbackSchema}.node_links WHERE force_viable = true
         ON CONFLICT (node_a_id, node_b_id) DO UPDATE
           SET force_viable = true`,
      );
      await client.query(
        `UPDATE link_rebuild_runs
            SET status = 'published', published_at = NOW(), updated_at = NOW()
          WHERE generation = $1`,
        [generation],
      );
      await client.query('COMMIT');
      published = true;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

    const released = await releaseDeferredLinkJobs(redis);
    console.log(
      `[link-recompute] published generation=${generation} jobs=${jobIds.size} deferred_released=${released} rollback_schema=${rollbackSchema}`,
    );
  } catch (error) {
    await query(
      `UPDATE link_rebuild_runs
          SET status = 'failed', error = $2, updated_at = NOW()
        WHERE generation = $1`,
      [generation, error instanceof Error ? error.message : String(error)],
    ).catch(() => undefined);
    throw error;
  } finally {
    if (!published && rebuildHeld) {
      await redis.del(LINK_V3_KEYS.rebuild).catch(() => 0);
      await releaseDeferredLinkJobs(redis).catch(() => 0);
    }
    await redis.quit().catch(() => undefined);
  }
}

main()
  .catch((error) => {
    console.error('[link-recompute] fatal error:', error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closeQueuePublisher();
    await pool.end();
  });
