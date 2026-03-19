import 'node:process';
import { Redis } from 'ioredis';
import { initDb, pool, query } from '../db/index.js';
import { backfillHistoricalLinks } from '../mqtt/client.js';
import { queueLinkJob, queuePhysicalLinkJob, closeQueuePublisher } from '../queue/publisher.js';

const LINK_JOB_QUEUE = 'meshcore:link_jobs';
const DEFAULT_PHYSICAL_RADIUS_KM = 60;
const MIN_PHYSICAL_RADIUS_KM = 20;
const MAX_PHYSICAL_RADIUS_KM = 100;
const PHYSICAL_RADIUS_MARGIN = 1.25;

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
  const derived = node.radius_m != null ? (node.radius_m / 1000) * PHYSICAL_RADIUS_MARGIN : DEFAULT_PHYSICAL_RADIUS_KM;
  return Math.min(MAX_PHYSICAL_RADIUS_KM, Math.max(MIN_PHYSICAL_RADIUS_KM, derived));
}

async function main() {
  await initDb();

  const redisUrl = process.env['REDIS_URL'] ?? 'redis://redis:6379';
  const redis = new Redis(redisUrl);
  redis.on('error', (err: Error) => console.error('[link-recompute/redis] error:', err.message));

  try {
    const before = await query<{ count: string; forced: string }>(
      `SELECT
         COUNT(*)::text AS count,
         COUNT(*) FILTER (WHERE force_viable = true)::text AS forced
       FROM node_links`,
    );
    const row = before.rows[0];
    const existingCount = Number(row?.count ?? 0);
    const forcedCount = Number(row?.forced ?? 0);
    console.log(`[link-recompute] existing node_links=${existingCount} forced_overrides=${forcedCount}`);

    const clearedQueue = await redis.del(LINK_JOB_QUEUE).catch(() => 0);
    console.log(`[link-recompute] cleared redis queue ${LINK_JOB_QUEUE}: removed=${clearedQueue}`);

    await query(
      `DELETE FROM node_links`,
    );

    const afterReset = await query<{ count: string; forced: string }>(
      `SELECT
         COUNT(*)::text AS count,
         COUNT(*) FILTER (WHERE force_viable = true)::text AS forced
       FROM node_links`,
    );
    const resetRow = afterReset.rows[0];
    console.log(
      `[link-recompute] node_links reset complete remaining_rows=${Number(resetRow?.count ?? 0)} forced_overrides=${Number(resetRow?.forced ?? 0)}`,
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
       ORDER BY n.node_id`,
    );

    let queuedPhysical = 0;
    for (let i = 0; i < nodes.rows.length; i += 1) {
      const a = nodes.rows[i]!;
      const aRadiusKm = candidateRadiusKm(a);
      for (let j = i + 1; j < nodes.rows.length; j += 1) {
        const b = nodes.rows[j]!;
        const maxRadiusKm = Math.max(aRadiusKm, candidateRadiusKm(b));
        if (distKm(a, b) > maxRadiusKm) continue;
        queuePhysicalLinkJob(a.node_id, b.node_id);
        queuedPhysical += 1;
      }
    }
    console.log(`[link-recompute] queued physical pair jobs=${queuedPhysical}`);

    await backfillHistoricalLinks((rxNodeId, srcNodeId, path, hopCount, pathHashSizeBytes) => {
      queueLinkJob(rxNodeId, srcNodeId, path, hopCount, pathHashSizeBytes);
    });

    const queuedDepth = await redis.llen(LINK_JOB_QUEUE).catch(() => -1);
    console.log(`[link-recompute] historical link rebuild queued depth=${queuedDepth}`);
  } finally {
    await redis.quit().catch(() => {});
  }
}

main()
  .catch((err) => {
    console.error('[link-recompute] fatal error:', err);
    process.exit(1);
  })
  .finally(async () => {
    await closeQueuePublisher();
    await pool.end();
    process.exit(0);
  });
