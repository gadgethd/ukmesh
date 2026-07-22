import { Redis } from 'ioredis';
import { isViewshedFeatureEnabled } from '../features.js';
import { getRedisConnectionOptions, getRedisUrl } from '../platform/config/redis.js';

const VIEWSHED_JOB_QUEUE = 'meshcore:viewshed_jobs';
const VIEWSHED_PENDING_SET = 'meshcore:viewshed_pending';
const VIEWSHED_COOLDOWN_PREFIX = 'meshcore:viewshed_cooldown:';
const LINK_JOB_QUEUE = 'meshcore:link_jobs';
const VIEWSHED_JOB_COOLDOWN_SECONDS = Math.max(
  30,
  Math.min(3_600, Number(process.env['VIEWSHED_JOB_COOLDOWN_SECONDS'] ?? 300) || 300),
);

const UK_LAT_MIN = 49.5;
const UK_LAT_MAX = 61.5;
const UK_LON_MIN = -8.5;
const UK_LON_MAX = 2.5;

let pub: Redis | null = null;

function getPublisher(): Redis {
  if (pub) return pub;

  pub = new Redis(getRedisUrl(), getRedisConnectionOptions());
  pub.on('error', (e: Error) => console.error('[redis/queue-pub] error', e.message));
  return pub;
}

export async function closeQueuePublisher(): Promise<void> {
  if (!pub) return;
  await pub.quit();
  pub = null;
}

/** Push a viewshed calculation job for a node with a known position. */
export function isViewshedEligibleCoordinate(lat: number, lon: number): boolean {
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;
  if (Math.abs(lat) < 1e-9 && Math.abs(lon) < 1e-9) return false;
  return lat >= UK_LAT_MIN && lat <= UK_LAT_MAX && lon >= UK_LON_MIN && lon <= UK_LON_MAX;
}

async function enqueueViewshedJob(
  publisher: Redis,
  nodeId: string,
  lat: number,
  lon: number,
  force: boolean,
): Promise<void> {
  const added = await publisher.sadd(VIEWSHED_PENDING_SET, nodeId);
  if (added !== 1) return;

  const cooldownKey = `${VIEWSHED_COOLDOWN_PREFIX}${nodeId}`;
  try {
    const allowed = force
      ? Boolean(await publisher.set(cooldownKey, '1', 'EX', VIEWSHED_JOB_COOLDOWN_SECONDS))
      : (await publisher.set(cooldownKey, '1', 'EX', VIEWSHED_JOB_COOLDOWN_SECONDS, 'NX')) === 'OK';
    if (!allowed) {
      await publisher.srem(VIEWSHED_PENDING_SET, nodeId);
      return;
    }
    await publisher.lpush(VIEWSHED_JOB_QUEUE, JSON.stringify({ node_id: nodeId, lat, lon }));
  } catch (err) {
    await publisher.srem(VIEWSHED_PENDING_SET, nodeId).catch(() => {});
    throw err;
  }
}

/** Push a viewshed calculation job for a node with a known position. */
export function queueViewshedJob(nodeId: string, lat: number, lon: number, force = false): void {
  if (!isViewshedFeatureEnabled()) return;
  if (!isViewshedEligibleCoordinate(lat, lon)) return;
  const publisher = getPublisher();
  void enqueueViewshedJob(publisher, nodeId, lat, lon, force)
    .catch((e: Error) => console.error('[redis/queue-pub] viewshed enqueue error', e.message));
}

/** Push a link observation job for a received packet with relay path data. */
export function queueLinkJob(
  rxNodeId: string,
  srcNodeId: string | undefined,
  pathHashes: string[],
  hopCount: number | undefined,
  pathHashSizeBytes: number | undefined,
): void {
  if (!pathHashes.length || (pathHashSizeBytes ?? 1) <= 1) return;
  void getPublisher().lpush(LINK_JOB_QUEUE, JSON.stringify({
    type: 'observe',
    rx_node_id: rxNodeId,
    src_node_id: srcNodeId,
    path_hashes: pathHashes,
    hop_count: hopCount,
    path_hash_size_bytes: pathHashSizeBytes,
  }));
}

/** Push a physical pair evaluation job for two positioned repeater nodes. */
export function queuePhysicalLinkJob(nodeAId: string, nodeBId: string): void {
  if (!nodeAId || !nodeBId || nodeAId === nodeBId) return;
  const [aId, bId] = nodeAId < nodeBId ? [nodeAId, nodeBId] : [nodeBId, nodeAId];
  void getPublisher().lpush(LINK_JOB_QUEUE, JSON.stringify({
    type: 'physical_pair',
    node_a_id: aId,
    node_b_id: bId,
  }));
}
