import { Redis } from 'ioredis';
import { isViewshedFeatureEnabled } from '../features.js';
import { getRedisConnectionOptions, getRedisUrl } from '../platform/config/redis.js';
import {
  admitLinkV3Job,
  linkObservationIdentity,
  linkPhysicalIdentity,
  type LinkQueueAdmission,
} from './linkQueueV3.js';

const VIEWSHED_JOB_QUEUE = 'meshcore:viewshed_jobs';
const VIEWSHED_PENDING_SET = 'meshcore:viewshed_pending';
const VIEWSHED_COOLDOWN_PREFIX = 'meshcore:viewshed_cooldown:';
const VIEWSHED_WORKER_HEARTBEAT = 'meshcore:viewshed:worker_heartbeat';
const LINK_JOB_QUEUE = 'meshcore:link_jobs';
const LINK_QUEUE_V3_PRODUCER_ENABLED = (process.env['LINK_QUEUE_V3_PRODUCER_ENABLED'] ?? '1') === '1';
const PLANNED_COVERAGE_QUEUE_MAX = Math.min(
  1_000,
  Math.max(1, Number(process.env['PLANNED_COVERAGE_QUEUE_MAX'] ?? 100) || 100),
);
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
  plannedJobId?: string,
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
    await publisher.lpush(VIEWSHED_JOB_QUEUE, JSON.stringify({
      node_id: nodeId,
      lat,
      lon,
      ...(plannedJobId ? { planned_job_id: plannedJobId } : {}),
    }));
  } catch (err) {
    await publisher.srem(VIEWSHED_PENDING_SET, nodeId).catch(() => {});
    throw err;
  }
}

export async function isViewshedWorkerHealthy(): Promise<boolean> {
  if (!isViewshedFeatureEnabled()) return false;
  const publisher = getPublisher();
  const [healthy, depth] = await Promise.all([
    publisher.exists(VIEWSHED_WORKER_HEARTBEAT),
    publisher.llen(VIEWSHED_JOB_QUEUE),
  ]);
  return healthy === 1 && depth < PLANNED_COVERAGE_QUEUE_MAX;
}

export async function queuePlannedViewshedJob(
  jobId: string,
  lat: number,
  lon: number,
): Promise<void> {
  if (!isViewshedFeatureEnabled()) throw new Error('VIEWSHED_DISABLED');
  if (!isViewshedEligibleCoordinate(lat, lon)) throw new Error('VIEWSHED_COORDINATE_INVALID');
  const payload = JSON.stringify({
    node_id: jobId,
    planned_job_id: jobId,
    lat,
    lon,
  });
  const result = Number(await getPublisher().eval(
    `if redis.call('LLEN', KEYS[1]) >= tonumber(ARGV[2]) then return 0 end
     local added = redis.call('SADD', KEYS[2], ARGV[1])
     if added == 0 then return 2 end
     redis.call('LPUSH', KEYS[1], ARGV[3])
     return 1`,
    2,
    VIEWSHED_JOB_QUEUE,
    VIEWSHED_PENDING_SET,
    jobId,
    PLANNED_COVERAGE_QUEUE_MAX,
    payload,
  ));
  if (result === 0) throw new Error('VIEWSHED_QUEUE_FULL');
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
  packetHash: string,
  rxNodeId: string,
  srcNodeId: string | undefined,
  pathHashes: string[],
  hopCount: number | undefined,
  pathHashSizeBytes: number | undefined,
  generation?: string,
): Promise<LinkQueueAdmission | null> {
  if (!pathHashes.length || (pathHashSizeBytes ?? 1) <= 1) return Promise.resolve(null);
  const publisher = getPublisher();
  if (!LINK_QUEUE_V3_PRODUCER_ENABLED) {
    return publisher.lpush(LINK_JOB_QUEUE, JSON.stringify({
      type: 'observe',
      packet_hash: packetHash,
      rx_node_id: rxNodeId,
      src_node_id: srcNodeId,
      path_hashes: pathHashes,
      hop_count: hopCount,
      path_hash_size_bytes: pathHashSizeBytes,
    })).then(() => ({ status: 'accepted', jobId: `legacy:${packetHash}:${rxNodeId}` }));
  }
  const identity = linkObservationIdentity(packetHash, rxNodeId, generation);
  const logicalIdentity = linkObservationIdentity(packetHash, rxNodeId);
  return admitLinkV3Job(publisher, {
    version: 3,
    type: 'observe',
    job_id: identity.jobId,
    dedupe_key: identity.dedupeKey,
    logical_job_id: logicalIdentity.jobId,
    packet_hash: packetHash,
    rx_node_id: rxNodeId,
    src_node_id: srcNodeId,
    path_hashes: pathHashes,
    hop_count: hopCount,
    path_hash_size_bytes: pathHashSizeBytes,
    ...(generation ? { generation } : {}),
  });
}

/** Push a physical pair evaluation job for two positioned repeater nodes. */
export function queuePhysicalLinkJob(
  nodeAId: string,
  nodeBId: string,
  generation?: string,
): Promise<LinkQueueAdmission | null> {
  if (!nodeAId || !nodeBId || nodeAId === nodeBId) return Promise.resolve(null);
  const publisher = getPublisher();
  const identity = linkPhysicalIdentity(nodeAId, nodeBId, generation);
  if (!LINK_QUEUE_V3_PRODUCER_ENABLED) {
    return publisher.lpush(LINK_JOB_QUEUE, JSON.stringify({
      type: 'physical_pair',
      node_a_id: identity.nodeAId,
      node_b_id: identity.nodeBId,
    })).then(() => ({ status: 'accepted', jobId: `legacy:${identity.nodeAId}:${identity.nodeBId}` }));
  }
  return admitLinkV3Job(publisher, {
    version: 3,
    type: 'physical_pair',
    job_id: identity.jobId,
    dedupe_key: identity.dedupeKey,
    node_a_id: identity.nodeAId,
    node_b_id: identity.nodeBId,
    ...(generation ? { generation } : {}),
  });
}
