import { Redis } from 'ioredis';
import { isViewshedFeatureEnabled } from '../features.js';
import { getRedisConnectionOptions, getRedisUrl } from '../platform/config/redis.js';
import {
  admitLinkV3Job,
  linkObservationIdentity,
  linkPhysicalIdentity,
  type LinkQueueAdmission,
} from './linkQueueV3.js';
import {
  viewshedQueueAdmissionsTotal,
  viewshedQueueBytes,
  viewshedQueueDepth,
} from '../metrics.js';

const VIEWSHED_JOB_QUEUE = 'meshcore:viewshed:v2:ready';
const VIEWSHED_PENDING_SET = 'meshcore:viewshed_pending';
const VIEWSHED_COOLDOWN_PREFIX = 'meshcore:viewshed_cooldown:';
const VIEWSHED_WORKER_HEARTBEAT = 'meshcore:viewshed:worker_heartbeat';
const VIEWSHED_PAYLOADS = 'meshcore:viewshed:v2:payloads';
const VIEWSHED_STATES = 'meshcore:viewshed:v2:states';
const VIEWSHED_ATTEMPTS = 'meshcore:viewshed:v2:attempts';
const VIEWSHED_BYTES = 'meshcore:viewshed:v2:bytes';
const VIEWSHED_QUEUE_COUNTERS = 'meshcore:viewshed:v2:counters';
const VIEWSHED_DEAD = 'meshcore:viewshed:v2:dead';
const VIEWSHED_EVENTS = 'meshcore:viewshed:v2:events';
const VIEWSHED_ENQUEUED = 'meshcore:viewshed:v2:enqueued';
const VIEWSHED_DIRTY = 'meshcore:viewshed:v2:dirty';
const LINK_JOB_QUEUE = 'meshcore:link_jobs';
const LINK_QUEUE_V3_PRODUCER_ENABLED = (process.env['LINK_QUEUE_V3_PRODUCER_ENABLED'] ?? '1') === '1';
const PLANNED_COVERAGE_QUEUE_MAX = Math.min(
  1_000,
  Math.max(1, Number(process.env['PLANNED_COVERAGE_QUEUE_MAX'] ?? 100) || 100),
);
const VIEWSHED_QUEUE_MAX = Math.min(
  10_000,
  Math.max(1, Number(process.env['VIEWSHED_QUEUE_MAX'] ?? 1_000) || 1_000),
);
const VIEWSHED_QUEUE_MAX_BYTES = Math.min(
  256 * 1024 * 1024,
  Math.max(64 * 1024, Number(process.env['VIEWSHED_QUEUE_MAX_BYTES'] ?? 16 * 1024 * 1024) || 16 * 1024 * 1024),
);
const VIEWSHED_MAX_PAYLOAD_BYTES = Math.min(
  64 * 1024,
  Math.max(256, Number(process.env['VIEWSHED_MAX_PAYLOAD_BYTES'] ?? 4 * 1024) || 4 * 1024),
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
let admissionClosed = false;

function getPublisher(): Redis {
  if (admissionClosed) throw new Error('QUEUE_ADMISSION_CLOSED');
  if (pub) return pub;

  pub = new Redis(getRedisUrl(), getRedisConnectionOptions());
  pub.on('error', (e: Error) => console.error('[redis/queue-pub] error', e.message));
  return pub;
}

export function stopQueueAdmission(): void {
  admissionClosed = true;
}

export async function closeQueuePublisher(): Promise<void> {
  admissionClosed = true;
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

export type ViewshedAdmissionStatus =
  | 'accepted'
  | 'coalesced'
  | 'cooldown'
  | 'full'
  | 'oversized'
  | 'worker_unavailable';

const VIEWSHED_ADMIT_SCRIPT = `
if redis.call('EXISTS', KEYS[7]) == 0 then return {'worker_unavailable', 0, 0} end
local payload_bytes = tonumber(ARGV[3])
if payload_bytes > tonumber(ARGV[6]) then return {'oversized', 0, 0} end
local state = redis.call('HGET', KEYS[3], ARGV[1])
if state == 'queued' or state == 'in_flight' then
  local old_bytes = tonumber(redis.call('HGET', KEYS[5], ARGV[1]) or '0')
  local total_bytes = tonumber(redis.call('HGET', KEYS[9], 'bytes') or '0')
  local next_bytes = total_bytes - old_bytes + payload_bytes
  if next_bytes > tonumber(ARGV[5]) then
    return {'full', tonumber(redis.call('HGET', KEYS[9], 'count') or '0'), total_bytes}
  end
  redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
  redis.call('HSET', KEYS[5], ARGV[1], payload_bytes)
  redis.call('HSET', KEYS[9], 'bytes', next_bytes)
  if state == 'in_flight' then redis.call('SADD', KEYS[13], ARGV[1]) end
  return {'coalesced', tonumber(redis.call('HGET', KEYS[9], 'count') or '0'), next_bytes}
end
if state == 'dead' then
  return {'cooldown', tonumber(redis.call('HGET', KEYS[9], 'count') or '0'), tonumber(redis.call('HGET', KEYS[9], 'bytes') or '0')}
end
if ARGV[7] ~= '1' and redis.call('EXISTS', KEYS[8]) == 1 then
  return {'cooldown', tonumber(redis.call('HGET', KEYS[9], 'count') or '0'), tonumber(redis.call('HGET', KEYS[9], 'bytes') or '0')}
end
local count = tonumber(redis.call('HGET', KEYS[9], 'count') or '0')
local bytes = tonumber(redis.call('HGET', KEYS[9], 'bytes') or '0')
if count + 1 > tonumber(ARGV[4]) or bytes + payload_bytes > tonumber(ARGV[5]) then
  return {'full', count, bytes}
end
redis.call('SET', KEYS[8], '1', 'EX', tonumber(ARGV[8]))
redis.call('SADD', KEYS[6], ARGV[1])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[3], ARGV[1], 'queued')
redis.call('HSET', KEYS[4], ARGV[1], '0')
redis.call('HSET', KEYS[5], ARGV[1], payload_bytes)
redis.call('LPUSH', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[9], 'count', count + 1, 'bytes', bytes + payload_bytes)
redis.call('ZADD', KEYS[12], ARGV[9], ARGV[1])
redis.call('LPUSH', KEYS[11], 'admit')
redis.call('LTRIM', KEYS[11], 0, 255)
return {'accepted', count + 1, bytes + payload_bytes}
`;

export async function admitViewshedV2Job(
  publisher: Redis,
  nodeId: string,
  lat: number,
  lon: number,
  force: boolean,
  plannedJobId?: string,
): Promise<ViewshedAdmissionStatus> {
  const cooldownKey = `${VIEWSHED_COOLDOWN_PREFIX}${nodeId}`;
  const payload = JSON.stringify({
    node_id: nodeId,
    lat,
    lon,
    ...(plannedJobId ? { planned_job_id: plannedJobId } : {}),
  });
  const payloadBytes = Buffer.byteLength(payload, 'utf8');
  const result = await publisher.eval(
    VIEWSHED_ADMIT_SCRIPT,
    13,
    VIEWSHED_JOB_QUEUE,
    VIEWSHED_PAYLOADS,
    VIEWSHED_STATES,
    VIEWSHED_ATTEMPTS,
    VIEWSHED_BYTES,
    VIEWSHED_PENDING_SET,
    VIEWSHED_WORKER_HEARTBEAT,
    cooldownKey,
    VIEWSHED_QUEUE_COUNTERS,
    VIEWSHED_DEAD,
    VIEWSHED_EVENTS,
    VIEWSHED_ENQUEUED,
    VIEWSHED_DIRTY,
    nodeId,
    payload,
    payloadBytes,
    plannedJobId ? PLANNED_COVERAGE_QUEUE_MAX : VIEWSHED_QUEUE_MAX,
    VIEWSHED_QUEUE_MAX_BYTES,
    VIEWSHED_MAX_PAYLOAD_BYTES,
    force ? '1' : '0',
    VIEWSHED_JOB_COOLDOWN_SECONDS,
    Date.now(),
  ) as [ViewshedAdmissionStatus, number | string, number | string];
  const status = String(result[0]) as ViewshedAdmissionStatus;
  viewshedQueueAdmissionsTotal.inc({ status });
  viewshedQueueDepth.set(Number(result[1]) || 0);
  viewshedQueueBytes.set(Number(result[2]) || 0);
  return status;
}

export async function isViewshedWorkerHealthy(): Promise<boolean> {
  if (admissionClosed) return false;
  if (!isViewshedFeatureEnabled()) return false;
  const publisher = getPublisher();
  const [healthy, depth] = await Promise.all([
    publisher.exists(VIEWSHED_WORKER_HEARTBEAT),
    publisher.hget(VIEWSHED_QUEUE_COUNTERS, 'count'),
  ]);
  return healthy === 1 && Number(depth ?? 0) < PLANNED_COVERAGE_QUEUE_MAX;
}

export async function queuePlannedViewshedJob(
  jobId: string,
  lat: number,
  lon: number,
): Promise<void> {
  if (admissionClosed) throw new Error('QUEUE_ADMISSION_CLOSED');
  if (!isViewshedFeatureEnabled()) throw new Error('VIEWSHED_DISABLED');
  if (!isViewshedEligibleCoordinate(lat, lon)) throw new Error('VIEWSHED_COORDINATE_INVALID');
  const status = await admitViewshedV2Job(getPublisher(), jobId, lat, lon, false, jobId);
  if (status === 'full') throw new Error('VIEWSHED_QUEUE_FULL');
  if (status === 'oversized') throw new Error('VIEWSHED_JOB_OVERSIZED');
  if (status === 'worker_unavailable') throw new Error('VIEWSHED_WORKER_UNAVAILABLE');
}

/** Push a viewshed calculation job for a node with a known position. */
export function queueViewshedJob(nodeId: string, lat: number, lon: number, force = false): void {
  if (admissionClosed) return;
  if (!isViewshedFeatureEnabled()) return;
  if (!isViewshedEligibleCoordinate(lat, lon)) return;
  const publisher = getPublisher();
  void admitViewshedV2Job(publisher, nodeId, lat, lon, force)
    .then((status) => {
      if (['full', 'oversized', 'worker_unavailable'].includes(status)) {
        console.warn('[redis/queue-pub] viewshed job not admitted', status, nodeId);
      }
    })
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
  if (admissionClosed) return Promise.resolve({ status: 'worker_unavailable', jobId: null });
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
  if (admissionClosed) return Promise.resolve({ status: 'worker_unavailable', jobId: null });
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
