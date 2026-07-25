import { Redis } from 'ioredis';
import { createHash, randomBytes } from 'node:crypto';
import { isViewshedFeatureEnabled } from '../features.js';
import { getRedisConnectionOptions, getRedisUrl } from '../platform/config/redis.js';

const VIEWSHED_JOB_QUEUE = 'meshcore:viewshed_jobs';
const VIEWSHED_PENDING_SET = 'meshcore:viewshed_pending';
const VIEWSHED_COOLDOWN_PREFIX = 'meshcore:viewshed_cooldown:';
const PLANNED_VIEWSHED_JOB_QUEUE = 'meshcore:planned_viewshed_jobs:v1';
const LINK_JOB_QUEUE = 'meshcore:link_jobs:v3';
const LINK_JOB_PAYLOADS = 'meshcore:link_jobs:v3:payloads';
const LINK_JOB_DELTAS = 'meshcore:link_jobs:v3:deltas';
const LINK_JOB_STATES = 'meshcore:link_jobs:v3:states';
const LINK_JOB_OUTSTANDING = 'meshcore:link_jobs:v3:outstanding';
const LINK_JOB_BYTES = 'meshcore:link_jobs:v3:bytes';
const LINK_JOB_RETRIES = 'meshcore:link_jobs:v3:retries';
const LINK_JOB_QUEUE_MAX = Math.min(100_000, Math.max(100, Number(process.env['LINK_JOB_QUEUE_MAX'] ?? 5_000) || 5_000));
const LINK_JOB_MAX_BYTES = Math.min(262_144, Math.max(1_024, Number(process.env['LINK_JOB_MAX_BYTES'] ?? 16_384) || 16_384));
const LINK_JOB_QUEUE_MAX_BYTES = Math.min(
  512 * 1024 * 1024,
  Math.max(1 * 1024 * 1024, Number(process.env['LINK_JOB_QUEUE_MAX_BYTES'] ?? 64 * 1024 * 1024) || 64 * 1024 * 1024),
);
const LINK_JOB_MAX_DELTA = Math.min(
  1_000_000,
  Math.max(1, Number(process.env['LINK_JOB_MAX_DELTA'] ?? 10_000) || 10_000),
);
const LINK_ADMISSION_SCRIPT = `
local existing = redis.call('HGET', KEYS[2], ARGV[1])
if existing then
  local delta = tonumber(redis.call('HGET', KEYS[3], ARGV[1]) or '0')
  local increment = tonumber(ARGV[4])
  local nextDelta = math.min(tonumber(ARGV[7]), delta + increment)
  redis.call('HSET', KEYS[3], ARGV[1], nextDelta)
  return 2
end
local outstanding = redis.call('ZCARD', KEYS[5])
local allocatedBytes = tonumber(redis.call('GET', KEYS[6]) or '0')
if outstanding >= tonumber(ARGV[5]) then return 0 end
if redis.call('LLEN', KEYS[1]) >= tonumber(ARGV[5]) then return 0 end
if allocatedBytes + tonumber(ARGV[3]) > tonumber(ARGV[6]) then return 0 end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[3], ARGV[1], 1)
redis.call('HSET', KEYS[4], ARGV[1], 'queued')
redis.call('HSET', KEYS[7], ARGV[1], 0)
redis.call('ZADD', KEYS[5], tonumber(ARGV[8]), ARGV[1])
redis.call('INCRBY', KEYS[6], tonumber(ARGV[3]))
redis.call('LPUSH', KEYS[1], ARGV[1])
return 1
`;
const PLANNED_OUTSTANDING = 'meshcore:planned_viewshed:outstanding:v1';
const PLANNED_FINGERPRINT_PREFIX = 'meshcore:planned_viewshed:fingerprint:v1:';
const PLANNED_META_PREFIX = 'meshcore:planned_viewshed:meta:v1:';
const PLANNED_HANDLE_PREFIX = 'meshcore:planned_viewshed:handle:v1:';
const PLANNED_WORKER_HEARTBEAT = 'meshcore:planned_viewshed:worker_heartbeat:v1';
const PLANNED_QUEUE_MAX = Math.min(10_000, Math.max(1, Number(process.env['PLANNED_COVERAGE_QUEUE_MAX'] ?? 128) || 128));
const PLANNED_RESULT_TTL_MS = Math.min(
  24 * 60 * 60_000,
  Math.max(
    60_000,
    (Number(process.env['PLANNED_COVERAGE_RESULT_TTL_SECONDS'] ?? 3_600) || 3_600) * 1_000,
  ),
);
const PLANNED_ADMISSION_SCRIPT = `
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[1])
if not redis.call('GET', KEYS[4]) then return {'unavailable', ''} end
if redis.call('EXISTS', KEYS[6]) == 1 then return {'collision', ''} end
local existing = redis.call('GET', KEYS[3])
if existing then
  local rawMeta = redis.call('GET', ARGV[8] .. existing)
  if rawMeta then
    local ok, meta = pcall(cjson.decode, rawMeta)
    if ok and (meta['state'] == 'queued' or meta['state'] == 'leased') then
      redis.call('SET', KEYS[6], existing, 'PX', ARGV[4])
      return {'reused', ARGV[7]}
    end
  end
  -- Completed/failed jobs are never shared with a new caller. This keeps the
  -- fingerprint from becoming a public history/timing oracle.
  redis.call('DEL', KEYS[3])
end
if redis.call('ZCARD', KEYS[2]) >= tonumber(ARGV[5]) then return {'full', ''} end
if redis.call('LLEN', KEYS[1]) >= tonumber(ARGV[5]) then return {'full', ''} end
if redis.call('EXISTS', KEYS[5]) == 1 then return {'collision', ''} end
redis.call('SET', KEYS[3], ARGV[2], 'PX', ARGV[4])
redis.call('ZADD', KEYS[2], tonumber(ARGV[1]) + tonumber(ARGV[4]), ARGV[2])
redis.call('SET', KEYS[5], ARGV[6], 'PX', ARGV[4])
redis.call('SET', KEYS[6], ARGV[2], 'PX', ARGV[4])
redis.call('LPUSH', KEYS[1], ARGV[3])
return {'queued', ARGV[7]}
`;
const VIEWSHED_JOB_COOLDOWN_SECONDS = Math.max(
  30,
  Math.min(3_600, Number(process.env['VIEWSHED_JOB_COOLDOWN_SECONDS'] ?? 300) || 300),
);

const UK_LAT_MIN = 49.5;
const UK_LAT_MAX = 61.5;
const UK_LON_MIN = -8.5;
const UK_LON_MAX = 2.5;

let pub: Redis | null = null;

export type LinkAdmission = 'queued' | 'coalesced' | 'full' | 'invalid';
export type PlannedAdmission =
  | { status: 'queued'; planId: string }
  | { status: 'full' | 'unavailable'; retryAfterSeconds: number };
export type PlannedCoverageState = 'queued' | 'leased' | 'ready' | 'failed' | 'expired';

function canonicalNodeId(value: string | undefined): string {
  const normalized = String(value ?? '').trim().toLowerCase();
  return /^[0-9a-f]{64}$/.test(normalized) ? normalized : '';
}

export function buildLinkJobKey(job: Record<string, unknown>): string {
  const type = String(job['type'] ?? '');
  let canonical: string;
  if (type === 'physical_pair') {
    const [aId, bId] = [
      String(job['node_a_id'] ?? ''),
      String(job['node_b_id'] ?? ''),
    ].sort();
    canonical = [
        'physical_pair',
        aId,
        bId,
      ].join('|');
  } else {
    canonical = [
        'observe',
        String(job['rx_node_id'] ?? ''),
        String(job['src_node_id'] ?? ''),
        String(job['path_hash_size_bytes'] ?? ''),
        String(job['hop_count'] ?? ''),
        Array.isArray(job['path_hashes']) ? job['path_hashes'].join(',') : '',
      ].join('|');
  }
  return createHash('sha256').update(canonical).digest('hex');
}

async function enqueueBoundedLinkJob(payload: Record<string, unknown>): Promise<LinkAdmission> {
  const serialized = JSON.stringify(payload);
  const payloadBytes = Buffer.byteLength(serialized);
  if (payloadBytes > LINK_JOB_MAX_BYTES) return 'invalid';
  const logicalKey = buildLinkJobKey(payload);
  const increment = payload['type'] === 'observe' ? 1 : 0;
  const result = Number(await getPublisher().eval(
    LINK_ADMISSION_SCRIPT,
    7,
    LINK_JOB_QUEUE,
    LINK_JOB_PAYLOADS,
    LINK_JOB_DELTAS,
    LINK_JOB_STATES,
    LINK_JOB_OUTSTANDING,
    LINK_JOB_BYTES,
    LINK_JOB_RETRIES,
    logicalKey,
    serialized,
    payloadBytes,
    increment,
    LINK_JOB_QUEUE_MAX,
    LINK_JOB_QUEUE_MAX_BYTES,
    LINK_JOB_MAX_DELTA,
    Date.now(),
  ));
  return result === 1 ? 'queued' : result === 2 ? 'coalesced' : 'full';
}

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

export async function queuePlannedViewshedJob(lat: number, lon: number): Promise<PlannedAdmission> {
  if (!isViewshedFeatureEnabled() || !isViewshedEligibleCoordinate(lat, lon)) {
    return { status: 'unavailable', retryAfterSeconds: 30 };
  }
  const normalizedLat = Math.round(lat * 10_000) / 10_000;
  const normalizedLon = Math.round(lon * 10_000) / 10_000;
  const fingerprint = createHash('sha256')
    .update(`${normalizedLat}|${normalizedLon}|${process.env['COVERAGE_MODEL_VERSION'] ?? '5'}|v2`)
    .digest('hex');
  try {
    for (let attempt = 0; attempt < 4; attempt += 1) {
      const jobId = `plan_${randomBytes(8).toString('hex')}`;
      const planId = `plan_${randomBytes(8).toString('hex')}`;
      const payload = JSON.stringify({
        node_id: jobId,
        lat: normalizedLat,
        lon: normalizedLon,
        fingerprint,
      });
      const metadata = JSON.stringify({
        planId: jobId,
        fingerprint,
        state: 'queued',
        createdAt: new Date().toISOString(),
      });
      const raw = await getPublisher().eval(
        PLANNED_ADMISSION_SCRIPT,
        6,
        PLANNED_VIEWSHED_JOB_QUEUE,
        PLANNED_OUTSTANDING,
        `${PLANNED_FINGERPRINT_PREFIX}${fingerprint}`,
        PLANNED_WORKER_HEARTBEAT,
        `${PLANNED_META_PREFIX}${jobId}`,
        `${PLANNED_HANDLE_PREFIX}${planId}`,
        Date.now(),
        jobId,
        payload,
        PLANNED_RESULT_TTL_MS,
        PLANNED_QUEUE_MAX,
        metadata,
        planId,
        PLANNED_META_PREFIX,
      ) as [string, string];
      if (raw[0] === 'queued' || raw[0] === 'reused') {
        // Deduplication is an internal compute optimization. Every caller gets
        // a separate opaque handle and cannot infer or delete another caller's
        // shared job.
        return { status: 'queued', planId: raw[1] };
      }
      if (raw[0] === 'collision') continue;
      return {
        status: raw[0] === 'unavailable' ? 'unavailable' : 'full',
        retryAfterSeconds: 30,
      };
    }
    return { status: 'unavailable', retryAfterSeconds: 30 };
  } catch {
    return { status: 'unavailable', retryAfterSeconds: 30 };
  }
}

export async function getPlannedCoverageState(planId: string): Promise<PlannedCoverageState> {
  try {
    const jobId = await resolvePlannedCoverageHandle(planId);
    if (!jobId) return 'expired';
    const raw = await getPublisher().get(`${PLANNED_META_PREFIX}${jobId}`);
    if (!raw) return 'expired';
    const parsed = JSON.parse(raw) as { state?: unknown };
    const state = parsed.state;
    return state === 'queued' || state === 'leased' || state === 'ready' || state === 'failed'
      ? state
      : 'expired';
  } catch {
    return 'expired';
  }
}

export async function resolvePlannedCoverageHandle(planId: string): Promise<string | null> {
  try {
    const jobId = await getPublisher().get(`${PLANNED_HANDLE_PREFIX}${planId}`);
    return jobId && /^plan_[0-9a-f]{16}$/.test(jobId) ? jobId : null;
  } catch {
    return null;
  }
}

export async function releasePlannedCoverage(planId: string): Promise<void> {
  // Public plan IDs are per-request handles.  Releasing one must not cancel or
  // delete the shared internal computation used by other callers.
  await getPublisher().del(`${PLANNED_HANDLE_PREFIX}${planId}`);
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
  const rxId = canonicalNodeId(rxNodeId);
  const srcId = canonicalNodeId(srcNodeId) || undefined;
  const normalizedHashes = pathHashes
    .slice(0, 64)
    .map((hash) => String(hash).trim().toUpperCase())
    .filter((hash) => /^[0-9A-F]{4,6}$/.test(hash));
  if (!rxId || normalizedHashes.length === 0) return;
  void enqueueBoundedLinkJob({
    type: 'observe',
    rx_node_id: rxId,
    src_node_id: srcId,
    path_hashes: normalizedHashes,
    hop_count: Number.isFinite(hopCount) ? Math.max(0, Math.min(64, Math.trunc(hopCount!))) : undefined,
    path_hash_size_bytes: pathHashSizeBytes,
  }).catch((e: Error) => console.error('[redis/queue-pub] link admission error', e.message));
}

/** Push a physical pair evaluation job for two positioned repeater nodes. */
export function queuePhysicalLinkJob(nodeAId: string, nodeBId: string): void {
  const normalizedA = canonicalNodeId(nodeAId);
  const normalizedB = canonicalNodeId(nodeBId);
  if (!normalizedA || !normalizedB || normalizedA === normalizedB) return;
  const [aId, bId] = normalizedA < normalizedB ? [normalizedA, normalizedB] : [normalizedB, normalizedA];
  void enqueueBoundedLinkJob({
    type: 'physical_pair',
    node_a_id: aId,
    node_b_id: bId,
  }).catch((e: Error) => console.error('[redis/queue-pub] physical-link admission error', e.message));
}
