import { createHash, randomUUID } from 'node:crypto';
import type { Redis } from 'ioredis';

export const LINK_V3_KEYS = {
  ready: 'meshcore:link:v3:ready',
  deferred: 'meshcore:link:v3:deferred',
  payloads: 'meshcore:link:v3:payloads',
  states: 'meshcore:link:v3:states',
  attempts: 'meshcore:link:v3:attempts',
  bytes: 'meshcore:link:v3:bytes',
  dedupe: 'meshcore:link:v3:dedupe',
  dedupeByJob: 'meshcore:link:v3:dedupe_by_job',
  leases: 'meshcore:link:v3:leases',
  tokens: 'meshcore:link:v3:tokens',
  dead: 'meshcore:link:v3:dead',
  completed: 'meshcore:link:v3:completed',
  counters: 'meshcore:link:v3:counters',
  rebuild: 'meshcore:link:v3:rebuild',
  workerHeartbeat: 'meshcore:link:v3:worker_heartbeat',
  events: 'meshcore:link:v3:events',
} as const;

export type LinkQueueAdmission =
  | { status: 'accepted'; jobId: string }
  | { status: 'coalesced' | 'duplicate'; jobId: string }
  | { status: 'full' | 'oversized' | 'worker_unavailable'; jobId: null };

export type LinkJobPayload =
  | {
      version: 3;
      type: 'observe';
      job_id: string;
      dedupe_key: string;
      logical_job_id: string;
      packet_hash: string;
      rx_node_id: string;
      src_node_id?: string;
      path_hashes: string[];
      hop_count?: number;
      path_hash_size_bytes?: number;
      generation?: string;
    }
  | {
      version: 3;
      type: 'physical_pair';
      job_id: string;
      dedupe_key: string;
      node_a_id: string;
      node_b_id: string;
      generation?: string;
    };

export const LINK_V3_ADMIT_SCRIPT = `
local existing = redis.call('HGET', KEYS[6], ARGV[2])
if existing then
  local existing_state = redis.call('HGET', KEYS[3], existing)
  if existing == ARGV[1] and existing_state == 'complete' then
    return {'duplicate', existing}
  end
  if existing_state == 'queued' or existing_state == 'in_flight' or existing_state == 'dead' then
    return {'coalesced', existing}
  end
end
local payload_bytes = tonumber(ARGV[4])
if payload_bytes > tonumber(ARGV[7]) then
  return {'oversized', ''}
end
local count = tonumber(redis.call('HGET', KEYS[10], 'count') or '0')
local bytes = tonumber(redis.call('HGET', KEYS[10], 'bytes') or '0')
if count + 1 > tonumber(ARGV[5]) or bytes + payload_bytes > tonumber(ARGV[6]) then
  return {'full', ''}
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
redis.call('HSET', KEYS[3], ARGV[1], 'queued')
redis.call('HSET', KEYS[4], ARGV[1], '0')
redis.call('HSET', KEYS[5], ARGV[1], tostring(payload_bytes))
redis.call('HSET', KEYS[6], ARGV[2], ARGV[1])
redis.call('HSET', KEYS[7], ARGV[1], ARGV[2])
redis.call('HINCRBY', KEYS[10], 'count', 1)
redis.call('HINCRBY', KEYS[10], 'bytes', payload_bytes)
if ARGV[8] == '' and redis.call('EXISTS', KEYS[9]) == 1 then
  redis.call('LPUSH', KEYS[8], ARGV[1])
else
  redis.call('LPUSH', KEYS[1], ARGV[1])
end
redis.call('LPUSH', KEYS[11], 'admit')
redis.call('LTRIM', KEYS[11], 0, 255)
return {'accepted', ARGV[1]}
`;

const ADMIT_KEYS = [
  LINK_V3_KEYS.ready,
  LINK_V3_KEYS.payloads,
  LINK_V3_KEYS.states,
  LINK_V3_KEYS.attempts,
  LINK_V3_KEYS.bytes,
  LINK_V3_KEYS.dedupe,
  LINK_V3_KEYS.dedupeByJob,
  LINK_V3_KEYS.deferred,
  LINK_V3_KEYS.rebuild,
  LINK_V3_KEYS.counters,
  LINK_V3_KEYS.events,
];

function positiveInt(value: string | undefined, fallback: number, max: number): number {
  const parsed = Number(value ?? fallback);
  return Math.min(max, Math.max(1, Number.isFinite(parsed) ? Math.floor(parsed) : fallback));
}

export const linkQueueLimits = {
  maxJobs: positiveInt(process.env['LINK_QUEUE_V3_MAX_JOBS'], 5_000, 100_000),
  maxBytes: positiveInt(process.env['LINK_QUEUE_V3_MAX_BYTES'], 64 * 1024 * 1024, 1024 * 1024 * 1024),
  maxPayloadBytes: positiveInt(process.env['LINK_QUEUE_V3_MAX_PAYLOAD_BYTES'], 32 * 1024, 1024 * 1024),
};

export function linkObservationIdentity(packetHash: string, rxNodeId: string, generation?: string): {
  jobId: string;
  dedupeKey: string;
} {
  const digest = createHash('sha256')
    .update(`observe\0${generation ?? 'live'}\0${packetHash.toLowerCase()}\0${rxNodeId.toLowerCase()}`)
    .digest('hex');
  return { jobId: `lo_${digest}`, dedupeKey: `observe:${generation ?? 'live'}:${digest}` };
}

export function linkPhysicalIdentity(nodeAId: string, nodeBId: string, generation?: string): {
  jobId: string;
  dedupeKey: string;
  nodeAId: string;
  nodeBId: string;
} {
  const [a, b] = nodeAId < nodeBId ? [nodeAId, nodeBId] : [nodeBId, nodeAId];
  return {
    jobId: `lp_${randomUUID()}`,
    dedupeKey: `physical:${generation ?? 'live'}:${a}:${b}`,
    nodeAId: a,
    nodeBId: b,
  };
}

export async function admitLinkV3Job(
  redis: Redis,
  payload: LinkJobPayload,
  options: { requireHealthyWorker?: boolean } = {},
): Promise<LinkQueueAdmission> {
  if (options.requireHealthyWorker !== false) {
    const healthy = await redis.exists(LINK_V3_KEYS.workerHeartbeat);
    if (healthy !== 1) return { status: 'worker_unavailable', jobId: null };
  }
  const serialized = JSON.stringify(payload);
  const payloadBytes = Buffer.byteLength(serialized);
  const response = await redis.eval(
    LINK_V3_ADMIT_SCRIPT,
    ADMIT_KEYS.length,
    ...ADMIT_KEYS,
    payload.job_id,
    payload.dedupe_key,
    serialized,
    String(payloadBytes),
    String(linkQueueLimits.maxJobs),
    String(linkQueueLimits.maxBytes),
    String(linkQueueLimits.maxPayloadBytes),
    payload.generation ?? '',
  ) as [LinkQueueAdmission['status'], string];
  const [status, jobId] = response;
  if (status === 'accepted' || status === 'coalesced' || status === 'duplicate') {
    return { status, jobId };
  }
  return { status, jobId: null };
}

const RELEASE_DEFERRED_SCRIPT = `
local current_owner = redis.call('GET', KEYS[4])
if ARGV[2] ~= '' then
  if current_owner ~= ARGV[2] then return -1 end
elseif current_owner then
  return -1
end
local released = 0
while released < tonumber(ARGV[1]) do
  local job_id = redis.call('RPOP', KEYS[1])
  if not job_id then break end
  if redis.call('HGET', KEYS[2], job_id) == 'queued' then
    redis.call('LPUSH', KEYS[3], job_id)
    released = released + 1
  end
end
if released > 0 then
  redis.call('LPUSH', KEYS[5], 'release')
  redis.call('LTRIM', KEYS[5], 0, 255)
end
if redis.call('LLEN', KEYS[1]) == 0 and ARGV[2] ~= '' then
  if redis.call('GET', KEYS[4]) == ARGV[2] then redis.call('DEL', KEYS[4]) end
end
return released
`;

export const RENEW_LINK_REBUILD_LEASE_SCRIPT = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
`;

export async function renewLinkRebuildLease(
  redis: Redis,
  ownerToken: string,
  ttlMs: number,
): Promise<boolean> {
  return Number(await redis.eval(
    RENEW_LINK_REBUILD_LEASE_SCRIPT,
    1,
    LINK_V3_KEYS.rebuild,
    ownerToken,
    ttlMs,
  )) === 1;
}

export async function releaseDeferredLinkJobs(
  redis: Redis,
  batchSize = 1_000,
  ownerToken?: string,
): Promise<number> {
  let total = 0;
  while (true) {
    const released = Number(await redis.eval(
      RELEASE_DEFERRED_SCRIPT,
      5,
      LINK_V3_KEYS.deferred,
      LINK_V3_KEYS.states,
      LINK_V3_KEYS.ready,
      LINK_V3_KEYS.rebuild,
      LINK_V3_KEYS.events,
      batchSize,
      ownerToken ?? '',
    ));
    if (released === -1) {
      if (ownerToken) throw new Error('LINK_REBUILD_LEASE_LOST');
      return total;
    }
    total += released;
    if (released < batchSize) return total;
  }
}

type ModelJob = {
  bytes: number;
  state: 'queued' | 'in_flight' | 'complete' | 'dead';
  attempts: number;
  token?: string;
  leaseUntil?: number;
  dedupeKey: string;
};

/** Deterministic protocol model used to exhaustively exercise transition rules. */
export class LinkQueueV3Model {
  readonly jobs = new Map<string, ModelJob>();
  readonly dedupe = new Map<string, string>();
  readonly ready: string[] = [];
  readonly deferred: string[] = [];
  readonly dead: string[] = [];
  count = 0;
  bytes = 0;
  rebuildActive = false;

  constructor(readonly maxJobs: number, readonly maxBytes: number, readonly maxAttempts = 3) {}

  admit(jobId: string, dedupeKey: string, bytes: number, generation = false): LinkQueueAdmission['status'] {
    const existingId = this.dedupe.get(dedupeKey);
    const existing = existingId ? this.jobs.get(existingId) : undefined;
    if (existingId === jobId && existing?.state === 'complete') return 'duplicate';
    if (existing && ['queued', 'in_flight', 'dead'].includes(existing.state)) return 'coalesced';
    if (this.count + 1 > this.maxJobs || this.bytes + bytes > this.maxBytes) return 'full';
    this.jobs.set(jobId, { bytes, state: 'queued', attempts: 0, dedupeKey });
    this.dedupe.set(dedupeKey, jobId);
    this.count += 1;
    this.bytes += bytes;
    (this.rebuildActive && !generation ? this.deferred : this.ready).unshift(jobId);
    return 'accepted';
  }

  claim(token: string, now: number, leaseMs: number): string | null {
    while (this.ready.length > 0) {
      const jobId = this.ready.pop()!;
      const job = this.jobs.get(jobId);
      if (!job || job.state !== 'queued') continue;
      job.state = 'in_flight';
      job.attempts += 1;
      job.token = token;
      job.leaseUntil = now + leaseMs;
      return jobId;
    }
    return null;
  }

  ack(jobId: string, token: string): boolean {
    const job = this.jobs.get(jobId);
    if (!job || job.state !== 'in_flight' || job.token !== token) return false;
    job.state = 'complete';
    delete job.token;
    delete job.leaseUntil;
    this.count -= 1;
    this.bytes -= job.bytes;
    return true;
  }

  nack(jobId: string, token: string): 'retry' | 'dead' | 'invalid' {
    const job = this.jobs.get(jobId);
    if (!job || job.state !== 'in_flight' || job.token !== token) return 'invalid';
    delete job.token;
    delete job.leaseUntil;
    if (job.attempts >= this.maxAttempts) {
      job.state = 'dead';
      this.dead.unshift(jobId);
      return 'dead';
    }
    job.state = 'queued';
    this.ready.unshift(jobId);
    return 'retry';
  }

  reap(now: number): number {
    let requeued = 0;
    for (const [jobId, job] of this.jobs) {
      if (job.state !== 'in_flight' || (job.leaseUntil ?? Infinity) > now) continue;
      job.state = 'queued';
      delete job.token;
      delete job.leaseUntil;
      this.ready.unshift(jobId);
      requeued += 1;
    }
    return requeued;
  }

  releaseDeferred(): number {
    let released = 0;
    while (this.deferred.length > 0) {
      const jobId = this.deferred.pop()!;
      if (this.jobs.get(jobId)?.state === 'queued') {
        this.ready.unshift(jobId);
        released += 1;
      }
    }
    this.rebuildActive = false;
    return released;
  }
}
