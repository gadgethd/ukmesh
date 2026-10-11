/**
 * RAM-first MQTT ingestion — admission store.
 *
 * admitPacketToRam() is the RAM-mode equivalent of insertPacket(): it runs the
 * same input transformation (buildPacketBatchInput) and privacy
 * classification, then admits the packet to a bounded Redis Stream. The
 * broker acknowledgement is released only after a successful XADD, so a
 * refused admission (queue full / bytes bound / Redis unavailable) results in
 * redelivery, never in an acknowledged-but-lost packet.
 *
 * Bounds (strict, admission-side — entries are never silently trimmed):
 *  - packet count and byte budget are enforced ATOMICALLY by a Lua script:
 *    the XLEN check, the running byte counter (a companion key maintained at
 *    admission and release) and the XADD all run server-side in one step, so
 *    concurrent MQTT connections cannot race past the same capacity check.
 *  - byte accounting is conservative (raw value + fixed per-entry overhead)
 *    and reconciled against the real stream on writer startup and on a slow
 *    cadence, keeping the counter honest across crashes and failed cleanups.
 */
import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { getRedisConnectionOptions, getRedisUrl } from '../platform/config/redis.js';
import { buildPacketBatchInput, type InsertPacketInput } from '../db/packetInput.js';
import { classifyForAdmission } from '../db/packetBatch.js';
import {
  decodeRamStreamValue,
  accountedRamEntryBytes,
  encodePendingEntry,
  entryFieldName,
  getRamAdmissionKey,
  getRamCommittedKey,
  getRamConsumerGroup,
  getRamIngestMode,
  getRamQueueBytesKey,
  getRamQueueMaxBytes,
  getRamQueueMaxPackets,
  getRamStreamKey,
  RAM_ENTRY_OVERHEAD_BYTES,
  RAM_REDIS_HEADROOM_BYTES,
  type RamPendingEntry,
} from './protocol.js';
import {
  ramAdmissionDuration,
  ramAdmissionsTotal,
  ramEarliestPendingAge,
  ramPendingDepth,
  ramRetainedDepth,
  ramPelDepth,
  ramOldestPelAge,
  ramQueueBytes,
  ramQueueReservedBytes,
  ramQueueDepth,
} from '../metrics.js';

/** Minimal surface this module needs; keeps unit tests free of a live Redis. */
export interface RamStoreRedis {
  xlen(key: string): Promise<number>;
  xadd(key: string, id: string, field: string, value: string): Promise<string>;
  xrevrange(
    key: string,
    max: string,
    min: string,
    ...args: (string | number)[]
  ): Promise<Array<[string, string[]]>>;
  xrange(
    key: string,
    min: string,
    max: string,
    ...args: (string | number)[]
  ): Promise<Array<[string, string[]]>>;
  memory(subcommand: string, key: string): Promise<number | null>;
  get(key: string): Promise<string | null>;
  eval(script: string, numKeys: number, ...args: (string | number)[]): Promise<unknown>;
  hmget(key: string, ...fields: string[]): Promise<Array<string | null>>;
  xpending(key: string, group: string): Promise<unknown>;
}

/**
 * Atomic admission: check count, check bytes, add, account — one server-side
 * step. A retransmitted delivery returns the original encoded entry before
 * capacity checks. The ledger key is removed only with final stream cleanup.
 */
const ADMIT_SCRIPT = `
if tonumber(ARGV[6]) == 1 then
  local existing = redis.call('GET', KEYS[3])
  if existing then return {1, existing} end
end
local len = redis.call('XLEN', KEYS[1])
if len >= tonumber(ARGV[1]) then return -1 end
local recorded = redis.call('GET', KEYS[2])
if (len > 0 and not recorded) or (recorded and (not tonumber(recorded) or tonumber(recorded) < 0)) then return -3 end
local used = tonumber(recorded or '0')
local add = tonumber(ARGV[3])
if used + add > tonumber(ARGV[2]) then return -2 end
local memory = redis.call('INFO', 'memory')
local maxmemory = tonumber(string.match(memory, 'maxmemory:(%d+)')) or 0
local resident = tonumber(string.match(memory, 'used_memory:(%d+)'))
local policy = string.match(memory, 'maxmemory_policy:([^%s]+)')
local headroom = tonumber(ARGV[7])
if not resident or maxmemory == 0 or policy ~= 'noeviction' or tonumber(ARGV[2]) + headroom > maxmemory
  or resident + add + headroom >= maxmemory then return -4 end
local id = redis.call('XADD', KEYS[1], '*', ARGV[4], ARGV[5])
redis.call('INCRBY', KEYS[2], add)
redis.call('SET', KEYS[3], ARGV[5])
return {0, ARGV[5]}
`;

// The stream has a strict admission count bound. Recomputing the companion
// counter in one Redis command cannot race XADD or final cleanup; both are
// Lua commands on the same server. Refuse an unexpectedly oversized stream.
const RECONCILE_BYTES_SCRIPT = `
local limit = tonumber(ARGV[1])
local rows = redis.call('XRANGE', KEYS[1], '-', '+', 'COUNT', limit + 1)
if #rows > limit then return -1 end
local total = 0
for _, row in ipairs(rows) do
  local fields = row[2]
  local value = fields[2] or ''
  total = total + string.len(value) * 2 + tonumber(ARGV[2])
end
redis.call('SET', KEYS[2], total)
return total
`;

export async function reconcileRamQueueBytes(): Promise<number> {
  const result = await getClient().eval(
    RECONCILE_BYTES_SCRIPT,
    2,
    getRamStreamKey(),
    getRamQueueBytesKey(),
    getRamQueueMaxPackets(),
    RAM_ENTRY_OVERHEAD_BYTES,
  );
  if (typeof result !== 'number' || result < 0) {
    throw new Error('RAM_QUEUE_BYTES_RECONCILIATION_REFUSED');
  }
  return result;
}

export type RamDelivery = { key: string; redelivered: boolean };

export type RamAdmissionResult = {
  observationId: string;
  isPrivate: boolean;
  visibilityOk: boolean;
};

export class RamAdmissionRefusedError extends Error {
  readonly reason: 'queue_full' | 'queue_bytes' | 'redis_unavailable' | 'redis_headroom';

  constructor(reason: RamAdmissionRefusedError['reason'], cause?: unknown) {
    super(`RAM_INGEST_REFUSED:${reason}${cause ? `:${(cause as Error).message ?? String(cause)}` : ''}`);
    this.name = 'RamAdmissionRefusedError';
    this.reason = reason;
  }
}

let redis: RamStoreRedis | null = null;
let redisOwned = false;
let cachedBytes = 0;

export function configureRamStore(client: RamStoreRedis | null): void {
  redis = client;
  redisOwned = false;
  cachedBytes = 0;
}

export function getRamRedis(): RamStoreRedis | null {
  return redis;
}

function getClient(): RamStoreRedis {
  if (redis) return redis;
  const owned = new Redis(getRedisUrl(), {
    ...getRedisConnectionOptions(),
    maxRetriesPerRequest: 2,
    enableOfflineQueue: false,
    commandTimeout: 30_000,
  });
  owned.on('error', (error: Error) => {
    console.error('[ram] redis error', error.message);
  });
  redis = owned as unknown as RamStoreRedis;
  redisOwned = true;
  return redis;
}

export async function closeRamStore(): Promise<void> {
  if (redis && redisOwned) {
    try {
      await (redis as unknown as Redis).quit();
    } catch {
      /* best-effort */
    }
  }
  redis = null;
  redisOwned = false;
}

/**
 * Age (seconds) of the OLDEST genuinely pending observation given a
 * oldest-first entry list. Markers and undecodable records are skipped;
 * committed entries are excluded by the writer's retention trim, so the first
 * decodable entry is the oldest uncommitted observation. Pure for testing.
 */
export function oldestPendingAgeSeconds(
  entries: Array<[string, string[]]>,
  nowMs: number,
  committedIds: ReadonlySet<string> = new Set(),
): number | null {
  let oldestAt = Number.POSITIVE_INFINITY;
  for (const [id, fields] of entries) {
    if (committedIds.has(id)) continue;
    const decoded = decodeRamStreamValue(fields[1]);
    if (!decoded || decoded.kind !== 'entry') continue;
    oldestAt = Math.min(oldestAt, decoded.entry.admittedAtMs);
  }
  return Number.isFinite(oldestAt) ? Math.max(0, (nowMs - oldestAt) / 1000) : null;
}

/** Refresh queue gauges (count, bytes, oldest pending age). Called by the writer tick. */
export async function refreshPendingStats(): Promise<{ pending: number; pel: number; oldestPelAgeSeconds: number } | null> {
  if (getRamIngestMode() !== 'ram') return null;
  const client = getClient();
  const key = getRamStreamKey();
  // Oldest-first sample: XREVRANGE reported the NEWEST entry's age, which is
  // always ~0 in a busy queue and hid stuck deliveries. XRANGE returns the
  // oldest entries, where the genuinely oldest pending observation lives.
  const [len, mem, reserved, oldestFirst, pel] = await Promise.all([
    client.xlen(key),
    client.memory('USAGE', key).catch(() => cachedBytes),
    client.get(getRamQueueBytesKey()),
    client.xrange(key, '-', '+', 'COUNT', getRamQueueMaxPackets()),
    client.xpending(key, getRamConsumerGroup()).catch((error: unknown) => {
      if ((error as Error).message?.includes('NOGROUP')) return [0, null, null, []];
      throw error;
    }),
  ]);
  cachedBytes = mem === null ? 0 : typeof mem === 'number' && Number.isFinite(mem) ? mem : cachedBytes;
  ramQueueDepth.set(len);
  ramQueueBytes.set(cachedBytes);
  ramQueueReservedBytes.set(Math.max(0, Number(reserved ?? 0) || 0));
  const entryIds = oldestFirst
    .filter(([, fields]) => decodeRamStreamValue(fields[1])?.kind === 'entry')
    .map(([id]) => id);
  const committedValues = entryIds.length
    ? await client.hmget(getRamCommittedKey(), ...entryIds)
    : [];
  const committedIds = new Set(entryIds.filter((_, index) => committedValues[index] !== null));
  const pendingCount = entryIds.length - committedIds.size;
  ramPendingDepth.set(pendingCount);
  ramRetainedDepth.set(committedIds.size);
  ramEarliestPendingAge.set(oldestPendingAgeSeconds(oldestFirst, Date.now(), committedIds) ?? 0);
  let pelCount = 0;
  let oldestPelAgeSeconds = 0;
  if (Array.isArray(pel)) {
    const count = Number(pel[0]);
    pelCount = Number.isFinite(count) ? count : 0;
    ramPelDepth.set(pelCount);
    const oldestId = typeof pel[1] === 'string' ? pel[1] : null;
    const oldestMs = oldestId ? Number(oldestId.split('-')[0]) : NaN;
    oldestPelAgeSeconds = Number.isFinite(oldestMs) ? Math.max(0, (Date.now() - oldestMs) / 1000) : 0;
    ramOldestPelAge.set(oldestPelAgeSeconds);
  }
  return { pending: pendingCount, pel: pelCount, oldestPelAgeSeconds };
}

/**
 * Admit one packet to the RAM stream. Mirrors insertPacket's input shape so
 * the client ingest path can swap implementations on the mode flag alone.
 * Throws RamAdmissionRefusedError when the packet cannot be admitted — the
 * caller must NOT acknowledge the broker in that case.
 */
export async function admitPacketToRam(
  p: InsertPacketInput,
  delivery?: RamDelivery,
): Promise<RamAdmissionResult> {
  const startedNs = process.hrtime.bigint();
  const input = buildPacketBatchInput(p);

  // Admission-time classification: the same cache the durable batcher uses;
  // the database re-classifies authoritatively at commit (fail-closed fence).
  const classification = await classifyForAdmission({
    network: input.network,
    rxNodeId: input.rxNodeId,
    srcNodeId: input.srcNodeId,
    pathHashes: input.pathHashes,
    pathHashSizeBytes: input.pathHashSizeBytes,
  });
  const isPrivate = classification.generation < 1 ? true : classification.isPrivate;
  const visibilityOk = classification.generation < 1 ? false : classification.visibilityOk;

  const client = getClient();
  const key = getRamStreamKey();
  const maxPackets = getRamQueueMaxPackets();
  const maxBytes = getRamQueueMaxBytes();

  try {
    const deliveryKey = delivery?.key ?? randomUUID();
    const entry: RamPendingEntry = {
      observationId: randomUUID(),
      deliveryKey,
      admittedAtMs: Date.now(),
      isPrivate,
      visibilityOk,
      input,
    };
    const value = encodePendingEntry(entry);
    const entryBytes = accountedRamEntryBytes(value);

    // Count check, byte check, XADD and byte accounting in one atomic
    // server-side step: concurrent connections cannot both pass the same
    // capacity check.
    const runAdmission = () => client.eval(
      ADMIT_SCRIPT, 3, key, getRamQueueBytesKey(), getRamAdmissionKey(deliveryKey),
      maxPackets, maxBytes, entryBytes, entryFieldName(), value,
      delivery?.redelivered ? 1 : 0,
      RAM_REDIS_HEADROOM_BYTES,
    );
    let result = await runAdmission();
    if (result === -3) {
      await reconcileRamQueueBytes();
      result = await runAdmission();
    }
    if (result === -1) {
      ramAdmissionsTotal.inc({ outcome: 'queue_full' });
      throw new RamAdmissionRefusedError('queue_full');
    }
    if (result === -2) {
      ramAdmissionsTotal.inc({ outcome: 'queue_bytes' });
      throw new RamAdmissionRefusedError('queue_bytes');
    }
    if (result === -4) {
      ramAdmissionsTotal.inc({ outcome: 'redis_headroom' });
      throw new RamAdmissionRefusedError('redis_headroom');
    }
    if (!Array.isArray(result) || result.length !== 2 || typeof result[1] !== 'string') {
      throw new RamAdmissionRefusedError('redis_unavailable', new Error('unexpected eval reply'));
    }
    const stored = decodeRamStreamValue(result[1]);
    if (!stored || stored.kind !== 'entry') {
      throw new RamAdmissionRefusedError('redis_unavailable', new Error('invalid stored admission'));
    }
    ramAdmissionsTotal.inc({ outcome: 'admitted' });
    ramAdmissionDuration.observe(Number(process.hrtime.bigint() - startedNs) / 1e9);
    return { observationId: stored.entry.observationId, isPrivate, visibilityOk };
  } catch (error) {
    if (error instanceof RamAdmissionRefusedError) throw error;
    ramAdmissionsTotal.inc({ outcome: 'redis_error' });
    throw new RamAdmissionRefusedError('redis_unavailable', error);
  }
}

export type PendingStreamEntry = { id: string; entry: RamPendingEntry };

/**
 * Read newest stream entries, including the retained post-commit overlap.
 * The caller deduplicates by observation ID against the same PG read snapshot.
 * A cumulative marker cannot classify entries admitted during a commit.
 */
export async function readPendingEntries(limit: number, strict = false): Promise<PendingStreamEntry[]> {
  if (getRamIngestMode() !== 'ram' || limit <= 0) return [];
  try {
    const client = getClient();
    const raw = await client.xrevrange(
      getRamStreamKey(),
      '+',
      '-',
      'COUNT',
      getRamQueueMaxPackets(),
    );
    const out: PendingStreamEntry[] = [];
    for (const [id, fields] of raw) {
      const decoded = decodeRamStreamValue(fields[1]);
      if (!decoded) continue;
      if (decoded.kind === 'commit') continue; // legacy marker, not an authority boundary
      out.push({ id, entry: decoded.entry });
    }
    return out.reverse();
  } catch (error) {
    if (strict) throw error;
    console.error('[ram] pending read failed', (error as Error).message);
    return [];
  }
}
