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
  encodePendingEntry,
  entryFieldName,
  getRamIngestMode,
  getRamQueueBytesKey,
  getRamQueueMaxBytes,
  getRamQueueMaxPackets,
  getRamStreamKey,
  RAM_ENTRY_OVERHEAD_BYTES,
  type RamPendingEntry,
} from './protocol.js';
import {
  ramAdmissionDuration,
  ramAdmissionsTotal,
  ramEarliestPendingAge,
  ramQueueBytes,
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
  memory(subcommand: string, key: string): Promise<number>;
  eval(script: string, numKeys: number, ...args: (string | number)[]): Promise<unknown>;
}

/**
 * Atomic admission: check count, check bytes, add, account — one server-side
 * step. Returns the new entry id, -1 (count exhausted) or -2 (bytes budget).
 */
const ADMIT_SCRIPT = `
local len = redis.call('XLEN', KEYS[1])
if len >= tonumber(ARGV[1]) then return -1 end
local used = tonumber(redis.call('GET', KEYS[2]) or '0')
local add = tonumber(ARGV[3])
if used + add > tonumber(ARGV[2]) then return -2 end
local id = redis.call('XADD', KEYS[1], '*', ARGV[4], ARGV[5])
redis.call('INCRBY', KEYS[2], add)
return id
`;

export type RamAdmissionResult = {
  observationId: string;
  isPrivate: boolean;
  visibilityOk: boolean;
};

export class RamAdmissionRefusedError extends Error {
  readonly reason: 'queue_full' | 'queue_bytes' | 'redis_unavailable';

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
): number | null {
  for (const [id, fields] of entries) {
    const decoded = decodeRamStreamValue(fields[1]);
    if (!decoded || decoded.kind !== 'entry') continue;
    const ms = Number(id.split('-')[0]);
    if (!Number.isFinite(ms)) continue;
    return Math.max(0, (nowMs - ms) / 1000);
  }
  return null;
}

/** Refresh queue gauges (count, bytes, oldest pending age). Called by the writer tick. */
export async function refreshPendingStats(): Promise<void> {
  if (getRamIngestMode() !== 'ram') return;
  const client = getClient();
  const key = getRamStreamKey();
  // Oldest-first sample: XREVRANGE reported the NEWEST entry's age, which is
  // always ~0 in a busy queue and hid stuck deliveries. XRANGE returns the
  // oldest entries, where the genuinely oldest pending observation lives.
  const [len, mem, oldestFirst] = await Promise.all([
    client.xlen(key),
    client.memory('USAGE', key).catch(() => cachedBytes),
    client.xrange(key, '-', '+', 'COUNT', 50),
  ]);
  cachedBytes = typeof mem === 'number' && Number.isFinite(mem) ? mem : cachedBytes;
  ramQueueDepth.set(len);
  ramQueueBytes.set(cachedBytes);
  const oldestAge = oldestPendingAgeSeconds(oldestFirst, Date.now());
  if (oldestAge !== null) {
    ramEarliestPendingAge.set(oldestAge);
  }
}

/**
 * Admit one packet to the RAM stream. Mirrors insertPacket's input shape so
 * the client ingest path can swap implementations on the mode flag alone.
 * Throws RamAdmissionRefusedError when the packet cannot be admitted — the
 * caller must NOT acknowledge the broker in that case.
 */
export async function admitPacketToRam(p: InsertPacketInput): Promise<RamAdmissionResult> {
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
    const entry: RamPendingEntry = {
      observationId: randomUUID(),
      admittedAtMs: Date.now(),
      isPrivate,
      visibilityOk,
      input,
    };
    const value = encodePendingEntry(entry);
    const entryBytes = Buffer.byteLength(value) + RAM_ENTRY_OVERHEAD_BYTES;

    // Count check, byte check, XADD and byte accounting in one atomic
    // server-side step: concurrent connections cannot both pass the same
    // capacity check.
    const result = await client.eval(
      ADMIT_SCRIPT,
      2,
      key,
      getRamQueueBytesKey(),
      maxPackets,
      maxBytes,
      entryBytes,
      entryFieldName(),
      value,
    );
    if (result === -1) {
      ramAdmissionsTotal.inc({ outcome: 'queue_full' });
      throw new RamAdmissionRefusedError('queue_full');
    }
    if (result === -2) {
      ramAdmissionsTotal.inc({ outcome: 'queue_bytes' });
      throw new RamAdmissionRefusedError('queue_bytes');
    }
    if (typeof result !== 'string' || result.length === 0) {
      throw new RamAdmissionRefusedError('redis_unavailable', new Error('unexpected eval reply'));
    }
    ramAdmissionsTotal.inc({ outcome: 'admitted' });
    ramAdmissionDuration.observe(Number(process.hrtime.bigint() - startedNs) / 1e9);
    return { observationId: entry.observationId, isPrivate, visibilityOk };
  } catch (error) {
    if (error instanceof RamAdmissionRefusedError) throw error;
    ramAdmissionsTotal.inc({ outcome: 'redis_error' });
    throw new RamAdmissionRefusedError('redis_unavailable', error);
  }
}

export type PendingStreamEntry = { id: string; entry: RamPendingEntry };

/**
 * Read the newest pending (not yet committed) entries, newest first →
 * returned oldest-last. Committed entries are retained briefly in the stream
 * (handoff-overlap window), so the latest commit marker is the boundary:
 * entries at or below it are committed and excluded. Best-effort: returns []
 * on any Redis failure.
 */
export async function readPendingEntries(limit: number): Promise<PendingStreamEntry[]> {
  if (getRamIngestMode() !== 'ram') return [];
  try {
    const client = getClient();
    const raw = await client.xrevrange(
      getRamStreamKey(),
      '+',
      '-',
      'COUNT',
      Math.min(2_000, limit * 3 + 25),
    );
    const out: PendingStreamEntry[] = [];
    for (const [id, fields] of raw) {
      const decoded = decodeRamStreamValue(fields[1]);
      if (!decoded) continue;
      // Latest commit boundary: everything at or below the newest marker is
      // committed (entries are retained briefly for the overlap window) and
      // must not be reported as pending.
      if (decoded.kind === 'commit') break;
      if (out.length >= limit) break;
      out.push({ id, entry: decoded.entry });
    }
    return out.reverse();
  } catch (error) {
    console.error('[ram] pending read failed', (error as Error).message);
    return [];
  }
}
