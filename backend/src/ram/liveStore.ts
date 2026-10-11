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
 *  - packet count: XLEN checked before every admission;
 *  - bytes: MEMORY USAGE of the stream key, refreshed by the writer tick
 *    (≤1 s cadence) and lazily on admission when the queue is filling.
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
  getRamQueueMaxBytes,
  getRamQueueMaxPackets,
  getRamStreamKey,
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
  memory(subcommand: string, key: string): Promise<number>;
}

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
let lastStatsAtMs = 0;
let cachedBytes = 0;

export function configureRamStore(client: RamStoreRedis | null): void {
  redis = client;
  redisOwned = false;
  lastStatsAtMs = 0;
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

/** Refresh queue gauges (count, bytes, oldest pending age). Called by the writer tick. */
export async function refreshPendingStats(): Promise<void> {
  if (getRamIngestMode() !== 'ram') return;
  const client = getClient();
  const key = getRamStreamKey();
  const [len, mem, recent] = await Promise.all([
    client.xlen(key),
    client.memory('USAGE', key).catch(() => cachedBytes),
    client.xrevrange(key, '+', '-', 'COUNT', 50),
  ]);
  cachedBytes = typeof mem === 'number' && Number.isFinite(mem) ? mem : cachedBytes;
  lastStatsAtMs = Date.now();
  ramQueueDepth.set(len);
  ramQueueBytes.set(cachedBytes);
  for (const [id, fields] of recent) {
    const decoded = decodeRamStreamValue(fields[1]);
    if (decoded && decoded.kind === 'entry') {
      const ms = Number(id.split('-')[0]);
      if (Number.isFinite(ms)) {
        ramEarliestPendingAge.set(Math.max(0, (Date.now() - ms) / 1000));
      }
      break;
    }
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
    const len = await client.xlen(key);
    if (len >= maxPackets) {
      ramAdmissionsTotal.inc({ outcome: 'queue_full' });
      throw new RamAdmissionRefusedError('queue_full');
    }
    // Byte bound uses the writer-maintained cache; refresh lazily when the
    // queue is filling and the cached value is stale (≤5 s behind reality).
    if (len >= maxPackets * 0.5 && Date.now() - lastStatsAtMs > 5_000) {
      await refreshPendingStats();
    }
    if (cachedBytes >= maxBytes) {
      ramAdmissionsTotal.inc({ outcome: 'queue_bytes' });
      throw new RamAdmissionRefusedError('queue_bytes');
    }

    const entry: RamPendingEntry = {
      observationId: randomUUID(),
      admittedAtMs: Date.now(),
      isPrivate,
      visibilityOk,
      input,
    };
    await client.xadd(key, '*', entryFieldName(), encodePendingEntry(entry));
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
 * returned oldest-last. Committed entries are removed from the stream at
 * commit time, so anything present here is genuinely pending; commit markers
 * are skipped. Best-effort: returns [] on any Redis failure.
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
      if (out.length >= limit) break;
      const decoded = decodeRamStreamValue(fields[1]);
      if (!decoded) continue;
      if (decoded.kind === 'commit') continue;
      out.push({ id, entry: decoded.entry });
    }
    return out.reverse();
  } catch (error) {
    console.error('[ram] pending read failed', (error as Error).message);
    return [];
  }
}
