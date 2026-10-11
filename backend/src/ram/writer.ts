/**
 * RAM-first MQTT ingestion — batch writer.
 *
 * Consumes the RAM stream with a consumer group (crash-safe: entries stay in
 * the Redis PEL until acknowledged after a successful database commit) and
 * persists batches through the same SQL as the durable path. Flush triggers:
 * batch reaches MQTT_RAM_BATCH_SIZE, or the oldest accumulated packet reaches
 * MQTT_RAM_MAX_AGE_MS — whichever comes first, checked on every wake (the
 * blocking read wakes at least once per second).
 *
 * Commit handoff: XACK (durable acknowledgement to the stream) → byte-budget
 * release → commit marker. Entries are NOT deleted at commit: they are
 * retained for a short overlap window so a reader that queried the database
 * just BEFORE the commit and the stream just AFTER it cannot fall into the
 * gap between the two stores. A retention sweep deletes committed entries
 * only when they are provably safe to remove: acknowledged (absent from the
 * PEL), at or below the newest commit boundary, and older than a minimum
 * retention guard. On failure the entries are left un-acked and retried with
 * backoff; a crashed writer's in-flight entries are reclaimed from the PEL
 * (own '0' reads + XAUTOCLAIM).
 */
import { createHash } from 'node:crypto';
import {
  isRetryableDatabaseError,
  retryDelay,
  type PacketBatchInput,
  type PersistEntry,
} from '../db/packetBatch.js';
import {
  decodeRamStreamValue,
  encodeCommitMarker,
  entryFieldName,
  getRamQuarantineKey,
  getRamQueueBytesKey,
  RAM_ENTRY_OVERHEAD_BYTES,
  type RamPendingEntry,
} from './protocol.js';
import {
  ramCommitBatchSize,
  ramCommitDuration,
  ramCommitTotal,
  ramBatchDedupeTotal,
  ramQuarantinedTotal,
  ramReclaimedTotal,
  ramRetryTotal,
  ramSweepDeletedTotal,
  ramVisibilityLatency,
} from '../metrics.js';

type StreamEntry = { id: string; entry: RamPendingEntry; bytes: number };

export interface RamWriterRedis {
  xgroup(
    command: string,
    key: string,
    group: string,
    id: string,
    ...args: string[]
  ): Promise<unknown>;
  xreadgroup(
    ...args: (string | number)[]
  ): Promise<Array<[string, Array<[string, string[]]>]> | null>;
  xack(key: string, group: string, ...ids: string[]): Promise<number>;
  xdel(key: string, ...ids: string[]): Promise<number>;
  xadd(key: string, id: string, field: string, value: string): Promise<string>;
  /** XADD with an approximate MAXLEN cap; used for the bounded quarantine stream. */
  xaddWithLimit(key: string, maxLen: number, field: string, value: string): Promise<string>;
  xautoclaim(
    key: string,
    group: string,
    consumer: string,
    minIdleTime: number,
    start: string,
    ...args: (string | number)[]
  ): Promise<[string, Array<[string, string[]]>, string[]]>;
  xrange(
    key: string,
    min: string,
    max: string,
    ...args: (string | number)[]
  ): Promise<Array<[string, string[]]>>;
  xrevrange(
    key: string,
    max: string,
    min: string,
    ...args: (string | number)[]
  ): Promise<Array<[string, string[]]>>;
  xpending(
    key: string,
    group: string,
    start: string,
    end: string,
    count: number,
  ): Promise<Array<[string, string, number, number]>>;
  decrby(key: string, amount: number): Promise<number>;
}

export type RamWriterDeps = {
  redis: RamWriterRedis;
  persist: (entries: PersistEntry[], idempotent: boolean) => Promise<void>;
  now: () => number;
  sleep: (ms: number) => Promise<void>;
  batchSize: () => number;
  maxAgeMs: () => number;
  streamKey: () => string;
  group: () => string;
  consumer: () => string;
  statsTick: (() => Promise<void>) | null;
  markHealthy: () => void;
  /** Runs after a batch commits (bookkeeping included); receives the persisted batch. */
  onCommitted: ((entries: StreamEntry[]) => void) | null;
  log: (message: string, error?: unknown) => void;
};

const MAX_PERSIST_ATTEMPTS = 7;
/**
 * Hard cap per flush. 26 columns per row against Postgres's 65,535 bind
 * parameter ceiling: 2,000 rows = 52,000 parameters, comfortably under the
 * limit while still draining reclaimed backlogs quickly. The normal loop
 * flushes at the configured batch size; this only bounds startup reclaim
 * of a large leftover PEL.
 */
const MAX_FLUSH_PACKETS = 2_000;
const MARKERS_KEPT = 20;
const STATS_INTERVAL_MS = 5_000;
/** Committed entries are retained at least this long before the sweep may delete them. */
const MIN_RETENTION_MS = 5_000;
const SWEEP_INTERVAL_MS = 10_000;
const SWEEP_SCAN_LIMIT = 2_000;
const PEL_LIST_LIMIT = 5_000;
/** Bounded recovery evidence for undecodable entries. */
const QUARANTINE_MAX_LEN = 500;
const QUARANTINE_VALUE_CAP = 65_536;
const FAILURE_BACKOFF_START_MS = 5_000;
const FAILURE_BACKOFF_MAX_MS = 30_000;

/** Identity used for intra-batch replay dedupe (mirrors the SQL existence check). */
export function persistIdentityKey(input: PacketBatchInput): string {
  return [
    input.time.toISOString(),
    input.packetHash,
    input.rxNodeId ?? '',
    input.topic,
    input.network,
  ].join('\u0000');
}

/** Collapse entries that share a single observation identity within one flush. */
export function collapseBatchEntries(batch: StreamEntry[]): StreamEntry[] {
  if (batch.length < 2) return batch;
  const seen = new Set<string>();
  const kept: StreamEntry[] = [];
  for (const item of batch) {
    const key = persistIdentityKey(item.entry.input);
    if (seen.has(key)) continue;
    seen.add(key);
    kept.push(item);
  }
  const dropped = batch.length - kept.length;
  if (dropped > 0) ramBatchDedupeTotal.inc(dropped);
  return kept;
}

/** Numeric stream-id comparison ("<ms>-<seq>"): correct across digit lengths. */
export function compareStreamIds(a: string, b: string): number {
  const [ams, aseq = '0'] = a.split('-');
  const [bms, bseq = '0'] = b.split('-');
  const ms = Number(ams) - Number(bms);
  if (ms !== 0) return ms;
  return Number(aseq) - Number(bseq);
}

/** Pure flush trigger used by the loop and by unit tests. */
export function flushDecision(
  acc: Array<{ entry: RamPendingEntry }>,
  nowMs: number,
  batchSize: number,
  maxAgeMs: number,
): 'none' | 'size' | 'age' {
  if (acc.length === 0) return 'none';
  if (acc.length >= batchSize) return 'size';
  const oldest = acc[0];
  if (oldest && nowMs - oldest.entry.admittedAtMs >= maxAgeMs) return 'age';
  return 'none';
}

type RamWriter = {
  running: boolean;
  start(): Promise<void>;
  requestStop(): void;
  drain(deadlineMs: number): Promise<void>;
  /** Test-only: run exactly one loop iteration. */
  tickOnce(): Promise<void>;
  /** Delete committed entries that are provably safe to remove. */
  sweepRetained(): Promise<void>;
  accumulatorSize(): number;
};

export function createRamWriter(deps: RamWriterDeps): RamWriter {
  const acc: StreamEntry[] = [];
  const accIds = new Set<string>();
  let running = false;
  let lastStatsAt = 0;
  let lastSweepAt = 0;
  // Newest commit boundary known to this writer instance (set in flush, seeded
  // on startup): the retention sweep's safety anchor.
  let lastThrough: string | null = null;
  let consecutiveFlushFailures = 0;
  let retryAfterMs = 0;

  function entryBytes(value: string): number {
    return Buffer.byteLength(value) + RAM_ENTRY_OVERHEAD_BYTES;
  }

  /**
   * Undecodable entries are acknowledged so they can never block the stream,
   * but not discarded silently: a bounded quarantine stream keeps the raw
   * value (size-capped) plus its digest for recovery, while the structured
   * log line carries only the id, byte length and digest — never content.
   */
  async function quarantineMalformed(id: string, value: string): Promise<void> {
    ramQuarantinedTotal.inc();
    const bytes = Buffer.byteLength(value);
    const digest = createHash('sha256').update(value).digest('hex');
    deps.log(`[ram] quarantined undecodable stream entry id=${id} bytes=${bytes} sha256=${digest}`);
    try {
      await deps.redis.xaddWithLimit(
        getRamQuarantineKey(),
        QUARANTINE_MAX_LEN,
        'q',
        JSON.stringify({
          src: id,
          at: deps.now(),
          sha256: digest,
          bytes,
          raw: value.length <= QUARANTINE_VALUE_CAP ? value : '',
        }),
      );
    } catch (error) {
      deps.log('[ram] quarantine write failed', error);
    }
  }

  function appendEntries(entries: StreamEntry[]): number {
    let added = 0;
    for (const item of entries) {
      if (accIds.has(item.id)) continue;
      acc.push(item);
      accIds.add(item.id);
      added += 1;
    }
    return added;
  }

  async function readNew(count: number, blockMs: number): Promise<void> {
    const res = await deps.redis.xreadgroup(
      'GROUP',
      deps.group(),
      deps.consumer(),
      'COUNT',
      count,
      'BLOCK',
      blockMs,
      'STREAMS',
      deps.streamKey(),
      '>',
    );
    if (!res) return;
    for (const [, items] of res) {
      for (const [id, fields] of items) {
        const decoded = decodeRamStreamValue(fields[1]);
        if (!decoded) {
          // Unparseable value: preserve bounded recovery evidence, then
          // acknowledge + delete so it can never block the stream.
          await quarantineMalformed(id, fields[1]);
          await deps.redis.xack(deps.streamKey(), deps.group(), id).catch(() => 0);
          await deps.redis.xdel(deps.streamKey(), id).catch(() => 0);
          continue;
        }
        if (decoded.kind === 'commit') {
          await deps.redis.xack(deps.streamKey(), deps.group(), id).catch(() => 0);
          continue;
        }
        appendEntries([
          { id, entry: decoded.entry, bytes: entryBytes(fields[1]) },
        ]);
      }
    }
  }

  /** Load this consumer's own un-acked entries (retry path after failures/restart). */
  async function readOwnPending(count: number): Promise<void> {
    const res = await deps.redis.xreadgroup(
      'GROUP',
      deps.group(),
      deps.consumer(),
      'COUNT',
      count,
      'STREAMS',
      deps.streamKey(),
      '0',
    );
    if (!res) return;
    for (const [, items] of res) {
      for (const [id, fields] of items) {
        const decoded = decodeRamStreamValue(fields[1]);
        if (decoded && decoded.kind === 'entry') {
          appendEntries([{ id, entry: decoded.entry, bytes: entryBytes(fields[1]) }]);
        }
      }
    }
  }

  async function persistWithRetry(batch: StreamEntry[]): Promise<void> {
    let attempt = 0;
    for (;;) {
      try {
        const entries: PersistEntry[] = batch.map(({ entry }) => ({
          input: entry.input,
          observationId: entry.observationId,
        }));
        // Always idempotent: a replay after a crash (or an ambiguous commit)
        // re-delivers the SAME frozen entries, and the existence check keyed
        // on their identity must apply from the first attempt.
        await deps.persist(entries, true);
        return;
      } catch (error) {
        attempt += 1;
        if (attempt >= MAX_PERSIST_ATTEMPTS || !isRetryableDatabaseError(error)) throw error;
        ramRetryTotal.inc();
        await deps.sleep(retryDelay(attempt - 1));
      }
    }
  }

  async function flush(reason: 'size' | 'age' | 'drain'): Promise<void> {
    if (acc.length === 0) return;
    const batch = acc.splice(0, Math.min(acc.length, MAX_FLUSH_PACKETS));
    const ids = batch.map((item) => item.id);
    const startedNs = process.hrtime.bigint();
    const nowMs = deps.now();
    const persistBatch = collapseBatchEntries(batch);
    try {
      await persistWithRetry(persistBatch);
    } catch (error) {
      // Leave the entries un-acked so they stay recoverable in the PEL; drop
      // them from memory and back off so a database outage cannot spin.
      for (const id of ids) accIds.delete(id);
      consecutiveFlushFailures += 1;
      retryAfterMs = nowMs + Math.min(
        FAILURE_BACKOFF_MAX_MS,
        FAILURE_BACKOFF_START_MS * consecutiveFlushFailures,
      );
      ramCommitTotal.inc({ outcome: 'failure' });
      ramCommitDuration.observe(
        { outcome: 'failure' },
        Number(process.hrtime.bigint() - startedNs) / 1e9,
      );
      deps.log(
        `[ram] batch commit failed (${batch.length} packets, ${reason}); entries left un-acked for retry`,
        error,
      );
      return;
    }

    consecutiveFlushFailures = 0;
    retryAfterMs = 0;
    for (const id of ids) accIds.delete(id);
    const streamKey = deps.streamKey();
    const group = deps.group();
    try {
      await deps.redis.xack(streamKey, group, ...ids);
      const releasedBytes = batch.reduce((sum, item) => sum + item.bytes, 0);
      await deps.redis.decrby(getRamQueueBytesKey(), releasedBytes).catch(() => 0);
      const lastId = ids[ids.length - 1];
      if (lastId) {
        await deps.redis.xadd(
          streamKey,
          '*',
          entryFieldName(),
          encodeCommitMarker({ kind: 'commit', through: lastId, atMs: deps.now(), packets: batch.length }),
        );
        lastThrough = lastId;
      }
    } catch (error) {
      // Commit succeeded; bookkeeping failed. Entries are retained and the
      // retention sweep re-deletes idempotently, so do not fail the flush —
      // surface and continue.
      deps.log('[ram] post-commit stream bookkeeping failed', error);
    }
    const finishedMs = deps.now();
    ramCommitTotal.inc({ outcome: 'success' });
    ramCommitBatchSize.observe(batch.length);
    ramCommitDuration.observe(
      { outcome: 'success' },
      Number(process.hrtime.bigint() - startedNs) / 1e9,
    );
    for (const item of batch) {
      ramVisibilityLatency.observe(Math.max(0, (finishedMs - item.entry.admittedAtMs) / 1000));
    }
    deps.markHealthy();
    if (deps.onCommitted) {
      try {
        deps.onCommitted(persistBatch);
      } catch (error) {
        deps.log('[ram] post-commit hook failed', error);
      }
    }
  }

  async function pruneMarkers(): Promise<void> {
    try {
      const recent = await deps.redis.xrange(deps.streamKey(), '-', '+', 'COUNT', 300);
      const markerIds: string[] = [];
      for (const [id, fields] of recent) {
        const decoded = decodeRamStreamValue(fields[1]);
        if (decoded && decoded.kind === 'commit') markerIds.push(id);
      }
      if (markerIds.length > MARKERS_KEPT) {
        const excess = markerIds.slice(0, markerIds.length - MARKERS_KEPT);
        await deps.redis.xdel(deps.streamKey(), ...excess);
      }
    } catch {
      /* best-effort */
    }
  }

  /**
   * Wait — deletion safety: an entry may only be removed when it is
   * acknowledged (not in the group PEL), at or below the newest commit
   * boundary, and past the minimum retention guard. Entries from failed
   * batches (out-of-order commits) stay in the PEL and are never touched.
   */
  async function sweepRetained(): Promise<void> {
    const streamKey = deps.streamKey();
    const group = deps.group();
    try {
      let through = lastThrough;
      if (!through) {
        const newest = await deps.redis.xrevrange(streamKey, '+', '-', 'COUNT', 500);
        for (const [, fields] of newest) {
          const decoded = decodeRamStreamValue(fields[1]);
          if (decoded && decoded.kind === 'commit') {
            through = decoded.marker.through;
            break;
          }
        }
      }
      if (!through) return;
      const pel = await deps.redis.xpending(streamKey, group, '-', '+', PEL_LIST_LIMIT).catch(() => null);
      if (!pel || pel.length >= PEL_LIST_LIMIT) return; // couldn't prove safety — skip
      const pelIds = new Set(pel.map(([id]) => id));
      const oldest = await deps.redis.xrange(streamKey, '-', '+', 'COUNT', SWEEP_SCAN_LIMIT);
      const cutoffMs = deps.now() - MIN_RETENTION_MS;
      const remove: string[] = [];
      for (const [id] of oldest) {
        if (compareStreamIds(id, through) > 0) break; // ordered ids: past this = pending/newer
        if (pelIds.has(id)) continue;
        const ms = Number(id.split('-')[0]);
        if (!Number.isFinite(ms) || ms > cutoffMs) continue;
        remove.push(id);
      }
      if (remove.length > 0) {
        await deps.redis.xdel(streamKey, ...remove).catch(() => 0);
        ramSweepDeletedTotal.inc(remove.length);
      }
    } catch {
      /* best-effort */
    }
  }

  async function tickOnce(): Promise<void> {
    const nowMs = deps.now();
    const batchSize = deps.batchSize();
    const exhausted = acc.length >= batchSize * 2;

    // 1. Retry previously failed (own PEL) entries once the backoff expires.
    if (!exhausted && acc.length < batchSize && nowMs >= retryAfterMs) {
      await readOwnPending(batchSize - acc.length);
    }
    // 2. Read new entries unless the accumulator is already at double batch capacity.
    if (!exhausted) {
      await readNew(batchSize - acc.length, running ? 1_000 : 100);
    }
    // 3. Stats refresh / marker pruning on a slow cadence.
    if (deps.statsTick && nowMs - lastStatsAt >= STATS_INTERVAL_MS) {
      lastStatsAt = nowMs;
      await deps.statsTick().catch(() => undefined);
      await pruneMarkers();
    }
    if (nowMs - lastSweepAt >= SWEEP_INTERVAL_MS) {
      lastSweepAt = nowMs;
      await sweepRetained();
    }
    // 4. Flush when the batch is full, the oldest entry is over the age bound,
    //    or (shutdown) whenever anything remains.
    const decision = flushDecision(acc, deps.now(), batchSize, deps.maxAgeMs());
    if (decision !== 'none') {
      await flush(decision);
    } else if (!running && acc.length > 0) {
      await flush('drain');
    }
  }

  return {
    get running() {
      return running;
    },
    async start(): Promise<void> {
      if (running) return;
      const streamKey = deps.streamKey();
      const group = deps.group();
      try {
        await deps.redis.xgroup('CREATE', streamKey, group, '0', 'MKSTREAM');
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        if (!message.includes('BUSYGROUP')) throw error;
      }
      // Reclaim entries left un-acked by a previous writer instance.
      let cursor = '0-0';
      for (;;) {
        const [next, claimed] = await deps.redis.xautoclaim(
          streamKey,
          group,
          deps.consumer(),
          10_000,
          cursor,
          'COUNT',
          500,
        );
        if (claimed.length > 0) {
          ramReclaimedTotal.inc(claimed.length);
          for (const [id, fields] of claimed) {
            const decoded = decodeRamStreamValue(fields[1]);
            if (decoded && decoded.kind === 'entry') {
              appendEntries([{ id, entry: decoded.entry, bytes: entryBytes(fields[1]) }]);
            }
          }
        }
        if (!next || next === '0-0' || next === '0') break;
        cursor = next;
      }
      // Seed the retention-sweep anchor from the newest marker present.
      try {
        const newest = await deps.redis.xrevrange(streamKey, '+', '-', 'COUNT', 500);
        for (const [, fields] of newest) {
          const decoded = decodeRamStreamValue(fields[1]);
          if (decoded && decoded.kind === 'commit') {
            lastThrough = decoded.marker.through;
            break;
          }
        }
      } catch {
        /* best-effort */
      }
      running = true;
      deps.log(`[ram] writer started (group=${group}, consumer=${deps.consumer()}, stream=${streamKey})`);
    },
    requestStop(): void {
      running = false;
    },
    async drain(deadlineMs: number): Promise<void> {
      running = false;
      const stopAt = deps.now() + deadlineMs;
      while (deps.now() < stopAt) {
        await tickOnce();
        if (acc.length === 0) {
          // Confirm the stream has nothing left for us: one more new-read.
          await readNew(100, 50);
          if (acc.length === 0) break;
        }
      }
      if (acc.length > 0) {
        await flush('drain');
      }
    },
    tickOnce,
    sweepRetained,
    accumulatorSize: () => acc.length,
  };
}

// ---------------------------------------------------------------- runtime
// The production singleton (Redis connection, database query, health marker
// wiring) lives in writerRuntime.ts so this module — and its unit tests —
// stay free of database/redis client imports.

