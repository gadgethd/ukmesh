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
 * Commit handoff: XACK (durable acknowledgement to the stream) → XDEL (entry
 * leaves the stream; the database is now the only source) → commit marker
 * (observability + consumers that want an explicit boundary). On failure the
 * entries are left un-acked and retried with backoff; a crashed writer's
 * in-flight entries are reclaimed from the PEL (own '0' reads + XAUTOCLAIM).
 */
import {
  isRetryableDatabaseError,
  retryDelay,
  type PersistEntry,
} from '../db/packetBatch.js';
import {
  decodeRamStreamValue,
  encodeCommitMarker,
  entryFieldName,
  type RamPendingEntry,
} from './protocol.js';
import {
  ramCommitBatchSize,
  ramCommitDuration,
  ramCommitTotal,
  ramReclaimedTotal,
  ramRetryTotal,
  ramVisibilityLatency,
} from '../metrics.js';

type StreamEntry = { id: string; entry: RamPendingEntry };

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
  log: (message: string, error?: unknown) => void;
};

const MAX_PERSIST_ATTEMPTS = 7;
const MARKERS_KEPT = 20;
const STATS_INTERVAL_MS = 5_000;
const FAILURE_BACKOFF_START_MS = 5_000;
const FAILURE_BACKOFF_MAX_MS = 30_000;

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
  accumulatorSize(): number;
};

export function createRamWriter(deps: RamWriterDeps): RamWriter {
  const acc: StreamEntry[] = [];
  const accIds = new Set<string>();
  let running = false;
  let lastStatsAt = 0;
  let consecutiveFlushFailures = 0;
  let retryAfterMs = 0;

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
          // Unparseable value: acknowledge + delete so it can never block the stream.
          await deps.redis.xack(deps.streamKey(), deps.group(), id).catch(() => 0);
          await deps.redis.xdel(deps.streamKey(), id).catch(() => 0);
          continue;
        }
        if (decoded.kind === 'commit') {
          await deps.redis.xack(deps.streamKey(), deps.group(), id).catch(() => 0);
          continue;
        }
        appendEntries([{ id, entry: decoded.entry }]);
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
        if (decoded && decoded.kind === 'entry') appendEntries([{ id, entry: decoded.entry }]);
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
        await deps.persist(entries, attempt > 0);
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
    const batch = acc.splice(0, acc.length);
    const ids = batch.map((item) => item.id);
    const startedNs = process.hrtime.bigint();
    const nowMs = deps.now();
    try {
      await persistWithRetry(batch);
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
      await deps.redis.xdel(streamKey, ...ids);
      const lastId = ids[ids.length - 1];
      if (lastId) {
        await deps.redis.xadd(
          streamKey,
          '*',
          entryFieldName(),
          encodeCommitMarker({ kind: 'commit', through: lastId, atMs: deps.now(), packets: batch.length }),
        );
      }
    } catch (error) {
      // Commit succeeded; bookkeeping failed. Readers dedupe committed rows by
      // identity, so do not fail the flush — surface and continue.
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
            if (decoded && decoded.kind === 'entry') appendEntries([{ id, entry: decoded.entry }]);
          }
        }
        if (!next || next === '0-0' || next === '0') break;
        cursor = next;
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
    accumulatorSize: () => acc.length,
  };
}

// ---------------------------------------------------------------- runtime
// The production singleton (Redis connection, database query, health marker
// wiring) lives in writerRuntime.ts so this module — and its unit tests —
// stay free of database/redis client imports.

