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
 * Commit handoff: mark each committed stream ID, then XACK. Entries and their
 * byte reservation remain until final cleanup. Public readers capture Redis
 * before starting SQL and deduplicate the overlap by observation ID, so even
 * expiry during a slow SQL read cannot open a handoff gap. The retention sweep
 * deletes only acknowledged entries with an individual commit timestamp past
 * the overlap guard; it never infers completion from a newer batch. Independent
 * readers use the durable ledger for completeness beyond this overlap.
 * On failure the entries are left un-acked and retried with
 * backoff; a crashed writer's in-flight entries are reclaimed from the PEL
 * (own '0' reads + XAUTOCLAIM).
 */
import { createHash } from 'node:crypto';
import {
  isRetryableDatabaseError,
  retryDelay,
  type PersistEntry,
} from '../db/packetBatch.js';
import {
  decodeRamStreamValue,
  accountedRamEntryBytes,
  getRamAdmissionKey,
  getRamCommittedKey,
  getRamQuarantineKey,
  getRamQueueBytesKey,
  RAM_QUARANTINE_MAX_ENTRIES,
  RAM_QUARANTINE_ENTRY_MAX_BYTES,
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

type StreamEntry = { id: string; entry: RamPendingEntry; bytes: number; raw: string };

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
  xadd(key: string, ...args: (string | number)[]): Promise<string>;
  xtrim(key: string, strategy: string, threshold: number): Promise<number>;
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
  hset(key: string, field: string, value: string): Promise<number>;
  hget(key: string, field: string): Promise<string | null>;
  eval(script: string, numKeys: number, ...args: (string | number)[]): Promise<unknown>;
}

export type RamWriterDeps = {
  redis: RamWriterRedis;
  persist: (entries: PersistEntry[], idempotent: boolean) => Promise<number | void>;
  now: () => number;
  sleep: (ms: number) => Promise<void>;
  batchSize: () => number;
  maxAgeMs: () => number;
  streamKey: () => string;
  group: () => string;
  consumer: () => string;
  statsTick: (() => Promise<void>) | null;
  markHealthy: () => void;
  markFailed?: (error: unknown) => void;
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
const MIN_RETENTION_MS = 120_000;
const SWEEP_INTERVAL_MS = 10_000;
const SWEEP_SCAN_LIMIT = 2_000;
const PEL_LIST_LIMIT = 5_000;
/** Bounded recovery evidence for undecodable entries. */
const QUARANTINE_MAX_LEN = RAM_QUARANTINE_MAX_ENTRIES;
const QUARANTINE_VALUE_CAP = RAM_QUARANTINE_ENTRY_MAX_BYTES;
const FAILURE_BACKOFF_START_MS = 5_000;
const FAILURE_BACKOFF_MAX_MS = 30_000;
const RECLAIM_INTERVAL_MS = 5_000;
const RECLAIM_PAGE_SIZE = 500;
const RECLAIM_PAGES_PER_PASS = 4;

function isPermanentPacketError(error: unknown): boolean {
  const code = (error as { code?: unknown } | null)?.code;
  return typeof code === 'string' && new Set(['22001', '22003', '22P02', '23502', '23514']).has(code);
}

// Deletion and accounting form one Redis command. A retry cannot release the
// same bytes twice: only a successful XDEL decrements the budget. The commit
// timestamp is checked again in Lua because a sweep may race a replay.
const CLEANUP_SCRIPT = `
local at = tonumber(redis.call('HGET', KEYS[3], ARGV[1]))
if not at or at > tonumber(ARGV[3]) then return 0 end
local removed = redis.call('XDEL', KEYS[1], ARGV[1])
if removed == 1 then
  redis.call('DECRBY', KEYS[2], tonumber(ARGV[2]))
  if tonumber(ARGV[4]) == 1 then redis.call('DEL', KEYS[4]) end
end
redis.call('HDEL', KEYS[3], ARGV[1])
return removed
`;

// A quarantined value is retained in the bounded evidence stream before this
// script runs. Source deletion, PEL release and byte accounting then happen
// together, including delivery-ledger cleanup for valid SQL poison rows.
const QUARANTINE_CLEANUP_SCRIPT = `
local acked = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
local removed = redis.call('XDEL', KEYS[1], ARGV[2])
if removed == 1 then
  redis.call('DECRBY', KEYS[2], tonumber(ARGV[3]))
  redis.call('HDEL', KEYS[3], ARGV[2])
  if tonumber(ARGV[4]) == 1 then redis.call('DEL', KEYS[4]) end
end
return removed
`;

/** Collapse only copies of the same stable observation ID within one flush. */
export function collapseBatchEntries(batch: StreamEntry[]): StreamEntry[] {
  if (batch.length < 2) return batch;
  const seen = new Set<string>();
  const kept: StreamEntry[] = [];
  for (const item of batch) {
    const key = item.entry.observationId;
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
  const oldestAt = Math.min(...acc.map((item) => item.entry.admittedAtMs));
  if (nowMs - oldestAt >= maxAgeMs) return 'age';
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
  let lastReclaimAt = 0;
  let reclaimCursor = '0-0';
  let consecutiveFlushFailures = 0;
  let retryAfterMs = 0;

  function entryBytes(value: string): number {
    return accountedRamEntryBytes(value);
  }

  /**
   * Undecodable entries are acknowledged so they can never block the stream,
   * but not discarded silently: a bounded quarantine stream keeps the raw
   * value (size-capped) plus its digest for recovery, while the structured
   * log line carries only the id, byte length and digest — never content.
   */
  async function quarantineMalformed(id: string, value: string, reason = 'undecodable', deliveryKey?: string): Promise<boolean> {
    const bytes = Buffer.byteLength(value);
    const digest = createHash('sha256').update(value).digest('hex');
    const record = {src:id,reason,at:deps.now(),sha256:digest,bytes,raw:bytes<=QUARANTINE_VALUE_CAP ? value : ''};
    let encoded = JSON.stringify(record);
    if (Buffer.byteLength(encoded)>QUARANTINE_VALUE_CAP) {
      record.raw = '';
      encoded = JSON.stringify(record);
    }
    try {
      await deps.redis.xadd(
        getRamQuarantineKey(),
        'MAXLEN',
        '~',
        QUARANTINE_MAX_LEN,
        '*',
        'q',
        encoded,
      );
      // Approximate XADD trimming is followed by an exact cap. Overshoot is
      // transient and one encoded record per concurrent writer; source
      // cleanup happens only after both commands succeed.
      await deps.redis.xtrim(getRamQuarantineKey(), 'MAXLEN', QUARANTINE_MAX_LEN);
      ramQuarantinedTotal.inc();
      deps.log(`[ram] quarantined ${reason} stream entry id=${id} bytes=${bytes} sha256=${digest}`);
    } catch (error) {
      deps.log('[ram] quarantine write failed', error);
      return false;
    }
    try {
      return await deps.redis.eval(
        QUARANTINE_CLEANUP_SCRIPT,
        4,
        deps.streamKey(), getRamQueueBytesKey(), getRamCommittedKey(),
        deliveryKey ? getRamAdmissionKey(deliveryKey) : `${deps.streamKey()}:unused`,
        deps.group(), id, entryBytes(value), deliveryKey ? 1 : 0,
      ) === 1;
    } catch (error) {
      deps.log('[ram] quarantine cleanup failed; source retained', error);
      return false;
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
          // Keep the source in the PEL until bounded quarantine succeeds.
          await quarantineMalformed(id, fields[1] ?? '').catch((error: unknown) => {
            deps.log('[ram] malformed cleanup failed', error);
          });
          continue;
        }
        if (decoded.kind === 'commit') {
          await deps.redis.xack(deps.streamKey(), deps.group(), id).catch(() => 0);
          continue;
        }
        appendEntries([
          { id, entry: decoded.entry, bytes: entryBytes(fields[1]), raw: fields[1] },
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
          appendEntries([{ id, entry: decoded.entry, bytes: entryBytes(fields[1]), raw: fields[1] }]);
        } else if (!decoded) {
          await quarantineMalformed(id, fields[1] ?? '').catch((error: unknown) => {
            deps.log('[ram] malformed own-PEL cleanup failed', error);
          });
        }
      }
    }
  }

  async function ensureGroup(): Promise<void> {
    try {
      await deps.redis.xgroup('CREATE', deps.streamKey(), deps.group(), '0', 'MKSTREAM');
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (!message.includes('BUSYGROUP')) throw error;
    }
  }

  async function reclaimForeign(minIdleMs = 10_000): Promise<void> {
    for (let page = 0; page < RECLAIM_PAGES_PER_PASS; page += 1) {
      const available = MAX_FLUSH_PACKETS - acc.length;
      if (available <= 0) return;
      const [next, claimed] = await deps.redis.xautoclaim(
        deps.streamKey(), deps.group(), deps.consumer(), minIdleMs,
        reclaimCursor, 'COUNT', Math.min(RECLAIM_PAGE_SIZE, available),
      );
      if (claimed.length > 0) {
        ramReclaimedTotal.inc(claimed.length);
        for (const [id, fields] of claimed) {
          const decoded = decodeRamStreamValue(fields[1]);
          if (decoded && decoded.kind === 'entry') {
            appendEntries([{ id, entry: decoded.entry, bytes: entryBytes(fields[1] ?? ''), raw: fields[1] ?? '' }]);
          } else if (!decoded) {
            await quarantineMalformed(id, fields[1] ?? '').catch((error: unknown) => {
              deps.log('[ram] malformed reclaimed cleanup failed', error);
            });
          }
        }
      }
      if (!next || next === '0-0' || next === '0') {
        reclaimCursor = '0-0';
        return;
      }
      reclaimCursor = next;
    }
  }

  async function persistWithRetry(batch: StreamEntry[]): Promise<number | void> {
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
        return await deps.persist(entries, true);
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
    let insertedCount: number | void;
    try {
      insertedCount = await persistWithRetry(persistBatch);
    } catch (error) {
      if (isPermanentPacketError(error)) {
        // A batch-level constraint error may identify one poison row. Retry
        // each distinct observation independently so valid work behind it
        // can commit. Quarantine is durable before any poison row is acked.
        let unresolved = false;
        let quarantined = false;
        let accepted = 0;
        for (const item of persistBatch) {
          const copies = batch.filter((candidate) => candidate.entry.observationId === item.entry.observationId);
          try {
            const count = await persistWithRetry([item]);
            accepted += typeof count === 'number' ? count : 1;
            const committedAt = String(deps.now());
            for (const copy of copies) await deps.redis.hset(getRamCommittedKey(), copy.id, committedAt);
            await deps.redis.xack(deps.streamKey(), deps.group(), ...copies.map((copy) => copy.id));
            try {
              deps.onCommitted?.([item]);
            } catch (hookError) {
              deps.log('[ram] post-commit hook failed', hookError);
            }
          } catch (rowError) {
            if (!isPermanentPacketError(rowError)) {
              unresolved = true;
              deps.log('[ram] isolated row failed transiently; left in PEL', rowError);
              continue;
            }
            for (const copy of copies) {
              if (!await quarantineMalformed(copy.id, copy.raw, 'sql-poison', copy.entry.deliveryKey)) {
                unresolved = true;
              } else {
                quarantined = true;
              }
            }
          }
        }
        for (const id of ids) accIds.delete(id);
        ramCommitBatchSize.observe(accepted);
        ramCommitTotal.inc({ outcome: unresolved || quarantined ? 'failure' : 'success' });
        if (unresolved || quarantined) deps.markFailed?.(error);
        else deps.markHealthy();
        return;
      }
      // Leave the entries un-acked so they stay recoverable in the PEL; drop
      // them from memory and back off so a database outage cannot spin.
      for (const id of ids) accIds.delete(id);
      consecutiveFlushFailures += 1;
      retryAfterMs = nowMs + Math.min(
        FAILURE_BACKOFF_MAX_MS,
        FAILURE_BACKOFF_START_MS * consecutiveFlushFailures,
      );
      ramCommitTotal.inc({ outcome: 'failure' });
      deps.markFailed?.(error);
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
    let bookkeepingFailed = false;
    try {
      const committedAt = String(deps.now());
      for (const id of ids) {
        await deps.redis.hset(getRamCommittedKey(), id, committedAt);
      }
      await deps.redis.xack(streamKey, group, ...ids);
    } catch (error) {
      bookkeepingFailed = true;
      deps.markFailed?.(error);
      // Commit succeeded; bookkeeping failed. Entries are retained and the
      // retention sweep re-deletes idempotently, so do not fail the flush —
      // surface and continue.
      deps.log('[ram] post-commit stream bookkeeping failed', error);
    }
    const finishedMs = deps.now();
    ramCommitTotal.inc({ outcome: 'success' });
    ramCommitBatchSize.observe(typeof insertedCount === 'number' ? insertedCount : persistBatch.length);
    ramCommitDuration.observe(
      { outcome: 'success' },
      Number(process.hrtime.bigint() - startedNs) / 1e9,
    );
    for (const item of batch) {
      ramVisibilityLatency.observe(Math.max(0, (finishedMs - item.entry.admittedAtMs) / 1000));
    }
    if (!bookkeepingFailed) deps.markHealthy();
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
   * acknowledged (not in the group PEL), individually marked committed, and
   * past the minimum retention guard measured from commit time. Failed older
   * batches and newer admissions cannot be mistaken for committed rows.
   */
  async function sweepRetained(): Promise<void> {
    const streamKey = deps.streamKey();
    const group = deps.group();
    try {
      const pel = await deps.redis.xpending(streamKey, group, '-', '+', PEL_LIST_LIMIT).catch(() => null);
      if (!pel || pel.length >= PEL_LIST_LIMIT) return; // couldn't prove safety — skip
      const pelIds = new Set(pel.map(([id]) => id));
      const oldest = await deps.redis.xrange(streamKey, '-', '+', 'COUNT', SWEEP_SCAN_LIMIT);
      const cutoffMs = deps.now() - MIN_RETENTION_MS;
      let removed = 0;
      for (const [id, fields] of oldest) {
        if (pelIds.has(id)) continue;
        const decoded = decodeRamStreamValue(fields[1]);
        if (!decoded || decoded.kind !== 'entry') continue;
        const committedAt = Number(await deps.redis.hget(getRamCommittedKey(), id));
        if (!Number.isFinite(committedAt) || committedAt <= 0 || committedAt > cutoffMs) continue;
        const deliveryKey = decoded.entry.deliveryKey;
        const result = await deps.redis.eval(
          CLEANUP_SCRIPT,
          4,
          streamKey,
          getRamQueueBytesKey(),
          getRamCommittedKey(),
          deliveryKey ? getRamAdmissionKey(deliveryKey) : `${streamKey}:unused`,
          id,
          entryBytes(fields[1] ?? ''),
          cutoffMs,
          deliveryKey ? 1 : 0,
        );
        if (result === 1) removed += 1;
      }
      if (removed > 0) ramSweepDeletedTotal.inc(removed);
    } catch {
      /* best-effort */
    }
  }

  async function tickCore(): Promise<void> {
    const nowMs = deps.now();
    const batchSize = deps.batchSize();
    const exhausted = acc.length >= batchSize * 2;

    // 1. Retry previously failed (own PEL) entries once the backoff expires.
    if (!exhausted && acc.length < batchSize && nowMs >= retryAfterMs) {
      await readOwnPending(batchSize - acc.length);
    }
    if (nowMs - lastReclaimAt >= RECLAIM_INTERVAL_MS && acc.length < MAX_FLUSH_PACKETS) {
      lastReclaimAt = nowMs;
      await reclaimForeign();
    }
    // 2. Read new entries unless the accumulator is already at double batch capacity.
    if (!exhausted && acc.length < batchSize) {
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

  async function tickOnce(): Promise<void> {
    try {
      await tickCore();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (!message.includes('NOGROUP')) throw error;
      await ensureGroup();
      reclaimCursor = '0-0';
      await tickCore();
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
      await ensureGroup();
      // A young foreign PEL may not qualify yet; tickCore revisits it.
      await reclaimForeign();
      lastReclaimAt = deps.now();
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
      // A foreign consumer may still own entries that were too young during
      // startup. Shutdown is the last local writer pass, so inspect them now.
      await reclaimForeign(0);
      if (acc.length > 0) await flush('drain');
      const pending = await deps.redis.xpending(deps.streamKey(), deps.group(), '-', '+', 1);
      if (pending.length > 0) {
        throw new Error(`RAM_DRAIN_INCOMPLETE:pel=${pending[0]![0]} accumulator=${acc.length}`);
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
