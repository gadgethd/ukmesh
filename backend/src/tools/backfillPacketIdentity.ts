import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import pg from 'pg';

/**
 * P2-5: resumable identity backfill for historical `mesh.packets` rows.
 *
 * Only rows whose stored 64-char packet_hash is reproducible from retained
 * `raw_hex` are marked resolved. Short/empty legacy hashes are never expanded.
 * One bounded batch is selected, updated and checkpointed in one transaction.
 *
 * Compressed (columnstore) chunks hold the oldest history — the rows most in
 * need of a backfill — so every chunk in the hypertable is enumerated. A
 * compressed chunk is decompressed before it is read and updated, and is
 * recompressed as soon as its last row has been processed. Decompressions are
 * persisted in the state row before any read, so a crash or a `--limit` stop
 * can be recovered: `finally` recompresses everything this run (or a crashed
 * predecessor) decompressed, and a resumed run never decompresses a chunk that
 * is already decompressed.
 *
 * The completion claim is structural: `packets_complete` is written only when
 * every enumerated chunk was read to exhaustion. Otherwise the run records
 * `packets_partial`, the unvisited chunks, and how many of them hold
 * compressed data. Per-chunk results are persisted in the state row.
 */

type Row = {
  time: string;
  packet_hash: string;
  rx_node_id: string | null;
  observation_id: string;
  raw_hex: string | null;
  ctid: string;
};

type Counts = {
  selected: number;
  resolved: number;
  unresolved: number;
  mismatch: number;
};

type State = {
  high_water: string | null;
  last_time: string | null;
  last_observation_id: string | null;
  last_hash: string | null;
  last_rx_node_id: string | null;
  decompressed_chunks: string[];
};

type Chunk = {
  schema: string;
  name: string;
  compressed: boolean;
  rangeStart: string;
  rangeEnd: string;
};

type ChunkResult = {
  schema: string;
  name: string;
  range_start: string;
  range_end: string;
  compressed_at_start: boolean;
  decompressed_previously: boolean;
  decompressed_by_run: boolean;
  recompressed: boolean;
  rows_selected: number;
  resolved: number;
  unresolved: number;
  mismatch: number;
  unresolved_reasons: Record<string, number>;
  complete: boolean;
  note: string | null;
  remaining_eligible: number | null;
};

type BackfillResult = Counts & {
  unresolvedReasons: Record<string, number>;
  chunks: ChunkResult[];
  chunksTotal: number;
  chunksComplete: number;
  skippedCompressed: number;
  eligible: number | null;
  status: 'packets_complete' | 'packets_partial' | null;
  statusReason: string | null;
};

type UnresolvedReason = 'missing' | 'invalid-hex' | 'too-short' | 'malformed-header';

type ResolveOutcome =
  | { ok: true; uid: string }
  | { ok: false; reason: UnresolvedReason };

const RUN_LOCK_KEY = 'meshcore.packet_identity_backfill';
// Resuming from the start of the checkpointed instant is safe because the
// update only touches rows whose packet_uid is still NULL.
const CURSOR_FLOOR = '00000000-0000-0000-0000-000000000000';

/**
 * Hold one session-level advisory lock for the whole run. A transaction-level
 * lock is released at every batch COMMIT, which let a second run interleave
 * between batches.
 */
async function acquireRunLock(client: pg.PoolClient): Promise<void> {
  const result = await client.query<{ locked: boolean }>(
    'SELECT pg_try_advisory_lock(hashtext($1)) AS locked',
    [RUN_LOCK_KEY],
  );
  if (!result.rows[0]?.locked) throw new Error('packet identity backfill is already running');
}

async function releaseRunLock(client: pg.PoolClient): Promise<void> {
  await client.query('SELECT pg_advisory_unlock(hashtext($1)) AS unlocked', [RUN_LOCK_KEY]);
}

function resolveRawHex(rawHex: string | null): ResolveOutcome {
  const hex = rawHex?.trim() ?? '';
  if (!hex) return { ok: false, reason: 'missing' };
  if (hex.length % 2 !== 0 || !/^[0-9A-Fa-f]+$/.test(hex)) return { ok: false, reason: 'invalid-hex' };
  const bytes = Buffer.from(hex, 'hex');
  if (bytes.length < 2) return { ok: false, reason: 'too-short' };

  const routeType = bytes[0]! & 0x03;
  let offset = 1;
  if (routeType === 0 || routeType === 3) {
    if (bytes.length < offset + 4) return { ok: false, reason: 'malformed-header' };
    offset += 4;
  }
  if (bytes.length <= offset) return { ok: false, reason: 'malformed-header' };

  const encodedLength = bytes[offset]!;
  const pathHashCount = encodedLength & 0x3f;
  const pathHashSize = (encodedLength >> 6) + 1;
  if (pathHashSize > 3) return { ok: false, reason: 'malformed-header' };

  const pathEnd = offset + 1 + pathHashCount * pathHashSize;
  if (bytes.length <= pathEnd) return { ok: false, reason: 'malformed-header' };

  const hash = createHash('sha256');
  hash.update(Uint8Array.of(bytes[0]! & 0xFC));
  hash.update(bytes.subarray(pathEnd));
  return { ok: true, uid: hash.digest('hex').toUpperCase() };
}

function parseArgs(argv: string[]) {
  const args = { batchSize: 500, dryRun: false, limit: 0, phase: 'packets', since: null as string | null };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--batch-size') args.batchSize = Number(argv[++i]);
    else if (arg === '--limit') args.limit = Number(argv[++i]);
    else if (arg === '--dry-run') args.dryRun = true;
    else if (arg === '--phase') args.phase = argv[++i] ?? 'packets';
    else if (arg === '--since') args.since = argv[++i] ?? null;
    else throw new Error(`unknown argument: ${arg}`);
  }
  if (!Number.isInteger(args.batchSize) || args.batchSize < 1 || args.batchSize > 5000) {
    throw new Error('--batch-size must be an integer between 1 and 5000');
  }
  if (!Number.isInteger(args.limit) || args.limit < 0) throw new Error('--limit must be a non-negative integer');
  if (!['packets', 'paths', 'aliases'].includes(args.phase)) {
    throw new Error('--phase must be packets, paths or aliases');
  }
  return args;
}

function dsn(): string {
  const value = process.env['BACKFILL_DATABASE_URL']
    ?? process.env['MIGRATE_DATABASE_URL']
    ?? process.env['DATABASE_URL'];
  if (!value) throw new Error('BACKFILL_DATABASE_URL, MIGRATE_DATABASE_URL or DATABASE_URL is required');
  return value;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function loadState(client: pg.PoolClient): Promise<State> {
  const result = await client.query<State>(`
    SELECT high_water::text AS high_water,
           last_time::text AS last_time,
           last_observation_id::text AS last_observation_id,
           last_hash,
           last_rx_node_id,
           COALESCE(decompressed_chunks, '{}'::text[]) AS decompressed_chunks
      FROM ops.packet_identity_backfill_state WHERE singleton
  `);
  if (!result.rows[0]) throw new Error('packet identity backfill state row is missing');
  return { ...result.rows[0], decompressed_chunks: result.rows[0].decompressed_chunks ?? [] };
}

/**
 * Translate a pre-131 cursor (time, hash, rx_node_id) to the unique
 * (time, observation_id) cursor instead of refusing to resume. The minimum
 * observation id at the checkpointed instant cannot skip work: rows already
 * resolved are excluded by the packet_uid IS NULL filter.
 */
async function migrateLegacyCheckpoint(
  client: pg.PoolClient,
  state: State,
  dryRun: boolean,
): Promise<State> {
  if (!state.last_time) return state;
  let observationId = CURSOR_FLOOR;
  if (state.last_hash) {
    const legacy = await client.query<{ observation_id: string | null }>(`
      SELECT observation_id::text AS observation_id
        FROM mesh.packets
       WHERE time = $1::timestamptz
         AND packet_hash = $2
         AND COALESCE(rx_node_id, '') = $3
         AND observation_id IS NOT NULL
       ORDER BY observation_id
       LIMIT 1
    `, [state.last_time, state.last_hash, state.last_rx_node_id ?? '']);
    observationId = legacy.rows[0]?.observation_id ?? CURSOR_FLOOR;
  }
  const migrated = { ...state, last_observation_id: observationId };
  if (!dryRun) {
    await client.query(`
      UPDATE ops.packet_identity_backfill_state
         SET last_observation_id = $1::uuid,
             last_hash = NULL,
             last_rx_node_id = NULL,
             updated_at = NOW()
       WHERE singleton
    `, [observationId]);
  }
  return migrated;
}

/**
 * Every chunk of the hypertable, compressed or not, in ascending time order.
 * The previous implementation filtered `is_compressed = FALSE`, which silently
 * excluded the oldest (and largest) half of the table.
 */
async function packetChunks(client: pg.PoolClient): Promise<Chunk[]> {
  const result = await client.query<{
    schema: string; name: string; compressed: boolean; range_start: string; range_end: string;
  }>(`
    SELECT chunk_schema AS schema,
           chunk_name AS name,
           is_compressed AS compressed,
           range_start::text AS range_start,
           range_end::text AS range_end
      FROM timescaledb_information.chunks
     WHERE hypertable_schema = 'mesh' AND hypertable_name = 'packets'
     ORDER BY range_start, chunk_name
  `);
  return result.rows.map((row) => ({
    schema: row.schema,
    name: row.name,
    compressed: row.compressed,
    rangeStart: row.range_start,
    rangeEnd: row.range_end,
  }));
}

function quoteIdent(value: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) throw new Error(`refusing unexpected chunk identifier: ${value}`);
  return `"${value}"`;
}

function chunkKey(chunk: { schema: string; name: string }): string {
  return `${chunk.schema}.${chunk.name}`;
}

function parseChunkKey(key: string): { schema: string; name: string } {
  const dot = key.indexOf('.');
  if (dot <= 0 || dot === key.length - 1) throw new Error(`refusing unexpected chunk key: ${key}`);
  return { schema: key.slice(0, dot), name: key.slice(dot + 1) };
}

/**
 * Decompress through format('%I.%I') so a chunk name can never be interpolated
 * into SQL. TimescaleDB treats a second decompress of an already-decompressed
 * chunk as a no-op NOTICE, so a resumed run is safe even if the catalog and the
 * persisted marker disagree.
 */
async function decompressChunk(client: pg.PoolClient, chunk: { schema: string; name: string }): Promise<void> {
  await client.query(
    `SELECT decompress_chunk(format('%I.%I', $1::text, $2::text)::regclass)`,
    [chunk.schema, chunk.name],
  );
}

async function recompressChunk(client: pg.PoolClient, chunk: { schema: string; name: string }): Promise<void> {
  await client.query(
    `SELECT compress_chunk(format('%I.%I', $1::text, $2::text)::regclass)`,
    [chunk.schema, chunk.name],
  );
}

async function persistDecompressed(client: pg.PoolClient, owned: Set<string>): Promise<void> {
  await client.query(`
    UPDATE ops.packet_identity_backfill_state
       SET decompressed_chunks = $1::text[], updated_at = NOW()
     WHERE singleton
  `, [[...owned]]);
}

function cursorWhere(state: State, since: string | null, values: unknown[]): string[] {
  const where = [
    'packet_uid IS NULL',
    'length(packet_hash) = 64',
    'observation_id IS NOT NULL',
  ];
  values.push(state.high_water);
  where.push(`time <= $${values.length}::timestamptz`);
  if (since) {
    values.push(since);
    where.push(`time >= $${values.length}::timestamptz`);
  }
  if (state.last_time && state.last_observation_id) {
    values.push(state.last_time, state.last_observation_id);
    const p = values.length - 1;
    where.push(`(
      time > $${p}::timestamptz
      OR (time = $${p}::timestamptz AND observation_id > $${p + 1}::uuid)
    )`);
  }
  return where;
}

/**
 * Read one batch. Real runs read the chunk relation by ctid (the update needs
 * it). Dry runs read the hypertable with the chunk's time bounds: a compressed
 * chunk cannot expose ctid, but it can be counted through the columnstore scan
 * without being decompressed, so `--dry-run` costs no disk.
 */
async function packetBatch(
  client: pg.PoolClient,
  state: State,
  chunk: Chunk,
  batchSize: number,
  since: string | null,
  withCtid: boolean,
): Promise<Row[]> {
  if (!state.high_water) return [];
  const values: unknown[] = [];
  const where = cursorWhere(state, since, values);
  let source: string;
  if (withCtid) {
    source = `${quoteIdent(chunk.schema)}.${quoteIdent(chunk.name)}`;
  } else {
    values.push(chunk.rangeStart, chunk.rangeEnd);
    where.push(`time >= $${values.length - 1}::timestamptz`, `time < $${values.length}::timestamptz`);
    source = 'mesh.packets';
  }
  values.push(batchSize);
  const result = await client.query<Row>(`
    SELECT time::text AS time, packet_hash, rx_node_id,
           observation_id::text AS observation_id, raw_hex,
           ${withCtid ? 'ctid::text AS ctid' : "''::text AS ctid"}
      FROM ${source}
     WHERE ${where.join(' AND ')}
     ORDER BY time, observation_id
     LIMIT $${values.length}
  `, values);
  return result.rows;
}

/** Eligible rows for the current cursor, optionally restricted to one chunk. */
async function eligibleCount(
  client: pg.PoolClient,
  state: State,
  since: string | null,
  chunk?: Chunk,
): Promise<number> {
  if (!state.high_water) return 0;
  const values: unknown[] = [];
  const where = cursorWhere(state, since, values);
  if (chunk) {
    values.push(chunk.rangeStart, chunk.rangeEnd);
    where.push(`time >= $${values.length - 1}::timestamptz`, `time < $${values.length}::timestamptz`);
  }
  const result = await client.query<{ n: string }>(`
    SELECT count(*)::text AS n FROM mesh.packets WHERE ${where.join(' AND ')}
  `, values);
  return Number(result.rows[0]?.n ?? 0);
}

/**
 * Rows this run would still visit in one chunk.
 *
 * `pending` counts rows the checkpoint cursor would select. When that is zero
 * the chunk is either finished or entirely "before" the cursor — and a cursor
 * written by an older version (which skipped compressed chunks) proves
 * nothing. The second count asks the chunk what the cursor excludes; a
 * non-zero answer means the cursor cannot be trusted for this chunk, so the
 * caller restarts that chunk from its own range start. `packet_uid IS NULL`
 * keeps the re-read idempotent.
 */
async function chunkPending(
  client: pg.PoolClient,
  cursor: State,
  since: string | null,
  chunk: Chunk,
): Promise<{ pending: number; resetCursor: boolean }> {
  const withCursor = await eligibleCount(client, cursor, since, chunk);
  if (withCursor > 0) return { pending: withCursor, resetCursor: false };
  const withoutCursor = await eligibleCount(
    client,
    { ...cursor, last_time: null, last_observation_id: null },
    since,
    chunk,
  );
  return { pending: withoutCursor, resetCursor: withoutCursor > 0 };
}

function emptyResult(chunk: Chunk, compressedAtStart: boolean, decompressedPreviously: boolean): ChunkResult {
  return {
    schema: chunk.schema,
    name: chunk.name,
    range_start: chunk.rangeStart,
    range_end: chunk.rangeEnd,
    compressed_at_start: compressedAtStart,
    decompressed_previously: decompressedPreviously,
    decompressed_by_run: false,
    recompressed: false,
    rows_selected: 0,
    resolved: 0,
    unresolved: 0,
    mismatch: 0,
    unresolved_reasons: {},
    complete: false,
    note: null,
    remaining_eligible: null,
  };
}

function countSkippedCompressed(results: ChunkResult[]): number {
  return results.filter(
    (entry) => (entry.compressed_at_start || entry.decompressed_previously) && !entry.complete,
  ).length;
}

async function recordChunkResults(
  client: pg.PoolClient,
  results: ChunkResult[],
  skippedCompressed: number,
): Promise<void> {
  await client.query(`
    UPDATE ops.packet_identity_backfill_state
       SET chunk_results = $1::jsonb, skipped_compressed = $2, updated_at = NOW()
     WHERE singleton
  `, [JSON.stringify(results), skippedCompressed]);
}

export async function backfillPackets(
  pool: pg.Pool,
  batchSize: number,
  dryRun: boolean,
  limit: number,
  since: string | null,
): Promise<BackfillResult> {
  const client = await pool.connect();
  const total: Counts = { selected: 0, resolved: 0, unresolved: 0, mismatch: 0 };
  const unresolvedReasons: Record<string, number> = {};
  const results: ChunkResult[] = [];
  const owned = new Set<string>();
  const chunksByKey = new Map<string, { schema: string; name: string }>();
  let lockHeld = false;
  let failure: unknown = null;
  let status: BackfillResult['status'] = null;
  let statusReason: string | null = null;
  let eligible: number | null = null;
  let skippedCompressed = 0;

  try {
    await acquireRunLock(client);
    lockHeld = true;
    let state = await loadState(client);
    if (state.last_time && !state.last_observation_id) {
      state = await migrateLegacyCheckpoint(client, state, dryRun);
    }
    if (!state.high_water) {
      const highWater = await client.query<{ high_water: string | null }>(`
        SELECT max(time)::text AS high_water FROM mesh.packets
         WHERE packet_uid IS NULL AND length(packet_hash) = 64
      `);
      state = { ...state, high_water: highWater.rows[0]?.high_water ?? null };
      if (!dryRun) {
        await client.query(`
          UPDATE ops.packet_identity_backfill_state
             SET high_water = $1::timestamptz, status = 'running', updated_at = NOW()
           WHERE singleton
        `, [state.high_water]);
      }
    }
    // A dry run never writes and never holds decompressions, so a marker left
    // by a crashed real run is reported but not acted on.
    if (!dryRun) {
      for (const key of state.decompressed_chunks) owned.add(key);
    }

    const chunks = await packetChunks(client);
    for (const chunk of chunks) chunksByKey.set(chunkKey(chunk), chunk);

    let cursor: State = { ...state };
    let stoppedByLimit = false;

    for (const chunk of chunks) {
      const key = chunkKey(chunk);
      const previouslyDecompressed = owned.has(key);
      const result = emptyResult(chunk, chunk.compressed || previouslyDecompressed, previouslyDecompressed);
      results.push(result);

      if (stoppedByLimit) {
        result.note = 'stopped-by-limit';
        result.remaining_eligible = (await chunkPending(client, cursor, since, chunk)).pending;
        continue;
      }

      // Never trust the checkpoint cursor for a chunk on geometry alone. An
      // older version advanced the cursor over uncompressed chunks only, so a
      // compressed chunk can sit entirely "before" a cursor that never read a
      // single one of its rows. Ask the chunk instead: rows the cursor would
      // still select, and — when that is zero — rows the cursor excludes.
      const pending = await chunkPending(client, cursor, since, chunk);
      if (dryRun) eligible = (eligible ?? 0) + pending.pending;
      if (pending.pending === 0) {
        result.complete = true;
        result.note = 'no-eligible-rows';
        continue;
      }
      let chunkCursor = cursor;
      if (pending.resetCursor) {
        chunkCursor = { ...cursor, last_time: null, last_observation_id: null };
        result.note = 'cursor-does-not-cover-chunk';
        console.error(JSON.stringify({
          cursor_reset_for_chunk: key,
          pending: pending.pending,
          reason: 'the checkpoint cursor predates the rows this chunk still holds',
        }));
      }

      if (chunk.compressed && !dryRun) {
        try {
          await decompressChunk(client, chunk);
        } catch (error) {
          result.note = 'decompress-failed';
          throw new Error(
            `cannot decompress chunk ${key}: ${errorMessage(error)}. Aborting: continuing would silently skip its rows.`,
          );
        }
        result.decompressed_by_run = true;
        owned.add(key);
        // Persist before reading so a crash cannot leave an unowned
        // decompression behind.
        await persistDecompressed(client, owned);
      }

      if (limit > 0 && total.selected >= limit) {
        stoppedByLimit = true;
        result.note = 'stopped-by-limit';
        result.remaining_eligible = (await chunkPending(client, cursor, since, chunk)).pending;
        continue;
      }

      while (true) {
        await client.query('BEGIN');
        try {
          const rows = await packetBatch(client, chunkCursor, chunk, batchSize, since, !dryRun);
          if (rows.length === 0) {
            await client.query('COMMIT');
            result.complete = true;
            break;
          }

          const resolved: Row[] = [];
          const uids: string[] = [];
          let batchUnresolved = 0;
          let batchMismatch = 0;
          const batchReasons: Record<string, number> = {};
          for (const row of rows) {
            const outcome = resolveRawHex(row.raw_hex);
            if (!outcome.ok) {
              batchUnresolved += 1;
              batchReasons[outcome.reason] = (batchReasons[outcome.reason] ?? 0) + 1;
            } else if (outcome.uid !== row.packet_hash.toUpperCase()) {
              batchMismatch += 1;
            } else {
              resolved.push(row);
              uids.push(outcome.uid);
            }
          }

          let resolvedCount = 0;
          if (dryRun) {
            resolvedCount = resolved.length;
          } else if (resolved.length > 0) {
            const update = await client.query<{ ctid: string }>(`
              UPDATE ${quoteIdent(chunk.schema)}.${quoteIdent(chunk.name)} p
                 SET packet_uid = decode(v.uid_hex, 'hex'),
                     identity_version = 'wire-sha256-v1',
                     identity_provenance = 'historical-raw-hex-recompute'
                FROM (
                  SELECT unnest($1::tid[]) AS ctid,
                         unnest($2::text[]) AS uid_hex
                ) v
               WHERE p.ctid = v.ctid
                 AND p.packet_uid IS NULL
              RETURNING p.ctid::text AS ctid
            `, [resolved.map((row) => row.ctid), uids]);
            resolvedCount = update.rowCount ?? 0;
          }

          const last = rows[rows.length - 1]!;
          if (!dryRun) {
            await client.query(`
              UPDATE ops.packet_identity_backfill_state
                 SET status = 'running',
                     last_time = $1::timestamptz,
                     last_observation_id = $2::uuid,
                     resolved = resolved + $3,
                     unresolved = unresolved + $4,
                     ambiguous = ambiguous + $5,
                     batch_count = batch_count + 1,
                     updated_at = NOW()
               WHERE singleton
            `, [last.time, last.observation_id, resolvedCount, batchUnresolved, batchMismatch]);
          }
          await client.query('COMMIT');

          const next: State = { ...cursor, last_time: last.time, last_observation_id: last.observation_id };
          cursor = next;
          // The chunk cursor is the same checkpoint once the chunk is being
          // read; only its starting point differed for a reset chunk.
          chunkCursor = next;
          result.rows_selected += rows.length;
          result.resolved += resolvedCount;
          result.unresolved += batchUnresolved;
          result.mismatch += batchMismatch;
          for (const [reason, count] of Object.entries(batchReasons)) {
            result.unresolved_reasons[reason] = (result.unresolved_reasons[reason] ?? 0) + count;
            unresolvedReasons[reason] = (unresolvedReasons[reason] ?? 0) + count;
          }
          total.selected += rows.length;
          total.resolved += resolvedCount;
          total.unresolved += batchUnresolved;
          total.mismatch += batchMismatch;
          console.log(JSON.stringify({
            batch: rows.length,
            chunk: key,
            selected: total.selected,
            resolved: total.resolved,
            unresolved: total.unresolved,
            mismatch: total.mismatch,
            last: last.time,
          }));
          if (rows.length < batchSize) {
            result.complete = true;
          }
          if (limit > 0 && total.selected >= limit) {
            stoppedByLimit = true;
            if (!result.complete) result.note = 'stopped-by-limit';
            break;
          }
          if (result.complete) break;
        } catch (error) {
          await client.query('ROLLBACK');
          throw error;
        }
      }

      if (result.complete && owned.has(key)) {
        try {
          await recompressChunk(client, chunk);
          owned.delete(key);
          result.recompressed = true;
          await persistDecompressed(client, owned);
        } catch (error) {
          result.note = `recompress-failed: ${errorMessage(error)}`;
          console.error(JSON.stringify({ recompress_failed: key, error: errorMessage(error) }));
        }
      }
      if (!dryRun) {
        skippedCompressed = countSkippedCompressed(results);
        await recordChunkResults(client, results, skippedCompressed);
      }
    }

    const incomplete = results.filter((entry) => !entry.complete);
    skippedCompressed = countSkippedCompressed(results);

    if (dryRun) {
      if (limit === 0 && eligible !== null && total.selected !== eligible) {
        throw new Error(
          `dry run counted ${total.selected} rows but the eligible denominator is ${eligible}; refusing to report a partial count as complete`,
        );
      }
    } else {
      status = incomplete.length === 0 ? 'packets_complete' : 'packets_partial';
      statusReason = incomplete.length === 0
        ? null
        : `${incomplete.length} of ${results.length} chunks unvisited (${incomplete
          .map((entry) => `${entry.schema}.${entry.name}`)
          .join(', ')})${stoppedByLimit ? '; stopped by --limit' : ''}`;
      await recordChunkResults(client, results, skippedCompressed);
      await client.query(`
        UPDATE ops.packet_identity_backfill_state
           SET status = $1, status_reason = $2, updated_at = NOW()
         WHERE singleton
      `, [status, statusReason]);
    }

    return {
      ...total,
      unresolvedReasons,
      chunks: results,
      chunksTotal: results.length,
      chunksComplete: results.length - incomplete.length,
      skippedCompressed,
      eligible,
      status,
      statusReason,
    };
  } catch (error) {
    failure = error;
    if (!dryRun && lockHeld) {
      try {
        await client.query('ROLLBACK');
      } catch {
        // No transaction in flight.
      }
      try {
        skippedCompressed = countSkippedCompressed(results);
        await recordChunkResults(client, results, skippedCompressed);
        await client.query(`
          UPDATE ops.packet_identity_backfill_state
             SET status = 'failed', status_reason = $1, updated_at = NOW()
           WHERE singleton
        `, [errorMessage(error)]);
      } catch {
        // The connection may be gone; the decompression marker is already durable.
      }
    }
    throw error;
  } finally {
    if (lockHeld) {
      // Recompress everything this run (or a crashed predecessor) decompressed.
      // Compressed chunks are the only ones in `owned`, so an ingest-time chunk
      // is never converted to columnstore by the backfill.
      for (const key of [...owned]) {
        try {
          const chunk = chunksByKey.get(key) ?? parseChunkKey(key);
          await recompressChunk(client, chunk);
          owned.delete(key);
          const result = results.find((entry) => chunkKey(entry) === key);
          if (result) result.recompressed = true;
        } catch (error) {
          console.error(JSON.stringify({ recompress_failed: key, error: errorMessage(error) }));
        }
      }
      try {
        if (!dryRun) await persistDecompressed(client, owned);
      } catch (error) {
        console.error(JSON.stringify({ persist_decompressed_failed: errorMessage(error) }));
      }
      if (failure instanceof Error) {
        console.error(JSON.stringify({ failed: failure.message, chunks: results.length }));
      }
      try {
        await releaseRunLock(client);
        client.release();
      } catch (error) {
        // Destroying the session is the only guaranteed way to drop a
        // session-level advisory lock when the unlock query itself fails.
        client.release(error instanceof Error ? error : true);
      }
    } else {
      client.release();
    }
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.phase !== 'packets') throw new Error('only --phase packets is implemented in this release');
  const pool = new pg.Pool({ connectionString: dsn(), max: 1 });
  try {
    const result = await backfillPackets(pool, args.batchSize, args.dryRun, args.limit, args.since);
    console.log(JSON.stringify({ dryRun: args.dryRun, ...result }));
  } finally {
    await pool.end();
  }
}

const invokedDirectly = process.argv[1] !== undefined
  && import.meta.url === pathToFileURL(process.argv[1]).href;

if (invokedDirectly) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
