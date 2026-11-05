import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import pg from 'pg';

/**
 * P2-5: resumable identity backfill for historical `mesh.packets` rows.
 *
 * Only rows whose stored 64-char packet_hash is reproducible from retained
 * `raw_hex` are marked resolved. Short/empty legacy hashes are never expanded.
 * One bounded batch is selected, updated and checkpointed in one transaction.
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
};

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

function canonicalFromRawHex(rawHex: string | null): string | undefined {
  const hex = rawHex?.trim() ?? '';
  if (!hex || hex.length % 2 !== 0 || !/^[0-9A-Fa-f]+$/.test(hex)) return undefined;
  const bytes = Buffer.from(hex, 'hex');
  if (bytes.length < 2) return undefined;

  const routeType = bytes[0]! & 0x03;
  let offset = 1;
  if (routeType === 0 || routeType === 3) {
    if (bytes.length < offset + 4) return undefined;
    offset += 4;
  }
  if (bytes.length <= offset) return undefined;

  const encodedLength = bytes[offset]!;
  const pathHashCount = encodedLength & 0x3f;
  const pathHashSize = (encodedLength >> 6) + 1;
  if (pathHashSize > 3) return undefined;

  const pathEnd = offset + 1 + pathHashCount * pathHashSize;
  if (bytes.length <= pathEnd) return undefined;

  const hash = createHash('sha256');
  hash.update(Uint8Array.of(bytes[0]! & 0xFC));
  hash.update(bytes.subarray(pathEnd));
  return hash.digest('hex').toUpperCase();
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

async function loadState(client: pg.PoolClient): Promise<State> {
  const result = await client.query<State>(`
    SELECT high_water::text AS high_water,
           last_time::text AS last_time,
           last_observation_id::text AS last_observation_id,
           last_hash,
           last_rx_node_id
      FROM ops.packet_identity_backfill_state WHERE singleton
  `);
  if (!result.rows[0]) throw new Error('packet identity backfill state row is missing');
  return result.rows[0];
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

async function uncompressedChunks(client: pg.PoolClient): Promise<Array<{ schema: string; name: string }>> {
  const result = await client.query<{ schema: string; name: string }>(`
    SELECT chunk_schema AS schema, chunk_name AS name
      FROM timescaledb_information.chunks
     WHERE hypertable_schema = 'mesh' AND hypertable_name = 'packets' AND is_compressed = FALSE
     ORDER BY range_start
  `);
  return result.rows;
}

function quoteIdent(value: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) throw new Error(`refusing unexpected chunk identifier: ${value}`);
  return `"${value}"`;
}

async function packetBatch(
  client: pg.PoolClient,
  state: State,
  chunk: { schema: string; name: string },
  batchSize: number,
  since: string | null,
): Promise<Row[]> {
  if (!state.high_water) return [];
  const values: unknown[] = [state.high_water];
  const where = [
    'packet_uid IS NULL',
    'length(packet_hash) = 64',
    'observation_id IS NOT NULL',
    'time <= $1::timestamptz',
  ];
  if (since) { values.push(since); where.push(`time >= $${values.length}::timestamptz`); }
  if (state.last_time && state.last_observation_id) {
    values.push(state.last_time, state.last_observation_id);
    const p = values.length - 1;
    where.push(`(
      time > $${p}::timestamptz
      OR (time = $${p}::timestamptz AND observation_id > $${p + 1}::uuid)
    )`);
  }
  values.push(batchSize);
  const result = await client.query<Row>(`
    SELECT time::text AS time, packet_hash, rx_node_id,
           observation_id::text AS observation_id, raw_hex, ctid::text AS ctid
      FROM ${quoteIdent(chunk.schema)}.${quoteIdent(chunk.name)}
     WHERE ${where.join(' AND ')}
     ORDER BY time, observation_id
     LIMIT $${values.length}
  `, values);
  return result.rows;
}

export async function backfillPackets(pool: pg.Pool, batchSize: number, dryRun: boolean, limit: number, since: string | null) {
  const client = await pool.connect();
  const total: Counts = { selected: 0, resolved: 0, unresolved: 0, mismatch: 0 };
  let lockHeld = false;
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

    const chunks = await uncompressedChunks(client);
    let partial = false;
    for (const chunk of chunks) {
      while (true) {
        await client.query('BEGIN');
        try {
          const rows = await packetBatch(client, state, chunk, batchSize, since);
          if (rows.length === 0) {
            await client.query('COMMIT');
            break;
          }

          const resolved: Row[] = [];
          let unresolved = 0;
          let mismatch = 0;
          for (const row of rows) {
            const uid = canonicalFromRawHex(row.raw_hex);
            if (!uid) unresolved += 1;
            else if (uid !== row.packet_hash.toUpperCase()) mismatch += 1;
            else resolved.push(row);
          }

          let resolvedCount = dryRun ? resolved.length : 0;
          if (!dryRun && resolved.length > 0) {
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
            `, [
              resolved.map((row) => row.ctid),
              resolved.map((row) => canonicalFromRawHex(row.raw_hex)!),
            ]);
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
            `, [last.time, last.observation_id, resolvedCount, unresolved, mismatch]);
          }
          await client.query('COMMIT');

          total.selected += rows.length;
          total.resolved += resolvedCount;
          total.unresolved += unresolved;
          total.mismatch += mismatch;
          console.log(JSON.stringify({ batch: rows.length, ...total, last: last.time }));
          if (limit > 0 && total.selected >= limit) { partial = true; break; }
          if (dryRun || rows.length < batchSize) break;
          state = await loadState(client);
        } catch (error) {
          await client.query('ROLLBACK');
          throw error;
        }
      }
      if (partial) break;
    }

    if (!dryRun) {
      await client.query(`
        UPDATE ops.packet_identity_backfill_state
           SET status = $1, updated_at = NOW() WHERE singleton
      `, [partial ? 'running' : 'packets_complete']);
    }
    return total;
  } finally {
    if (lockHeld) {
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
