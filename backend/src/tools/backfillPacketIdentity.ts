import { createHash } from 'node:crypto';
import pg from 'pg';

/**
 * P2-5: resumable identity backfill for historical `mesh.packets` rows.
 *
 * Only rows whose stored 64-char packet_hash is reproducible from retained
 * `raw_hex` are marked resolved. Short/empty legacy hashes are never expanded.
 * One bounded batch is selected, updated and checkpointed in one transaction.
 */

type Row = {
  time: Date;
  packet_hash: string;
  rx_node_id: string | null;
  raw_hex: string | null;
};

type Counts = {
  selected: number;
  resolved: number;
  unresolved: number;
  mismatch: number;
};

type State = {
  high_water: Date | null;
  last_time: Date | null;
  last_hash: string | null;
  last_rx_node_id: string | null;
};

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
  const args = { batchSize: 500, dryRun: false, limit: 0, phase: 'packets' };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--batch-size') args.batchSize = Number(argv[++i]);
    else if (arg === '--limit') args.limit = Number(argv[++i]);
    else if (arg === '--dry-run') args.dryRun = true;
    else if (arg === '--phase') args.phase = argv[++i] ?? 'packets';
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
    SELECT high_water, last_time, last_hash, last_rx_node_id
      FROM ops.packet_identity_backfill_state WHERE singleton
  `);
  if (!result.rows[0]) throw new Error('packet identity backfill state row is missing');
  return result.rows[0];
}

async function packetBatch(client: pg.PoolClient, state: State, batchSize: number): Promise<Row[]> {
  if (!state.high_water) return [];
  const values: unknown[] = [state.high_water];
  const where = ['packet_uid IS NULL', 'length(packet_hash) = 64', 'time <= $1'];
  if (state.last_time && state.last_hash) {
    values.push(state.last_time, state.last_hash, state.last_rx_node_id ?? '');
    where.push(`(
      time > $2
      OR (time = $2 AND packet_hash > $3)
      OR (time = $2 AND packet_hash = $3 AND COALESCE(rx_node_id, '') > $4)
    )`);
  }
  values.push(batchSize);
  const result = await client.query<Row>(`
    SELECT time, packet_hash, rx_node_id, raw_hex
      FROM mesh.packets
     WHERE ${where.join(' AND ')}
     ORDER BY time, packet_hash, COALESCE(rx_node_id, '')
     LIMIT $${values.length}
  `, values);
  return result.rows;
}

async function backfillPackets(pool: pg.Pool, batchSize: number, dryRun: boolean, limit: number) {
  const client = await pool.connect();
  const total: Counts = { selected: 0, resolved: 0, unresolved: 0, mismatch: 0 };
  try {
    let state = await loadState(client);
    if (!state.high_water) {
      const highWater = await client.query<{ high_water: Date | null }>(`
        SELECT max(time) AS high_water FROM mesh.packets
         WHERE packet_uid IS NULL AND length(packet_hash) = 64
      `);
      state = { ...state, high_water: highWater.rows[0]?.high_water ?? null };
      if (!dryRun) {
        await client.query(`
          UPDATE ops.packet_identity_backfill_state
             SET high_water = $1, status = 'running', updated_at = NOW()
           WHERE singleton
        `, [state.high_water]);
      }
    }

    while (true) {
      await client.query('BEGIN');
      try {
        const rows = await packetBatch(client, state, batchSize);
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
          else if (uid !== row.packet_hash) mismatch += 1;
          else resolved.push(row);
        }

        if (!dryRun && resolved.length > 0) {
          await client.query(`
            UPDATE mesh.packets p
               SET packet_uid = decode(v.uid_hex, 'hex'),
                   identity_version = 'wire-sha256-v1',
                   identity_provenance = 'historical-raw-hex-recompute'
              FROM (
                SELECT unnest($1::timestamptz[]) AS time,
                       unnest($2::text[]) AS packet_hash,
                       unnest($3::text[]) AS rx_key,
                       unnest($4::text[]) AS uid_hex
              ) v
             WHERE p.time = v.time
               AND p.packet_hash = v.packet_hash
               AND COALESCE(p.rx_node_id, '') = v.rx_key
               AND p.packet_uid IS NULL
          `, [
            resolved.map((row) => row.time),
            resolved.map((row) => row.packet_hash),
            resolved.map((row) => row.rx_node_id ?? ''),
            resolved.map((row) => canonicalFromRawHex(row.raw_hex)!),
          ]);
        }

        const last = rows[rows.length - 1]!;
        if (!dryRun) {
          await client.query(`
            UPDATE ops.packet_identity_backfill_state
               SET status = 'running',
                   last_time = $1,
                   last_hash = $2,
                   last_rx_node_id = $3,
                   resolved = resolved + $4,
                   unresolved = unresolved + $5,
                   ambiguous = ambiguous + $6,
                   batch_count = batch_count + 1,
                   updated_at = NOW()
             WHERE singleton
          `, [last.time, last.packet_hash, last.rx_node_id, resolved.length, unresolved, mismatch]);
        }
        await client.query('COMMIT');

        total.selected += rows.length;
        total.resolved += resolved.length;
        total.unresolved += unresolved;
        total.mismatch += mismatch;
        console.log(JSON.stringify({ batch: rows.length, ...total, last: last.time }));
        if (limit > 0 && total.selected >= limit) break;
        if (dryRun || rows.length < batchSize) break;
        state = await loadState(client);
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      }
    }

    if (!dryRun) {
      await client.query(`
        UPDATE ops.packet_identity_backfill_state
           SET status = 'packets_complete', updated_at = NOW() WHERE singleton
      `);
    }
    return total;
  } finally {
    client.release();
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.phase !== 'packets') throw new Error('only --phase packets is implemented in this release');
  const pool = new pg.Pool({ connectionString: dsn(), max: 1 });
  try {
    const result = await backfillPackets(pool, args.batchSize, args.dryRun, args.limit);
    console.log(JSON.stringify({ dryRun: args.dryRun, ...result }));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
