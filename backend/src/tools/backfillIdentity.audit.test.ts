import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import pg from 'pg';
import { loadMigrationFiles } from '../db/migrations.js';
import { backfillPackets } from './backfillPacketIdentity.js';

type StateRow = {
  high_water: string | null;
  last_time: string | null;
  last_observation_id: string | null;
  last_hash: string | null;
  last_rx_node_id: string | null;
  decompressed_chunks: string[];
  status?: string;
};

type ChunkRow = {
  schema: string;
  name: string;
  compressed: boolean;
  range_start: string;
  range_end: string;
};

type Row = {
  time: string;
  packet_hash: string;
  rx_node_id: string | null;
  observation_id: string;
  raw_hex: string | null;
  ctid: string;
};

type Env = {
  state: StateRow;
  chunks: ChunkRow[];
  batches: Row[][];
  locks: Set<string>;
  legacyObservationId: string | null;
  updateRowCount?: number;
  eligible?: number;
  holdChunks?: Promise<void>;
  onLockAttempt?: (locked: boolean) => void;
  batchQueries: Array<{ sql: string; values: unknown[] }>;
  decompressed: Set<string>;
  recompressed: Set<string>;
  failDecompress?: boolean;
};

const HIGH_WATER = '2026-11-01T00:00:00.000Z';
const CHUNK: ChunkRow = {
  schema: '_timescaledb_internal',
  name: '_hyper_1_1_chunk',
  compressed: false,
  range_start: '2026-10-31T00:00:00.000Z',
  range_end: '2026-11-01T00:00:00.000Z',
};
const LEGACY_OBS = '11111111-1111-4111-8111-111111111111';
const CURSOR_FLOOR = '00000000-0000-0000-0000-000000000000';

function makeEnv(overrides: Partial<Env> = {}): Env {
  return {
    state: {
      high_water: HIGH_WATER,
      last_time: null,
      last_observation_id: null,
      last_hash: null,
      last_rx_node_id: null,
      decompressed_chunks: [],
    },
    chunks: [CHUNK],
    batches: [],
    locks: new Set<string>(),
    legacyObservationId: null,
    batchQueries: [],
    decompressed: new Set<string>(),
    recompressed: new Set<string>(),
    ...overrides,
  };
}

class FakeClient {
  readonly queries: Array<{ sql: string; values: unknown[] }> = [];
  released = false;
  destroyed = false;
  failUnlock = false;
  private xactLockKey: string | null = null;

  constructor(private readonly env: Env) {}

  async query<T>(sql: string, values: unknown[] = []): Promise<{ rows: T[]; rowCount: number }> {
    const text = sql.trim();
    this.queries.push({ sql: text, values });

    if (text === 'BEGIN') {
      return { rows: [], rowCount: 0 };
    }
    if (text === 'COMMIT' || text === 'ROLLBACK') {
      if (this.xactLockKey) {
        this.env.locks.delete(this.xactLockKey);
        this.xactLockKey = null;
      }
      return { rows: [], rowCount: 0 };
    }
    // Retained so this regression test can be run against the pre-fix tool,
    // which took a transaction-scoped lock once per batch.
    if (text.includes('pg_try_advisory_xact_lock')) {
      const key = 'meshcore.packet_identity_backfill';
      const locked = !this.env.locks.has(key);
      if (locked) {
        this.env.locks.add(key);
        this.xactLockKey = key;
      }
      return { rows: [{ locked } as T], rowCount: 1 };
    }
    if (text.includes('pg_try_advisory_lock')) {
      const key = String(values[0]);
      const locked = !this.env.locks.has(key);
      if (locked) this.env.locks.add(key);
      this.env.onLockAttempt?.(locked);
      return { rows: [{ locked } as T], rowCount: 1 };
    }
    if (text.includes('pg_advisory_unlock')) {
      if (this.failUnlock) throw new Error('connection lost');
      const key = String(values[0]);
      return { rows: [{ unlocked: this.env.locks.delete(key) } as T], rowCount: 1 };
    }
    if (text.includes('timescaledb_information.chunks')) {
      if (this.env.holdChunks) await this.env.holdChunks;
      if (text.includes('AS key') && text.includes('is_compressed AS compressed')) {
        const rows = this.env.chunks.map((chunk) => {
          const key = `${chunk.schema}.${chunk.name}`;
          return { key, compressed: chunk.compressed || this.env.recompressed.has(key) };
        });
        return { rows: rows as T[], rowCount: rows.length };
      }
      return { rows: this.env.chunks as T[], rowCount: this.env.chunks.length };
    }
    if (text.includes('FROM ops.packet_identity_backfill_state')) {
      return { rows: [{ ...this.env.state } as T], rowCount: 1 };
    }
    if (text.includes('SELECT max(time)')) {
      return { rows: [{ high_water: this.env.state.high_water } as T], rowCount: 1 };
    }
    if (text.includes('decompress_chunk')) {
      const key = `${values[0]}.${values[1]}`;
      if (this.env.failDecompress) throw new Error('chunk is not compressed');
      this.env.decompressed.add(key);
      return { rows: [{ decompress_chunk: key } as T], rowCount: 1 };
    }
    if (text.includes('compress_chunk')) {
      const key = `${values[0]}.${values[1]}`;
      this.env.recompressed.add(key);
      return { rows: [{ compress_chunk: key } as T], rowCount: 1 };
    }
    if (text.includes('count(*)::text AS n')) {
      const remaining = this.env.batches.reduce((total, batch) => total + batch.length, 0);
      return { rows: [{ n: String(this.env.eligible ?? remaining) } as T], rowCount: 1 };
    }
    if (text.includes('ORDER BY time, observation_id')) {
      const batch = this.env.batches.shift() ?? [];
      this.env.batchQueries.push({ sql: text, values });
      return { rows: batch as T[], rowCount: batch.length };
    }
    if (text.includes('FROM mesh.packets')) {
      return {
        rows: (this.env.legacyObservationId === null
          ? []
          : [{ observation_id: this.env.legacyObservationId }]) as T[],
        rowCount: this.env.legacyObservationId === null ? 0 : 1,
      };
    }
    if (text.includes('SET packet_uid = decode')) {
      const uids = (values[1] as string[] | undefined) ?? [];
      return { rows: [], rowCount: this.env.updateRowCount ?? uids.length };
    }
    if (text.includes('SET last_observation_id = $1::uuid')) {
      this.env.state.last_observation_id = values[0] as string;
      this.env.state.last_hash = null;
      this.env.state.last_rx_node_id = null;
      return { rows: [], rowCount: 1 };
    }
    if (text.includes('SET high_water = $1::timestamptz')) {
      this.env.state.high_water = values[0] as string | null;
      return { rows: [], rowCount: 1 };
    }
    if (text.includes('decompressed_chunks = $1::text[]')) {
      this.env.state.decompressed_chunks = (values[0] as string[]) ?? [];
      return { rows: [], rowCount: 1 };
    }
    if (text.includes('chunk_results = $1::jsonb')) {
      return { rows: [], rowCount: 1 };
    }
    if (text.includes('last_observation_id = $2::uuid')) {
      this.env.state.last_time = values[0] as string;
      this.env.state.last_observation_id = values[1] as string;
      return { rows: [], rowCount: 1 };
    }
    if (text.includes("SET status = 'failed'")) {
      this.env.state.status = 'failed';
      return { rows: [], rowCount: 1 };
    }
    if (text.includes('SET status = $1')) {
      return { rows: [], rowCount: 1 };
    }
    throw new Error(`unexpected query: ${text.slice(0, 100)}`);
  }

  release(error?: Error | boolean): void {
    this.released = true;
    if (error) this.destroyed = true;
  }
}

function poolOf(client: FakeClient): Parameters<typeof backfillPackets>[0] {
  return { connect: async () => client } as unknown as Parameters<typeof backfillPackets>[0];
}

// canonicalFromRawHex is intentionally not exported; derive the expected hash
// through the same wire layout: route type 1, zero path hashes, payload byte.
function sha256Hex(rawHex: string): string {
  const bytes = Buffer.from(rawHex, 'hex');
  const hash = createHash('sha256');
  hash.update(Uint8Array.of(bytes[0]! & 0xfc));
  hash.update(bytes.subarray(2));
  return hash.digest('hex').toUpperCase();
}

function row(overrides: Partial<Row> = {}): Row {
  return {
    time: '2026-10-01T00:00:00.000Z',
    packet_hash: sha256Hex('0100AA'),
    rx_node_id: 'RX1',
    observation_id: '22222222-2222-4222-8222-222222222222',
    raw_hex: '0100AA',
    ctid: '(0,1)',
    ...overrides,
  };
}

test('dry run reports the selected rows and the resolved count without writing', async () => {
  const resolved = row();
  const unresolved = row({ ctid: '(0,2)', observation_id: '33333333-3333-4333-8333-333333333333', raw_hex: null });
  const mismatch = row({
    ctid: '(0,3)',
    observation_id: '44444444-4444-4444-8444-444444444444',
    raw_hex: '0100BB',
    packet_hash: sha256Hex('0100CC'),
  });
  const env = makeEnv({ batches: [[resolved, unresolved, mismatch]], eligible: 3 });
  const client = new FakeClient(env);

  const result = await backfillPackets(poolOf(client), 500, true, 0, null);

  assert.equal(result.selected, 3);
  assert.equal(result.resolved, 1);
  assert.equal(result.unresolved, 1);
  assert.equal(result.mismatch, 1);
  assert.deepEqual(result.unresolvedReasons, { missing: 1 });
  assert.equal(result.eligible, 3);
  assert.equal(result.chunksTotal, 1);
  assert.equal(result.chunksComplete, 1);
  assert.equal(client.queries.some((query) => query.sql.includes('SET packet_uid = decode')), false);
  assert.equal(client.queries.some((query) => query.sql.includes('last_observation_id = $2::uuid')), false);
  assert.equal(client.queries.filter((query) => query.sql.includes('pg_try_advisory_lock')).length, 1);
  assert.equal(client.queries.some((query) => query.sql.includes('pg_advisory_unlock')), true);
  assert.equal(env.locks.size, 0);
  assert.equal(client.released, true);
  assert.equal(client.destroyed, false);
});

test('dry run reads every batch of every chunk instead of stopping after one page', async () => {
  const first = row({ ctid: '(0,1)' });
  const second = row({
    ctid: '(1,1)',
    time: '2026-10-02T00:00:00.000Z',
    observation_id: '77777777-7777-4777-8777-777777777777',
  });
  const env = makeEnv({
    chunks: [
      CHUNK,
      { ...CHUNK, name: '_hyper_1_2_chunk', compressed: true },
    ],
    batches: [[first], [second], []],
  });
  const client = new FakeClient(env);

  const result = await backfillPackets(poolOf(client), 1, true, 0, null);

  assert.equal(result.selected, 2, 'a dry run must walk past the first batch of each chunk');
  assert.equal(result.eligible, 2);
  assert.equal(env.decompressed.size, 0, 'a dry run must not decompress');
  assert.equal(client.queries.some((query) => query.sql.includes('decompressed_chunks = $1')), false);
});

test('two concurrent runs cannot both hold the run-scoped advisory lock', async () => {
  const locks = new Set<string>();
  let releaseChunks!: () => void;
  const heldChunks = new Promise<void>((resolve) => { releaseChunks = resolve; });
  let signalLocked!: () => void;
  const firstLocked = new Promise<void>((resolve) => { signalLocked = resolve; });

  const envA = makeEnv({
    locks,
    holdChunks: heldChunks,
    onLockAttempt: (locked) => { if (locked) signalLocked(); },
    batches: [[row()], [row({ ctid: '(0,2)', observation_id: '55555555-5555-4555-8555-555555555555' })], []],
  });
  const clientA = new FakeClient(envA);
  const runA = backfillPackets(poolOf(clientA), 1, false, 0, null);
  await firstLocked;

  const envB = makeEnv({ locks, chunks: [] });
  const clientB = new FakeClient(envB);
  let concurrentError: unknown;
  try {
    const runB = backfillPackets(poolOf(clientB), 1, false, 0, null);
    await assert.rejects(runB, /packet identity backfill is already running/);
  } catch (error) {
    concurrentError = error;
  } finally {
    releaseChunks();
    await runA;
  }
  if (concurrentError) throw concurrentError;
  assert.equal(clientB.queries.some((query) => query.sql === 'BEGIN'), false);
  assert.equal(clientB.released, true);

  const resultA = await runA;

  const lockIndex = clientA.queries.findIndex((query) => query.sql.includes('pg_try_advisory_lock'));
  const firstBegin = clientA.queries.findIndex((query) => query.sql === 'BEGIN');
  assert.ok(lockIndex >= 0 && firstBegin > lockIndex);
  // One lock for the whole run, not one per batch transaction.
  assert.equal(clientA.queries.filter((query) => query.sql.includes('pg_try_advisory_lock')).length, 1);
  assert.equal(clientA.queries.filter((query) => query.sql === 'BEGIN').length, 3);
  assert.equal(resultA.selected, 2);
  assert.equal(locks.size, 0);

  const sequential = new FakeClient(makeEnv({ locks, chunks: [] }));
  const sequentialResult = await backfillPackets(poolOf(sequential), 500, true, 0, null);
  assert.equal(sequentialResult.selected, 0);
  assert.equal(locks.size, 0);
});

test('a failed unlock destroys the session instead of returning a locked connection', async () => {
  const env = makeEnv({ chunks: [] });
  const client = new FakeClient(env);
  client.failUnlock = true;

  const result = await backfillPackets(poolOf(client), 500, true, 0, null);

  assert.equal(result.selected, 0);
  assert.equal(client.destroyed, true);
});

test('a legacy checkpoint resumes from the cursor floor, not its own page key', async () => {
  const legacyHash = sha256Hex('0100DD');
  const env = makeEnv({
    legacyObservationId: LEGACY_OBS,
    state: {
      high_water: HIGH_WATER,
      last_time: '2026-10-01T00:00:00.000Z',
      last_observation_id: null,
      last_hash: legacyHash,
      last_rx_node_id: 'RX1',
      decompressed_chunks: [],
    },
    batches: [[row({ ctid: '(0,9)', observation_id: '66666666-6666-4666-8666-666666666666' })]],
  });
  const client = new FakeClient(env);

  const result = await backfillPackets(poolOf(client), 500, false, 0, null);

  assert.equal(result.selected, 1);
  assert.equal(result.resolved, 1);
  const legacyLookup = client.queries.find(
    (query) => query.sql.includes('FROM mesh.packets') && query.sql.includes('time = $1::timestamptz'),
  );
  assert.equal(legacyLookup, undefined,
    'the legacy hash/receiver page key must not be consulted: it is what skipped same-instant rows');
  const migrationUpdate = client.queries.find((query) => query.sql.includes('SET last_observation_id = $1::uuid'));
  assert.deepEqual(migrationUpdate?.values, [CURSOR_FLOOR],
    'a cursor at the minimum observation id would still skip the row that holds it (the page predicate is exclusive)');
  const migrationIndex = client.queries.indexOf(migrationUpdate!);
  const firstBegin = client.queries.findIndex((query) => query.sql === 'BEGIN');
  assert.ok(migrationIndex >= 0 && migrationIndex < firstBegin);
  assert.equal(env.batchQueries[0]?.values.includes(CURSOR_FLOOR), true);
});

test('a legacy checkpoint with no matching row resumes from the start of its timestamp', async () => {
  const env = makeEnv({
    legacyObservationId: null,
    state: {
      high_water: HIGH_WATER,
      last_time: '2026-10-01T00:00:00.000Z',
      last_observation_id: null,
      last_hash: sha256Hex('0100EE'),
      last_rx_node_id: null,
      decompressed_chunks: [],
    },
    batches: [[]],
    eligible: 1,
  });
  const client = new FakeClient(env);

  await backfillPackets(poolOf(client), 500, false, 0, null);

  const migrationUpdate = client.queries.find((query) => query.sql.includes('SET last_observation_id = $1::uuid'));
  assert.deepEqual(migrationUpdate?.values, [CURSOR_FLOOR]);
  assert.equal(env.batchQueries[0]?.values.includes(CURSOR_FLOOR), true);
});

test('a compressed chunk is decompressed once and recompressed after its rows are processed', async () => {
  const compressedChunk: ChunkRow = { ...CHUNK, compressed: true };
  const env = makeEnv({ chunks: [compressedChunk], batches: [[row()], []] });
  const client = new FakeClient(env);

  const result = await backfillPackets(poolOf(client), 500, false, 0, null);

  assert.equal(result.selected, 1);
  assert.equal(result.resolved, 1);
  assert.deepEqual([...env.decompressed], ['_timescaledb_internal._hyper_1_1_chunk']);
  assert.deepEqual([...env.recompressed], ['_timescaledb_internal._hyper_1_1_chunk']);
  assert.equal(result.chunks[0]?.compressed_at_start, true);
  assert.equal(result.chunks[0]?.recompressed, true);
  assert.equal(result.skippedCompressed, 0);
  assert.equal(result.status, 'packets_complete');
  assert.deepEqual(env.state.decompressed_chunks, [], 'the marker is cleared once the chunk is recompressed');
  const decompressIndex = client.queries.findIndex((query) => query.sql.includes('decompress_chunk'));
  const markerIndex = client.queries.findIndex((query) => query.sql.includes('decompressed_chunks = $1::text[]'));
  const firstBegin = client.queries.findIndex((query) => query.sql === 'BEGIN');
  assert.ok(
    markerIndex >= 0 && decompressIndex > markerIndex && firstBegin > decompressIndex,
    'ownership must be persisted before the chunk is decompressed, and the chunk must be read only afterwards',
  );
});

test('a decompression failure aborts instead of silently skipping the chunk', async () => {
  const env = makeEnv({
    chunks: [{ ...CHUNK, compressed: true }],
    batches: [[row()], []],
    failDecompress: true,
  });
  const client = new FakeClient(env);

  await assert.rejects(
    backfillPackets(poolOf(client), 500, false, 0, null),
    /cannot decompress chunk _timescaledb_internal\._hyper_1_1_chunk: chunk is not compressed/,
  );
  assert.equal(env.decompressed.size, 0);
  assert.equal(client.queries.some((query) => query.sql.includes('ORDER BY time, observation_id')), false,
    'no row may be read from a chunk whose decompression failed');
  assert.equal(env.state.status, 'failed', 'the state row must record the failed phase');
});

test('an unvisited compressed chunk blocks the completion claim', async () => {
  const early: ChunkRow = {
    ...CHUNK,
    name: '_hyper_1_1_chunk',
    range_start: '2026-10-01T00:00:00.000Z',
    range_end: '2026-10-02T00:00:00.000Z',
  };
  const late: ChunkRow = {
    ...CHUNK,
    name: '_hyper_1_2_chunk',
    compressed: true,
    range_start: '2026-10-03T00:00:00.000Z',
    range_end: '2026-10-04T00:00:00.000Z',
  };
  const env = makeEnv({ chunks: [early, late], batches: [[row()]], eligible: 1 });
  const client = new FakeClient(env);

  const result = await backfillPackets(poolOf(client), 10, false, 1, null);

  assert.equal(result.selected, 1);
  assert.equal(result.status, 'packets_partial');
  assert.equal(result.skippedCompressed, 1);
  assert.equal(result.chunksComplete, 1);
  assert.match(result.statusReason ?? '', /_hyper_1_2_chunk/);
  assert.equal(env.decompressed.size, 0, 'a chunk the limit never reached must not be decompressed');
});

test('migration 133 declares and loads the unique (time, observation_id) index', () => {
  const name = '133_packets_observation_id_unique.sql';
  const sql = readFileSync(new URL(`../db/migrations/${name}`, import.meta.url), 'utf8');

  assert.match(
    sql,
    /CREATE UNIQUE INDEX IF NOT EXISTS packets_time_observation_id_uidx\s+ON mesh\.packets \(time, observation_id\)/,
  );
  assert.doesNotMatch(sql, /CREATE INDEX CONCURRENTLY/i);

  const migrations = loadMigrationFiles();
  const migration = migrations.find((entry) => entry.name === name);
  assert.ok(migration, `${name} is not loaded by the migration runner`);
  assert.equal(migration.transactional, true);
  assert.equal(new Set(migrations.map((entry) => entry.name)).size, migrations.length);
});

test('migration 135 widens the status check and adds the per-chunk columns', () => {
  const name = '135_packet_identity_backfill_chunks.sql';
  const sql = readFileSync(new URL(`../db/migrations/${name}`, import.meta.url), 'utf8');
  const migrations = loadMigrationFiles();
  const migration = migrations.find((entry) => entry.name === name);

  assert.ok(migration, `${name} is not loaded by the migration runner`);
  if (!migration) return;
  assert.equal(migration.transactional, true);
  assert.match(sql, /ADD COLUMN IF NOT EXISTS chunk_results JSONB NOT NULL DEFAULT '\[\]'::jsonb/);
  assert.match(sql, /ADD COLUMN IF NOT EXISTS skipped_compressed INTEGER NOT NULL DEFAULT 0/);
  assert.match(sql, /ADD COLUMN IF NOT EXISTS decompressed_chunks TEXT\[\] NOT NULL DEFAULT '\{\}'::text\[\]/);
  assert.match(sql, /'packets_partial'/);
  assert.match(sql, /pg_get_constraintdef\(oid\) LIKE '%packets_complete%'/);
});

/**
 * Migration 133's unique index must be enforced by the database, not just
 * declared in SQL. The migration comment documents a duplicate pre-check; a
 * database that already holds duplicates must be caught before the index is
 * applied, and a database that passes it must reject the next duplicate.
 *
 * Runs against a disposable database on the fixture server so the index is
 * created by the shipped migration file exactly as production applies it.
 */
const fixtureDatabaseUrl = process.env['TEST_BACKFILL_DATABASE_URL'];
const fixtureSkip = fixtureDatabaseUrl
  ? false
  : 'loopback TEST_BACKFILL_DATABASE_URL required for the migration 133 fixture';

test('migration 133 rejects a duplicate (time, observation_id) and reports a valid unique index', {
  skip: fixtureSkip,
  timeout: 120_000,
}, async (t) => {
  assert.ok(fixtureDatabaseUrl, 'TEST_BACKFILL_DATABASE_URL is required');
  const endpoint = new URL(fixtureDatabaseUrl);
  assert.ok(
    ['localhost', '127.0.0.1', '::1', '[::1]'].includes(endpoint.hostname),
    'only an isolated loopback test database is permitted',
  );

  const admin = new pg.Pool({ connectionString: fixtureDatabaseUrl, max: 1 });
  const name = `meshcore_identity_133_${process.pid}_${Date.now()}`;
  await admin.query(`CREATE DATABASE "${name}"`);
  const owned = new URL(fixtureDatabaseUrl);
  owned.pathname = `/${name}`;
  const pool = new pg.Pool({ connectionString: owned.toString(), max: 1 });
  t.after(async () => {
    await pool.end();
    await admin.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
    await admin.end();
  });

  const instant = '2026-11-02T09:00:00.000Z';
  const first = randomUUID();
  const second = randomUUID();
  await pool.query(`
    CREATE SCHEMA mesh;
    CREATE TABLE mesh.packets (
      time TIMESTAMPTZ NOT NULL,
      observation_id UUID
    );
  `);
  await pool.query(
    `INSERT INTO mesh.packets (time, observation_id) VALUES ($1::timestamptz, $2::uuid), ($1::timestamptz, $3::uuid)`,
    [instant, first, second],
  );

  // The exact pre-check the migration tells operators to run. A non-zero
  // result means the index must not be applied: fix the duplicates first.
  const duplicateCheck = `
    SELECT time, observation_id, count(*)
      FROM mesh.packets
     WHERE observation_id IS NOT NULL
     GROUP BY time, observation_id
    HAVING count(*) > 1`;
  const before = await pool.query(duplicateCheck);
  assert.equal(before.rowCount, 0, 'the fixture must be duplicate-free before migration 133 is applied');

  const sql = readFileSync(
    new URL('../db/migrations/133_packets_observation_id_unique.sql', import.meta.url),
    'utf8',
  );
  await pool.query('BEGIN');
  await pool.query(sql);
  await pool.query('COMMIT');

  const catalog = await pool.query<{ indisunique: boolean; indisvalid: boolean; indisready: boolean }>(`
    SELECT i.indisunique, i.indisvalid, i.indisready
      FROM pg_index i
     WHERE i.indexrelid = 'mesh.packets_time_observation_id_uidx'::regclass
  `);
  assert.equal(catalog.rowCount, 1, 'migration 133 must create packets_time_observation_id_uidx');
  assert.deepEqual(catalog.rows[0], { indisunique: true, indisvalid: true, indisready: true });

  await assert.rejects(
    pool.query(
      'INSERT INTO mesh.packets (time, observation_id) VALUES ($1::timestamptz, $2::uuid)',
      [instant, first],
    ),
    (error: unknown) => {
      const pgError = error as { code?: string; constraint?: string };
      assert.equal(pgError.code, '23505', 'a duplicate observation key must raise unique_violation');
      assert.equal(pgError.constraint, 'packets_time_observation_id_uidx');
      return true;
    },
  );

  // Rows with no observation id stay outside the key, as the backfill assumes.
  await pool.query(
    'INSERT INTO mesh.packets (time, observation_id) VALUES ($1::timestamptz, NULL), ($1::timestamptz, NULL)',
    [instant],
  );
});
