import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import test from 'node:test';
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

test('a legacy checkpoint is migrated to the observation cursor and resumes', async () => {
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
    (query) => query.sql.includes('FROM mesh.packets') && query.sql.includes('packet_hash = $2'),
  );
  assert.deepEqual(legacyLookup?.values, ['2026-10-01T00:00:00.000Z', legacyHash, 'RX1']);
  const migrationUpdate = client.queries.find((query) => query.sql.includes('SET last_observation_id = $1::uuid'));
  assert.deepEqual(migrationUpdate?.values, [LEGACY_OBS]);
  const migrationIndex = client.queries.indexOf(migrationUpdate!);
  const firstBegin = client.queries.findIndex((query) => query.sql === 'BEGIN');
  assert.ok(migrationIndex >= 0 && migrationIndex < firstBegin);
  assert.equal(env.batchQueries[0]?.values.includes(LEGACY_OBS), true);
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
    decompressIndex >= 0 && markerIndex > decompressIndex && firstBegin > markerIndex,
    'the decompression must be persisted before the chunk is read',
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
