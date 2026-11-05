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
  chunks: Array<{ schema: string; name: string }>;
  batches: Row[][];
  locks: Set<string>;
  legacyObservationId: string | null;
  updateRowCount?: number;
  holdChunks?: Promise<void>;
  onLockAttempt?: (locked: boolean) => void;
  batchQueries: Array<{ sql: string; values: unknown[] }>;
};

const HIGH_WATER = '2026-11-01T00:00:00.000Z';
const CHUNK = { schema: '_timescaledb_internal', name: '_hyper_1_1_chunk' };
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
    },
    chunks: [CHUNK],
    batches: [],
    locks: new Set<string>(),
    legacyObservationId: null,
    batchQueries: [],
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
    if (text.includes('FROM mesh.packets')) {
      return {
        rows: (this.env.legacyObservationId === null
          ? []
          : [{ observation_id: this.env.legacyObservationId }]) as T[],
        rowCount: this.env.legacyObservationId === null ? 0 : 1,
      };
    }
    if (text.includes('ORDER BY time, observation_id')) {
      const batch = this.env.batches.shift() ?? [];
      this.env.batchQueries.push({ sql: text, values });
      return { rows: batch as T[], rowCount: batch.length };
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
    if (text.includes('last_observation_id = $2::uuid')) {
      this.env.state.last_time = values[0] as string;
      this.env.state.last_observation_id = values[1] as string;
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
  const env = makeEnv({ batches: [[resolved, unresolved, mismatch]] });
  const client = new FakeClient(env);

  const result = await backfillPackets(poolOf(client), 500, true, 0, null);

  assert.deepEqual(result, { selected: 3, resolved: 1, unresolved: 1, mismatch: 1 });
  assert.equal(client.queries.some((query) => query.sql.includes('SET packet_uid = decode')), false);
  assert.equal(client.queries.some((query) => query.sql.includes('last_observation_id = $2::uuid')), false);
  assert.equal(client.queries.filter((query) => query.sql.includes('pg_try_advisory_lock')).length, 1);
  assert.equal(client.queries.some((query) => query.sql.includes('pg_advisory_unlock')), true);
  assert.equal(env.locks.size, 0);
  assert.equal(client.released, true);
  assert.equal(client.destroyed, false);
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
    },
    batches: [[row({ ctid: '(0,9)', observation_id: '66666666-6666-4666-8666-666666666666' })]],
  });
  const client = new FakeClient(env);

  const result = await backfillPackets(poolOf(client), 500, false, 0, null);

  assert.equal(result.selected, 1);
  assert.equal(result.resolved, 1);
  const legacyLookup = client.queries.find((query) => query.sql.includes('FROM mesh.packets'));
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
    },
    batches: [[]],
  });
  const client = new FakeClient(env);

  await backfillPackets(poolOf(client), 500, false, 0, null);

  const migrationUpdate = client.queries.find((query) => query.sql.includes('SET last_observation_id = $1::uuid'));
  assert.deepEqual(migrationUpdate?.values, [CURSOR_FLOOR]);
  assert.equal(env.batchQueries[0]?.values.includes(CURSOR_FLOOR), true);
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
