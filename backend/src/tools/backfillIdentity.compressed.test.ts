import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import test, { type TestContext } from 'node:test';
import pg from 'pg';

/**
 * Regression tests for the G2 defect: the identity backfill only enumerated
 * `is_compressed = FALSE` chunks, processed 1,860,579 of 3,796,145 eligible
 * rows, and still wrote `packets_complete`.
 *
 * These run against a real TimescaleDB (CI uses
 * `timescale/timescaledb:latest-pg16`): a genuine hypertable with one
 * compressed and one uncompressed chunk. `TEST_BACKFILL_DATABASE_URL` must
 * point at a throwaway loopback database.
 *
 * Revert lines each test is built to catch:
 *  - packetChunks() back to `AND is_compressed = FALSE`          → (a), (b), (d)
 *  - dropping the decompress/recompress calls                     → (a), (e)
 *  - the `dryRun || rows.length < batchSize` early break          → (b)
 *  - `[partial ? 'running' : 'packets_complete']`                  → (d), (e)
 */

const databaseUrl = process.env['TEST_BACKFILL_DATABASE_URL'];
const skip = databaseUrl
  ? false
  : 'real TimescaleDB required: set TEST_BACKFILL_DATABASE_URL to a throwaway loopback database';

const backendRoot = fileURLToPath(new URL('../..', import.meta.url));
const toolPath = fileURLToPath(new URL('./backfillPacketIdentity.ts', import.meta.url));

type Fixture = pg.Pool;

function wireFixture(fill: number): { hash: string; rawHex: string } {
  const body = Buffer.alloc(8, fill);
  const hash = createHash('sha256')
    .update(Buffer.concat([Buffer.from([0x00]), body]))
    .digest('hex')
    .toUpperCase();
  return { hash, rawHex: `0100${body.toString('hex')}` };
}

const FIXTURE_SQL = `
CREATE SCHEMA mesh;
CREATE SCHEMA ops;

CREATE TABLE mesh.packets (
  time TIMESTAMPTZ NOT NULL,
  packet_hash TEXT NOT NULL,
  packet_uid BYTEA,
  observation_id UUID,
  rx_node_id TEXT,
  raw_hex TEXT,
  identity_version TEXT,
  identity_provenance TEXT,
  network TEXT DEFAULT 'ukmesh',
  topic TEXT DEFAULT 'meshcore/EMA/OBS/packets',
  payload JSONB DEFAULT '{}'::jsonb,
  visibility_ok BOOLEAN DEFAULT TRUE
);

-- Mirrors migration 130 + 131 + 134.
CREATE TABLE ops.packet_identity_backfill_state (
  singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
  phase TEXT NOT NULL DEFAULT 'packets',
  status TEXT NOT NULL DEFAULT 'idle'
    CHECK (status IN ('idle', 'running', 'packets_complete', 'packets_partial', 'complete', 'failed')),
  high_water TIMESTAMPTZ,
  last_time TIMESTAMPTZ,
  last_hash TEXT,
  last_rx_node_id TEXT,
  last_observation_id UUID,
  resolved BIGINT NOT NULL DEFAULT 0,
  unresolved BIGINT NOT NULL DEFAULT 0,
  ambiguous BIGINT NOT NULL DEFAULT 0,
  batch_count BIGINT NOT NULL DEFAULT 0,
  chunk_results JSONB NOT NULL DEFAULT '[]'::jsonb,
  skipped_compressed INTEGER NOT NULL DEFAULT 0,
  status_reason TEXT,
  decompressed_chunks TEXT[] NOT NULL DEFAULT '{}'::text[],
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO ops.packet_identity_backfill_state (singleton) VALUES (TRUE);
`;

async function prepare(t: TestContext): Promise<Fixture> {
  assert.ok(databaseUrl, 'TEST_BACKFILL_DATABASE_URL is required');
  const endpoint = new URL(databaseUrl);
  assert.ok(
    ['localhost', '127.0.0.1', '::1', '[::1]'].includes(endpoint.hostname),
    'only an isolated loopback test database is permitted',
  );
  const pool = new pg.Pool({ connectionString: databaseUrl, max: 2 });
  t.after(async () => { await pool.end(); });

  const version = await pool.query<{ extversion: string }>(
    `SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'`,
  );
  assert.ok(version.rows[0], 'TEST_BACKFILL_DATABASE_URL must point at a TimescaleDB database');

  const strays = await pool.query<{ table_schema: string }>(
    `SELECT table_schema FROM information_schema.tables
      WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'mesh', 'ops')
        AND table_schema NOT LIKE 'pg\\_%'
        AND table_schema NOT LIKE '\\_timescaledb%'
        AND table_schema NOT LIKE 'timescaledb\\_%'`,
  );
  assert.equal(strays.rowCount, 0, 'refusing to run: the target database already holds other tables');

  await pool.query(`
    DROP SCHEMA IF EXISTS mesh CASCADE;
    DROP SCHEMA IF EXISTS ops CASCADE;
  `);
  await pool.query(FIXTURE_SQL);
  await pool.query(`SELECT create_hypertable('mesh.packets', 'time', chunk_time_interval => INTERVAL '1 day')`);
  // Production settings from migration 051.
  await pool.query(`
    ALTER TABLE mesh.packets SET (
      timescaledb.compress,
      timescaledb.compress_segmentby = 'network',
      timescaledb.compress_orderby = 'time DESC'
    )
  `);
  return pool;
}

type BackfillSummary = {
  dryRun: boolean;
  selected: number;
  resolved: number;
  unresolved: number;
  mismatch: number;
  unresolvedReasons: Record<string, number>;
  chunks: Array<{
    schema: string;
    name: string;
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
  }>;
  chunksTotal: number;
  chunksComplete: number;
  skippedCompressed: number;
  eligible: number | null;
  status: string | null;
  statusReason: string | null;
};

type BackfillRun = {
  code: number;
  stdout: string;
  stderr: string;
  summary: BackfillSummary | undefined;
};

function runBackfill(args: string[]): Promise<BackfillRun> {
  assert.ok(databaseUrl);
  return new Promise((resolve) => {
    const child = spawn(process.execPath, ['--import', 'tsx', toolPath, ...args], {
      cwd: backendRoot,
      env: { ...process.env, BACKFILL_DATABASE_URL: databaseUrl },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    const timer = setTimeout(() => child.kill('SIGKILL'), 120_000);
    child.stdout.on('data', (chunk: Buffer) => { stdout += chunk.toString(); });
    child.stderr.on('data', (chunk: Buffer) => { stderr += chunk.toString(); });
    child.on('close', (code) => {
      clearTimeout(timer);
      const summary = stdout
        .split('\n')
        .map((line) => line.trim())
        .filter((line) => line.startsWith('{'))
        .map((line) => {
          try { return JSON.parse(line) as BackfillSummary; } catch { return undefined; }
        })
        .filter((value): value is BackfillSummary => value !== undefined && typeof value.dryRun === 'boolean')
        .pop();
      resolve({ code: code ?? -1, stdout, stderr, summary });
    });
  });
}

async function insertPacket(
  pool: Fixture,
  row: { time: string; hash: string; rawHex: string | null; rxNodeId?: string },
): Promise<void> {
  await pool.query(
    `INSERT INTO mesh.packets (time, packet_hash, packet_uid, observation_id, rx_node_id, raw_hex)
     VALUES ($1::timestamptz, $2, NULL, $3::uuid, $4, $5)`,
    [row.time, row.hash, randomUUID(), row.rxNodeId ?? 'AA'.repeat(32), row.rawHex],
  );
}

/** Compress the chunk that starts at the supplied day. */
async function compressChunkStartingAt(pool: Fixture, day: string): Promise<void> {
  const result = await pool.query<{ compressed: boolean }>(`
    SELECT compress_chunk(format('%I.%I', chunk_schema, chunk_name)::regclass)::text AS compressed
      FROM timescaledb_information.chunks
     WHERE hypertable_name = 'packets' AND range_start = $1::timestamptz
  `, [day]);
  assert.equal(result.rowCount, 1, `expected one chunk starting at ${day}`);
}

async function chunkStates(pool: Fixture): Promise<Array<{ name: string; compressed: boolean }>> {
  const result = await pool.query<{ name: string; compressed: boolean }>(`
    SELECT chunk_name AS name, is_compressed AS compressed
      FROM timescaledb_information.chunks
     WHERE hypertable_name = 'packets'
     ORDER BY range_start
  `);
  return result.rows;
}

async function resetForRerun(pool: Fixture): Promise<void> {
  await pool.query(`
    UPDATE mesh.packets SET packet_uid = NULL, identity_version = NULL, identity_provenance = NULL;
    UPDATE ops.packet_identity_backfill_state
       SET status = 'idle', high_water = NULL, last_time = NULL, last_observation_id = NULL,
           last_hash = NULL, last_rx_node_id = NULL, resolved = 0, unresolved = 0,
           ambiguous = 0, batch_count = 0, chunk_results = '[]'::jsonb,
           skipped_compressed = 0, status_reason = NULL, decompressed_chunks = '{}'::text[];
  `);
}

async function countEligible(pool: Fixture): Promise<number> {
  const result = await pool.query<{ n: string }>(`
    SELECT count(*)::text AS n FROM mesh.packets
     WHERE packet_uid IS NULL AND length(packet_hash) = 64 AND observation_id IS NOT NULL
  `);
  return Number(result.rows[0]!.n);
}

/**
 * (a) Rows in a compressed chunk and in an uncompressed chunk are both
 * resolved, and the compressed chunk is compressed again afterwards.
 */
test(
  'identity backfill resolves rows in a compressed chunk and recompresses it',
  { skip, timeout: 240_000 },
  async (t) => {
    const pool = await prepare(t);
    const a = wireFixture(0x41);
    const b = wireFixture(0x42);
    const c = wireFixture(0x43);
    const d = wireFixture(0x44);
    const e = wireFixture(0x45);
    const f = wireFixture(0x46);

    // Older chunk (will be compressed) and the current, uncompressed chunk.
    await insertPacket(pool, { time: '2026-01-01 00:00:01+00', hash: a.hash, rawHex: a.rawHex });
    await insertPacket(pool, { time: '2026-01-01 00:00:02+00', hash: b.hash, rawHex: b.rawHex });
    await insertPacket(pool, { time: '2026-01-01 00:00:03+00', hash: c.hash, rawHex: c.rawHex });
    await insertPacket(pool, { time: '2026-01-03 00:00:01+00', hash: d.hash, rawHex: d.rawHex });
    await insertPacket(pool, { time: '2026-01-03 00:00:02+00', hash: e.hash, rawHex: e.rawHex });
    await insertPacket(pool, { time: '2026-01-03 00:00:03+00', hash: f.hash, rawHex: f.rawHex });
    await compressChunkStartingAt(pool, '2026-01-01 00:00:00+00');

    const before = await chunkStates(pool);
    assert.deepEqual(before.map((chunk) => chunk.compressed), [true, false], 'fixture: one compressed, one not');

    const run = await runBackfill(['--batch-size', '2']);
    assert.equal(run.code, 0, run.stderr);
    assert.ok(run.summary, run.stdout);
    assert.equal(run.summary.selected, 6);
    assert.equal(run.summary.resolved, 6);
    assert.equal(run.summary.unresolved, 0);
    assert.equal(run.summary.chunksTotal, 2, 'every chunk must be enumerated');
    assert.equal(run.summary.chunksComplete, 2);
    assert.equal(run.summary.skippedCompressed, 0);
    assert.equal(run.summary.status, 'packets_complete');
    const compressedResult = run.summary.chunks.find((chunk) => chunk.compressed_at_start);
    assert.ok(compressedResult, 'the compressed chunk must appear in the per-chunk results');
    assert.equal(compressedResult.resolved, 3);
    assert.equal(compressedResult.recompressed, true);

    const written = await pool.query<{ n: string }>(
      `SELECT count(*)::text AS n FROM mesh.packets WHERE packet_uid IS NOT NULL`,
    );
    assert.equal(written.rows[0]!.n, '6', 'rows in both chunks must hold a UID');

    const after = await chunkStates(pool);
    assert.deepEqual(after.map((chunk) => chunk.compressed), [true, false], 'the backfill must recompress what it decompressed');

    const state = await pool.query<{
      status: string; skipped_compressed: number; chunk_results: unknown[]; decompressed_chunks: string[];
    }>(`
      SELECT status, skipped_compressed, chunk_results, decompressed_chunks
        FROM ops.packet_identity_backfill_state
    `);
    assert.equal(state.rows[0]!.status, 'packets_complete');
    assert.equal(state.rows[0]!.skipped_compressed, 0);
    assert.equal(state.rows[0]!.chunk_results.length, 2, 'the state row must carry per-chunk results');
    assert.deepEqual(state.rows[0]!.decompressed_chunks, [], 'no decompression may be left owned');
  },
);

/**
 * (b) A dry run counts every batch in every chunk and matches the eligible
 * denominator, without writing state and without decompressing.
 */
test(
  'dry run counts rows in compressed chunks and matches the eligible denominator',
  { skip, timeout: 240_000 },
  async (t) => {
    const pool = await prepare(t);
    const rows = [0x51, 0x52, 0x53, 0x54, 0x55, 0x56].map(wireFixture);
    for (let i = 0; i < 3; i += 1) {
      await insertPacket(pool, { time: `2026-02-01 00:00:0${i + 1}+00`, hash: rows[i]!.hash, rawHex: rows[i]!.rawHex });
    }
    for (let i = 3; i < 6; i += 1) {
      await insertPacket(pool, { time: `2026-02-03 00:00:0${i - 2}+00`, hash: rows[i]!.hash, rawHex: rows[i]!.rawHex });
    }
    await compressChunkStartingAt(pool, '2026-02-01 00:00:00+00');
    const eligible = await countEligible(pool);
    assert.equal(eligible, 6);

    const before = await pool.query<{ status: string; high_water: string | null; batch_count: string }>(`
      SELECT status, high_water::text AS high_water, batch_count::text AS batch_count
        FROM ops.packet_identity_backfill_state
    `);

    // batch-size 1 forces one batch per row: the pre-fix tool stopped after the
    // first batch of each chunk and reported 2.
    const run = await runBackfill(['--dry-run', '--batch-size', '1']);
    assert.equal(run.code, 0, run.stderr);
    assert.ok(run.summary, run.stdout);
    assert.equal(run.summary.selected, 6, 'a dry run must visit every batch of every chunk');
    assert.equal(run.summary.eligible, eligible, 'the dry run must report its denominator');
    assert.equal(run.summary.resolved, 6);
    assert.equal(run.summary.chunksTotal, 2);
    assert.equal(run.summary.chunksComplete, 2);

    const after = await pool.query<{ status: string; high_water: string | null; batch_count: string }>(`
      SELECT status, high_water::text AS high_water, batch_count::text AS batch_count
        FROM ops.packet_identity_backfill_state
    `);
    assert.deepEqual(after.rows[0], before.rows[0], 'a dry run must not write state');

    const untouched = await chunkStates(pool);
    assert.deepEqual(untouched.map((chunk) => chunk.compressed), [true, false], 'a dry run must not decompress');
    const stillNull = await pool.query<{ n: string }>(
      `SELECT count(*)::text AS n FROM mesh.packets WHERE packet_uid IS NULL`,
    );
    assert.equal(stillNull.rows[0]!.n, '6', 'a dry run must not write packet_uid');
  },
);

/**
 * (c) A corrupt row is reported as unresolved with a reason and does not abort
 * the run.
 */
test(
  'an unresolvable row is reported with a reason and does not abort the run',
  { skip, timeout: 240_000 },
  async (t) => {
    const pool = await prepare(t);
    const good = wireFixture(0x61);
    const badHex = wireFixture(0x62);
    const truncated = wireFixture(0x63);

    await insertPacket(pool, { time: '2026-03-01 00:00:01+00', hash: good.hash, rawHex: good.rawHex });
    await insertPacket(pool, { time: '2026-03-01 00:00:02+00', hash: badHex.hash, rawHex: 'zz' });
    await insertPacket(pool, { time: '2026-03-03 00:00:01+00', hash: truncated.hash, rawHex: '0100' });
    await compressChunkStartingAt(pool, '2026-03-01 00:00:00+00');

    const run = await runBackfill(['--batch-size', '10']);
    assert.equal(run.code, 0, run.stderr);
    assert.ok(run.summary, run.stdout);
    assert.equal(run.summary.selected, 3);
    assert.equal(run.summary.resolved, 1);
    assert.equal(run.summary.unresolved, 2);
    assert.deepEqual(run.summary.unresolvedReasons, { 'invalid-hex': 1, 'malformed-header': 1 });
    assert.equal(run.summary.status, 'packets_complete');
    assert.deepEqual(run.summary.chunks[0]?.unresolved_reasons, { 'invalid-hex': 1 },
      'the compressed chunk reports its own reason');
    assert.deepEqual(run.summary.chunks[1]?.unresolved_reasons, { 'malformed-header': 1 },
      'per-chunk reasons are not flattened into the first chunk');

    const state = await pool.query<{ unresolved: string; status: string }>(`
      SELECT unresolved::text AS unresolved, status FROM ops.packet_identity_backfill_state
    `);
    assert.equal(state.rows[0]!.unresolved, '2');
    assert.equal(state.rows[0]!.status, 'packets_complete');
  },
);

/**
 * (d) `packets_complete` is only claimed when no compressed chunk was skipped.
 * A `--limit` stop before the compressed chunk leaves `packets_partial`.
 */
test(
  'packets_complete is not written while a compressed chunk is unvisited',
  { skip, timeout: 240_000 },
  async (t) => {
    const pool = await prepare(t);
    const first = wireFixture(0x71);
    const later = wireFixture(0x72);

    await insertPacket(pool, { time: '2026-04-01 00:00:01+00', hash: first.hash, rawHex: first.rawHex });
    await insertPacket(pool, { time: '2026-04-03 00:00:01+00', hash: later.hash, rawHex: later.rawHex });
    await compressChunkStartingAt(pool, '2026-04-03 00:00:00+00');

    const limited = await runBackfill(['--batch-size', '10', '--limit', '1']);
    assert.equal(limited.code, 0, limited.stderr);
    assert.ok(limited.summary, limited.stdout);
    assert.equal(limited.summary.selected, 1, '--limit 1 stops after the first row');
    assert.equal(limited.summary.status, 'packets_partial');
    assert.equal(limited.summary.skippedCompressed, 1);
    assert.equal(limited.summary.chunksComplete, 1);
    assert.match(limited.summary.statusReason ?? '', /_hyper_\d+_\d+_chunk/);

    const limitedState = await pool.query<{ status: string; skipped_compressed: number; status_reason: string }>(`
      SELECT status, skipped_compressed, status_reason FROM ops.packet_identity_backfill_state
    `);
    assert.equal(limitedState.rows[0]!.status, 'packets_partial');
    assert.equal(limitedState.rows[0]!.skipped_compressed, 1);
    assert.match(limitedState.rows[0]!.status_reason, /unvisited/);

    const compressedAfterLimit = await chunkStates(pool);
    assert.deepEqual(compressedAfterLimit.map((chunk) => chunk.compressed), [false, true],
      'a chunk the limit never reached must not be left decompressed');

    const resumed = await runBackfill(['--batch-size', '10']);
    assert.equal(resumed.code, 0, resumed.stderr);
    assert.ok(resumed.summary, resumed.stdout);
    assert.equal(resumed.summary.selected, 1, 'the resumed run picks up the remaining row');
    assert.equal(resumed.summary.resolved, 1);
    assert.equal(resumed.summary.skippedCompressed, 0);
    assert.equal(resumed.summary.status, 'packets_complete', 'only a fully visited table may claim completion');

    const finalState = await pool.query<{ status: string; skipped_compressed: number; status_reason: string | null }>(`
      SELECT status, skipped_compressed, status_reason FROM ops.packet_identity_backfill_state
    `);
    assert.equal(finalState.rows[0]!.status, 'packets_complete');
    assert.equal(finalState.rows[0]!.skipped_compressed, 0);
    assert.equal(finalState.rows[0]!.status_reason, null);
  },
);

/**
 * (e) A run that stops inside a compressed chunk still recompresses it in
 * `finally`, and leaves no decompression marker behind.
 */
test(
  'a --limit stop inside a compressed chunk recompresses it and clears the marker',
  { skip, timeout: 240_000 },
  async (t) => {
    const pool = await prepare(t);
    const rows = [0x81, 0x82, 0x83, 0x84].map(wireFixture);
    for (let i = 0; i < rows.length; i += 1) {
      await insertPacket(pool, { time: `2026-05-01 00:00:0${i + 1}+00`, hash: rows[i]!.hash, rawHex: rows[i]!.rawHex });
    }
    await compressChunkStartingAt(pool, '2026-05-01 00:00:00+00');

    const run = await runBackfill(['--batch-size', '1', '--limit', '1']);
    assert.equal(run.code, 0, run.stderr);
    assert.ok(run.summary, run.stdout);
    assert.equal(run.summary.selected, 1);
    assert.equal(run.summary.status, 'packets_partial');
    assert.equal(run.summary.skippedCompressed, 1);
    assert.equal(run.summary.chunks[0]?.complete, false);

    const chunks = await chunkStates(pool);
    assert.equal(chunks[0]?.compressed, true, 'finally must recompress a partially processed chunk');
    const state = await pool.query<{ decompressed_chunks: string[]; status: string }>(`
      SELECT decompressed_chunks, status FROM ops.packet_identity_backfill_state
    `);
    assert.deepEqual(state.rows[0]!.decompressed_chunks, [], 'the decompression marker must be cleared');
    assert.equal(state.rows[0]!.status, 'packets_partial');
  },
);

/**
 * (g) The exact G2 recovery path: a checkpoint written by the old tool sits at
 * the end of the uncompressed range and the compressed chunk still holds
 * unresolved rows. The cursor must not be trusted to mean "already visited".
 */
test(
  'a checkpoint written over skipped compressed chunks does not hide them',
  { skip, timeout: 240_000 },
  async (t) => {
    const pool = await prepare(t);
    const old = [0xa1, 0xa2].map(wireFixture);
    const recent = wireFixture(0xa3);

    await insertPacket(pool, { time: '2026-08-01 00:00:01+00', hash: old[0]!.hash, rawHex: old[0]!.rawHex });
    await insertPacket(pool, { time: '2026-08-01 00:00:02+00', hash: old[1]!.hash, rawHex: old[1]!.rawHex });
    await insertPacket(pool, { time: '2026-08-03 00:00:01+00', hash: recent.hash, rawHex: recent.rawHex });
    await compressChunkStartingAt(pool, '2026-08-01 00:00:00+00');

    // Reproduce the G2 state: the uncompressed chunk was processed, the cursor
    // sits at its last row, and no per-chunk evidence was ever recorded.
    await pool.query(`
      UPDATE mesh.packets SET packet_uid = decode($1, 'hex'), identity_version = 'wire-sha256-v1'
       WHERE packet_hash = $2
    `, [recent.hash, recent.hash]);
    await pool.query(`
      UPDATE ops.packet_identity_backfill_state
         SET status = 'packets_complete',
             high_water = '2026-08-03 00:00:01+00'::timestamptz,
             last_time = '2026-08-03 00:00:01+00'::timestamptz,
             last_observation_id = (SELECT observation_id FROM mesh.packets WHERE packet_hash = $1),
             resolved = 1, batch_count = 1
    `, [recent.hash]);

    const run = await runBackfill(['--batch-size', '10']);
    assert.equal(run.code, 0, run.stderr);
    assert.ok(run.summary, run.stdout);
    assert.equal(run.summary.selected, 2, 'the compressed chunk must be re-read despite the cursor');
    assert.equal(run.summary.resolved, 2);
    assert.equal(run.summary.skippedCompressed, 0);
    assert.equal(run.summary.status, 'packets_complete');
    assert.equal(run.summary.chunks[0]?.note, 'cursor-does-not-cover-chunk');

    const unresolved = await pool.query<{ n: string }>(
      `SELECT count(*)::text AS n FROM mesh.packets WHERE packet_uid IS NULL`,
    );
    assert.equal(unresolved.rows[0]!.n, '0', 'no row may be left behind by an untrusted cursor');
    const chunks = await chunkStates(pool);
    assert.deepEqual(chunks.map((chunk) => chunk.compressed), [true, false]);
  },
);

/**
 * (f) A resumed run adopts a decompression a crashed predecessor left behind
 * instead of decompressing it twice, and recompresses it at the end.
 */
test(
  'a resumed run adopts a previously decompressed chunk and recompresses it',
  { skip, timeout: 240_000 },
  async (t) => {
    const pool = await prepare(t);
    const rows = [0x91, 0x92].map(wireFixture);
    await insertPacket(pool, { time: '2026-06-01 00:00:01+00', hash: rows[0]!.hash, rawHex: rows[0]!.rawHex });
    await insertPacket(pool, { time: '2026-06-01 00:00:02+00', hash: rows[1]!.hash, rawHex: rows[1]!.rawHex });
    await compressChunkStartingAt(pool, '2026-06-01 00:00:00+00');

    // Simulate a crash: the chunk is decompressed and the marker is persisted
    // before the process died.
    const chunk = await pool.query<{ key: string }>(`
      SELECT format('%I.%I', chunk_schema, chunk_name) AS key
        FROM timescaledb_information.chunks WHERE hypertable_name = 'packets'
    `);
    const key = chunk.rows[0]!.key;
    await pool.query(`SELECT decompress_chunk($1::regclass)`, [key]);
    await pool.query(
      `UPDATE ops.packet_identity_backfill_state SET decompressed_chunks = ARRAY[$1]::text[]`,
      [key],
    );

    const run = await runBackfill(['--batch-size', '10']);
    assert.equal(run.code, 0, run.stderr);
    assert.ok(run.summary, run.stdout);
    assert.equal(run.summary.resolved, 2);
    assert.equal(run.summary.status, 'packets_complete');
    assert.equal(run.summary.chunks[0]?.decompressed_previously, true);
    assert.equal(run.summary.chunks[0]?.decompressed_by_run, false, 'a resumed run must not decompress twice');
    assert.equal(run.summary.chunks[0]?.recompressed, true);

    const chunks = await chunkStates(pool);
    assert.equal(chunks[0]?.compressed, true);
  },
);
