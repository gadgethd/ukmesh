import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import test, { type TestContext } from 'node:test';
import pg from 'pg';
import { createDisposableDatabase } from '../db/fixtures/disposableDatabase.js';
import { backfillPackets, type BackfillEvent } from './backfillPacketIdentity.js';

/**
 * Regression tests for the G2 defect: the identity backfill only enumerated
 * `is_compressed = FALSE` chunks, processed 1,860,579 of 3,796,145 eligible
 * rows, and still wrote `packets_complete`.
 *
 * These run against a genuine TimescaleDB hypertable with one compressed and
 * one uncompressed chunk. `TEST_BACKFILL_DATABASE_URL` identifies a local
 * Timescale server on which the fixture can create disposable databases.
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
  : 'real TimescaleDB required: set TEST_BACKFILL_DATABASE_URL to a loopback Timescale server';

const backendRoot = fileURLToPath(new URL('../..', import.meta.url));
const toolPath = fileURLToPath(new URL('./backfillPacketIdentity.ts', import.meta.url));

type Fixture = pg.Pool & { fixtureUrl: string };

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
  const fixture = await createDisposableDatabase(t, databaseUrl, 'identity_compressed');
  const pool = Object.assign(new pg.Pool({ connectionString: fixture.url, max: 2 }), {
    fixtureUrl: fixture.url,
  });
  fixture.addCleanup(() => pool.end());

  await pool.query('CREATE EXTENSION IF NOT EXISTS timescaledb');
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

function spawnBackfill(pool: Fixture, args: string[]): { child: ReturnType<typeof spawn>; done: Promise<BackfillRun> } {
  const child = spawn(process.execPath, ['--import', 'tsx', toolPath, ...args], {
    cwd: backendRoot,
    env: { ...process.env, BACKFILL_DATABASE_URL: pool.fixtureUrl },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  const timer = setTimeout(() => child.kill('SIGKILL'), 120_000);
  child.stdout.on('data', (chunk: Buffer) => { stdout += chunk.toString(); });
  child.stderr.on('data', (chunk: Buffer) => { stderr += chunk.toString(); });
  const done = new Promise<BackfillRun>((resolve) => {
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
  return { child, done };
}

function runBackfill(pool: Fixture, args: string[]): Promise<BackfillRun> {
  return spawnBackfill(pool, args).done;
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

    const run = await runBackfill(pool, ['--batch-size', '2']);
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
    const run = await runBackfill(pool, ['--dry-run', '--batch-size', '1']);
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

    const run = await runBackfill(pool, ['--batch-size', '10']);
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

    const limited = await runBackfill(pool, ['--batch-size', '10', '--limit', '1']);
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

    const resumed = await runBackfill(pool, ['--batch-size', '10']);
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

    const run = await runBackfill(pool, ['--batch-size', '1', '--limit', '1']);
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

    const run = await runBackfill(pool, ['--batch-size', '10']);
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

    const run = await runBackfill(pool, ['--batch-size', '10']);
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

async function stateRow(pool: Fixture): Promise<{
  status: string;
  status_reason: string | null;
  high_water: string | null;
  decompressed_chunks: string[];
}> {
  const result = await pool.query(`
    SELECT status, status_reason, high_water::text AS high_water, decompressed_chunks
      FROM ops.packet_identity_backfill_state
  `);
  return result.rows[0]!;
}

async function chunkKeys(pool: Fixture): Promise<string[]> {
  const result = await pool.query<{ key: string }>(`
    SELECT chunk_schema || '.' || chunk_name AS key
      FROM timescaledb_information.chunks
     WHERE hypertable_name = 'packets'
     ORDER BY range_start
  `);
  return result.rows.map((row) => row.key);
}

/**
 * A pre-existing non-NULL high_water used to be applied forever: rows newer
 * than it were invisible while the run still reported `packets_complete`.
 * The run snapshot must replace it every time.
 */
test(
  'a stale non-NULL high_water does not hide later eligible rows',
  { skip, timeout: 240_000 },
  async (t) => {
    const pool = await prepare(t);
    const old = wireFixture(0xc1);
    const recent = wireFixture(0xc2);
    await insertPacket(pool, { time: '2026-09-01 00:00:01+00', hash: old.hash, rawHex: old.rawHex });
    await insertPacket(pool, { time: '2026-09-03 00:00:01+00', hash: recent.hash, rawHex: recent.rawHex });
    await compressChunkStartingAt(pool, '2026-09-01 00:00:00+00');

    await pool.query(`
      UPDATE ops.packet_identity_backfill_state
         SET status = 'packets_complete', high_water = '2026-09-01 00:00:00+00'::timestamptz,
             last_time = NULL, last_observation_id = NULL
    `);

    const run = await runBackfill(pool, ['--batch-size', '10']);
    assert.equal(run.code, 0, run.stderr);
    assert.ok(run.summary, run.stdout);
    assert.equal(run.summary.selected, 2, 'the row above the stale high_water must be selected');
    assert.equal(run.summary.resolved, 2);
    assert.equal(run.summary.status, 'packets_complete');

    const state = await stateRow(pool);
    assert.equal(state.status, 'packets_complete');
    const advanced = await pool.query<{ same: boolean }>(
      `SELECT high_water = '2026-09-03 00:00:01+00'::timestamptz AS same
         FROM ops.packet_identity_backfill_state`,
    );
    assert.equal(advanced.rows[0]!.same, true, 'the watermark must advance to this run snapshot');
    const unresolved = await pool.query<{ n: string }>(
      'SELECT count(*)::text AS n FROM mesh.packets WHERE packet_uid IS NULL',
    );
    assert.equal(unresolved.rows[0]!.n, '0');
  },
);

/**
 * A resumed cursor must still see an eligible row that arrived after the
 * checkpoint and after the previous high_water.
 */
test(
  'a resumed cursor does not hide a late eligible row above the old watermark',
  { skip, timeout: 240_000 },
  async (t) => {
    const pool = await prepare(t);
    const a = wireFixture(0xd1);
    const b = wireFixture(0xd2);
    const c = wireFixture(0xd3);
    const late = wireFixture(0xd4);
    await insertPacket(pool, { time: '2026-10-01 00:00:01+00', hash: a.hash, rawHex: a.rawHex });
    await insertPacket(pool, { time: '2026-10-01 00:00:02+00', hash: b.hash, rawHex: b.rawHex });
    await insertPacket(pool, { time: '2026-10-03 00:00:01+00', hash: c.hash, rawHex: c.rawHex });
    await compressChunkStartingAt(pool, '2026-10-01 00:00:00+00');

    // The first two rows are already resolved; the checkpoint sits at the last
    // of them and the old watermark equals the checkpoint instant.
    await pool.query(`
      UPDATE mesh.packets SET packet_uid = decode(packet_hash, 'hex')
       WHERE time <= '2026-10-01 00:00:02+00'::timestamptz
    `);
    const checkpoint = await pool.query<{ time: string; observation_id: string }>(`
      SELECT time::text AS time, observation_id::text AS observation_id
        FROM mesh.packets WHERE packet_hash = $1
    `, [b.hash]);
    await pool.query(`
      UPDATE ops.packet_identity_backfill_state
         SET status = 'packets_complete', high_water = $1::timestamptz,
             last_time = $1::timestamptz, last_observation_id = $2::uuid
    `, [checkpoint.rows[0]!.time, checkpoint.rows[0]!.observation_id]);

    // Arrives after the checkpoint and above the old high_water.
    await insertPacket(pool, { time: '2026-10-04 00:00:01+00', hash: late.hash, rawHex: late.rawHex });

    const run = await runBackfill(pool, ['--batch-size', '10']);
    assert.equal(run.code, 0, run.stderr);
    assert.ok(run.summary, run.stdout);
    assert.equal(run.summary.selected, 2, 'both the unseen row and the late row must be selected');
    assert.equal(run.summary.resolved, 2);
    assert.equal(run.summary.status, 'packets_complete');
    const advanced = await pool.query<{ same: boolean }>(
      `SELECT high_water = '2026-10-04 00:00:01+00'::timestamptz AS same
         FROM ops.packet_identity_backfill_state`,
    );
    assert.equal(advanced.rows[0]!.same, true);
    const unresolved = await pool.query<{ n: string }>(
      'SELECT count(*)::text AS n FROM mesh.packets WHERE packet_uid IS NULL',
    );
    assert.equal(unresolved.rows[0]!.n, '0');
  },
);

/**
 * The crash window: ownership must be durable before `decompress_chunk`
 * commits, so a process that dies immediately after decompression is still
 * recoverable by the next run.
 */
test(
  'decompression ownership is persisted before the chunk is decompressed',
  { skip, timeout: 240_000 },
  async (t) => {
    const pool = await prepare(t);
    const rows = [0xe1, 0xe2].map(wireFixture);
    for (let index = 0; index < rows.length; index += 1) {
      await insertPacket(pool, {
        time: `2026-11-01 00:00:0${index + 1}+00`, hash: rows[index]!.hash, rawHex: rows[index]!.rawHex,
      });
    }
    await compressChunkStartingAt(pool, '2026-11-01 00:00:00+00');

    const events: BackfillEvent[] = [];
    const result = await backfillPackets(pool, 10, false, 0, null, {
      onEvent: (event) => { events.push(event); },
    });
    assert.equal(result.status, 'packets_complete');
    const persisted = events.findIndex(
      (event) => event.type === 'persist-decompressed' && event.phase === 'decompress-intent',
    );
    const decompressed = events.findIndex((event) => event.type === 'decompress');
    assert.ok(persisted >= 0, 'the run must announce the decompression intent');
    assert.ok(decompressed > persisted, 'the marker must be persisted before the chunk is decompressed');

    const chunks = await chunkStates(pool);
    assert.equal(chunks[0]?.compressed, true, 'the chunk must be compressed again');
    const state = await stateRow(pool);
    assert.deepEqual(state.decompressed_chunks, []);
  },
);

/** A marker write failure must abort before the chunk is modified. */
test(
  'a decompression-ownership persist failure leaves the chunk compressed and fails the run',
  { skip, timeout: 240_000 },
  async (t) => {
    const pool = await prepare(t);
    const row = wireFixture(0xf1);
    await insertPacket(pool, { time: '2026-12-01 00:00:01+00', hash: row.hash, rawHex: row.rawHex });
    await compressChunkStartingAt(pool, '2026-12-01 00:00:00+00');

    await assert.rejects(
      backfillPackets(pool, 10, false, 0, null, {
        onEvent: (event) => {
          if (event.type === 'persist-decompressed' && event.phase === 'decompress-intent') {
            throw new Error('injected marker write failure');
          }
        },
      }),
      /cannot record decompression ownership/,
    );

    const chunks = await chunkStates(pool);
    assert.equal(chunks[0]?.compressed, true, 'the chunk must not be decompressed without a durable marker');
    const state = await stateRow(pool);
    assert.equal(state.status, 'failed');
    assert.match(state.status_reason ?? '', /cannot record decompression ownership/);
  },
);

/** A per-chunk recompression failure must not be swallowed. */
test(
  'a per-chunk recompress failure fails the run instead of claiming completion',
  { skip, timeout: 240_000 },
  async (t) => {
    const pool = await prepare(t);
    const rows = [0x101, 0x102].map(wireFixture);
    for (let index = 0; index < rows.length; index += 1) {
      await insertPacket(pool, {
        time: `2027-01-01 00:00:0${index + 1}+00`, hash: rows[index]!.hash, rawHex: rows[index]!.rawHex,
      });
    }
    await compressChunkStartingAt(pool, '2027-01-01 00:00:00+00');

    let injected = false;
    await assert.rejects(
      backfillPackets(pool, 10, false, 0, null, {
        onEvent: (event) => {
          if (!injected && event.type === 'recompress' && event.phase === 'per-chunk') {
            injected = true;
            throw new Error('injected per-chunk recompress failure');
          }
        },
      }),
      /cannot recompress chunk .*Aborting so completion is not claimed/,
    );

    const state = await stateRow(pool);
    assert.equal(state.status, 'failed', 'a recompress failure must never leave packets_complete');
    assert.match(state.status_reason ?? '', /cannot recompress chunk/);
    assert.deepEqual(state.decompressed_chunks, [], 'cleanup must reclaim the decompression');
    const chunks = await chunkStates(pool);
    assert.equal(chunks[0]?.compressed, true);
    const resolved = await pool.query<{ n: string }>(
      'SELECT count(*)::text AS n FROM mesh.packets WHERE packet_uid IS NOT NULL',
    );
    assert.equal(resolved.rows[0]!.n, '2');
  },
);

/** A final recompression failure must abort before completion is written. */
test(
  'a final recompress failure fails the run and keeps the chunk owned',
  { skip, timeout: 240_000 },
  async (t) => {
    const pool = await prepare(t);
    const rows = [0x111, 0x112].map(wireFixture);
    for (let index = 0; index < rows.length; index += 1) {
      await insertPacket(pool, {
        time: `2027-02-01 00:00:0${index + 1}+00`, hash: rows[index]!.hash, rawHex: rows[index]!.rawHex,
      });
    }
    await compressChunkStartingAt(pool, '2027-02-01 00:00:00+00');

    await assert.rejects(
      // batch/limit 1 leaves the chunk incomplete, so its recompression is
      // deferred to the final pass.
      backfillPackets(pool, 1, false, 1, null, {
        onEvent: (event) => {
          if (event.type === 'recompress' && event.phase === 'final') {
            throw new Error('injected final recompress failure');
          }
        },
      }),
      /cannot recompress 1 chunk\(s\) decompressed by this run/,
    );

    const state = await stateRow(pool);
    assert.equal(state.status, 'failed');
    assert.match(state.status_reason ?? '', /cannot recompress 1 chunk\(s\)/);
    const keys = await chunkKeys(pool);
    assert.deepEqual(state.decompressed_chunks, keys, 'the marker must keep the unrecompressed chunk owned');
    const chunks = await chunkStates(pool);
    assert.equal(chunks[0]?.compressed, false, 'the injected failure must leave the chunk decompressed');
  },
);

const RUN_LOCK_KEY = 'meshcore.packet_identity_backfill';

/** True when another session holds the run lock; the probe always releases its own. */
async function advisoryLockIsHeld(pool: Fixture): Promise<boolean> {
  const client = await pool.connect();
  try {
    const result = await client.query<{ locked: boolean }>(
      'SELECT pg_try_advisory_lock(hashtext($1)) AS locked',
      [RUN_LOCK_KEY],
    );
    if (result.rows[0]!.locked) {
      await client.query('SELECT pg_advisory_unlock(hashtext($1))', [RUN_LOCK_KEY]);
      return false;
    }
    return true;
  } finally {
    client.release();
  }
}

async function waitForLock(pool: Fixture, held: boolean, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await advisoryLockIsHeld(pool) === held) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`timed out waiting for the run lock to be ${held ? 'held' : 'released'}`);
}

/**
 * Pause the first UPDATE of the state row once, after the advisory lock is
 * held and before the first batch BEGIN. pg_sleep runs in the server backend,
 * so the child process is genuinely parked at that point rather than racing a
 * JavaScript hook.
 */
async function installPauseTrigger(pool: Fixture): Promise<void> {
  await pool.query(`
    CREATE TABLE ops.backfill_pause_once (done BOOLEAN NOT NULL DEFAULT FALSE);
    INSERT INTO ops.backfill_pause_once (done) VALUES (FALSE);
    CREATE FUNCTION ops.backfill_pause() RETURNS TRIGGER LANGUAGE plpgsql AS $fn$
    DECLARE first_call BOOLEAN;
    BEGIN
      UPDATE ops.backfill_pause_once
         SET done = TRUE
       WHERE done = FALSE
      RETURNING TRUE INTO first_call;
      IF first_call THEN PERFORM pg_sleep(15); END IF;
      RETURN NEW;
    END $fn$;
    CREATE TRIGGER backfill_pause
      BEFORE UPDATE ON ops.packet_identity_backfill_state
      FOR EACH ROW EXECUTE FUNCTION ops.backfill_pause();
  `);
}

/**
 * The dry run is a real read: it must resolve exactly the rows the real run
 * resolves, while writing no packet_uid and no checkpoint state.
 */
test(
  'a dry run resolves the same rows as the real run and writes no identity',
  { skip, timeout: 240_000 },
  async (t) => {
    const pool = await prepare(t);
    const first = wireFixture(0x121);
    const second = wireFixture(0x122);
    const third = wireFixture(0x123);
    const unresolved = wireFixture(0x124);
    const mismatch = wireFixture(0x125);

    await insertPacket(pool, { time: '2028-01-01 00:00:01+00', hash: first.hash, rawHex: first.rawHex });
    await insertPacket(pool, { time: '2028-01-01 00:00:02+00', hash: second.hash, rawHex: second.rawHex });
    await insertPacket(pool, { time: '2028-01-01 00:00:03+00', hash: third.hash, rawHex: third.rawHex });
    await insertPacket(pool, { time: '2028-01-01 00:00:04+00', hash: unresolved.hash, rawHex: null });
    await insertPacket(pool, { time: '2028-01-01 00:00:05+00', hash: mismatch.hash, rawHex: first.rawHex });

    const eligible = await countEligible(pool);
    assert.equal(eligible, 5);

    const dry = await runBackfill(pool, ['--dry-run', '--batch-size', '2']);
    assert.equal(dry.code, 0, dry.stderr);
    assert.ok(dry.summary, dry.stdout);
    assert.equal(dry.summary.selected, eligible);
    assert.equal(dry.summary.resolved, 3);
    assert.equal(dry.summary.unresolved, 1);
    assert.equal(dry.summary.mismatch, 1);

    const afterDry = await pool.query<{ n: string }>(
      'SELECT count(*)::text AS n FROM mesh.packets WHERE packet_uid IS NOT NULL',
    );
    assert.equal(afterDry.rows[0]!.n, '0', 'a dry run must not write packet_uid');
    const dryState = await pool.query<{ status: string; batch_count: string; resolved: string; high_water: string | null }>(`
      SELECT status, batch_count::text AS batch_count, resolved::text AS resolved, high_water::text AS high_water
        FROM ops.packet_identity_backfill_state
    `);
    assert.deepEqual(dryState.rows[0], {
      status: 'idle', batch_count: '0', resolved: '0', high_water: null,
    }, 'a dry run must not advance the checkpoint');

    const real = await runBackfill(pool, ['--batch-size', '2']);
    assert.equal(real.code, 0, real.stderr);
    assert.ok(real.summary, real.stdout);
    assert.equal(real.summary.selected, dry.summary.selected);
    assert.equal(real.summary.resolved, dry.summary.resolved, 'the dry-run resolved count must equal the real run');
    assert.equal(real.summary.unresolved, dry.summary.unresolved);
    assert.equal(real.summary.mismatch, dry.summary.mismatch);

    const afterReal = await pool.query<{ n: string }>(
      'SELECT count(*)::text AS n FROM mesh.packets WHERE packet_uid IS NOT NULL',
    );
    assert.equal(afterReal.rows[0]!.n, '3', 'the real run writes exactly the rows the dry run counted');
  },
);

/**
 * The session-level advisory lock is the only thing that stops two backfills
 * from interleaving. It must be taken before any BEGIN, released on a normal
 * exit, and released by PostgreSQL when the holder is SIGKILLed.
 */
test(
  'the run lock rejects a second process and is released on exit and after SIGKILL',
  { skip, timeout: 300_000 },
  async (t) => {
    const pool = await prepare(t);
    const a = wireFixture(0x131);
    const b = wireFixture(0x132);
    await insertPacket(pool, { time: '2028-02-01 00:00:01+00', hash: a.hash, rawHex: a.rawHex });
    await insertPacket(pool, { time: '2028-02-01 00:00:02+00', hash: b.hash, rawHex: b.rawHex });
    await installPauseTrigger(pool);

    const first = spawnBackfill(pool, ['--batch-size', '10']);
    await waitForLock(pool, true, 30_000);

    const second = await runBackfill(pool, ['--batch-size', '10']);
    assert.equal(second.code, 1, 'a second process must exit non-zero');
    assert.match(second.stderr, /packet identity backfill is already running/);
    assert.equal(second.summary, undefined, 'a rejected run must not report a summary');
    const during = await pool.query<{ batch_count: string }>(
      'SELECT batch_count::text AS batch_count FROM ops.packet_identity_backfill_state',
    );
    assert.equal(during.rows[0]!.batch_count, '0', 'the rejected run must fail before its first BEGIN');

    const firstRun = await first.done;
    assert.equal(firstRun.code, 0, firstRun.stderr);
    assert.equal(firstRun.summary?.status, 'packets_complete');

    const afterNormalExit = await runBackfill(pool, ['--batch-size', '10']);
    assert.equal(afterNormalExit.code, 0, 'a normal exit must release the lock');
    assert.equal(afterNormalExit.summary?.selected, 0);

    // Re-arm the pause and make the rows eligible again. The state reset runs
    // while the trigger is still disarmed so it cannot sleep.
    await pool.query(
      'UPDATE mesh.packets SET packet_uid = NULL, identity_version = NULL, identity_provenance = NULL',
    );
    await pool.query(`
      UPDATE ops.packet_identity_backfill_state
         SET status = 'idle', high_water = NULL, last_time = NULL, last_observation_id = NULL,
             last_hash = NULL, last_rx_node_id = NULL, batch_count = 0, resolved = 0
    `);
    await pool.query('UPDATE ops.backfill_pause_once SET done = FALSE');

    const doomed = spawnBackfill(pool, ['--batch-size', '10']);
    await waitForLock(pool, true, 30_000);
    doomed.child.kill('SIGKILL');
    await doomed.done;
    await waitForLock(pool, false, 90_000);

    const afterKill = await runBackfill(pool, ['--batch-size', '10']);
    assert.equal(afterKill.code, 0, 'PostgreSQL must release a SIGKILLed session lock');
    assert.equal(afterKill.summary?.resolved, 2);
  },
);
