import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import test, { type TestContext } from 'node:test';
import pg from 'pg';
import { getRecentPackets } from '../db/index.js';

/**
 * Regression tests for the three HIGH defects found by the 2026-11-03 identity
 * audit and fixed in 384226c ("p2: harden identity backfill and reader
 * statistics"). They execute real SQL against real PostgreSQL: the backfill
 * tests spawn the actual CLI, so the batch SQL, the checkpoint writes and the
 * `RETURNING ctid` accounting are all exercised as shipped.
 *
 * Harness: `TEST_BACKFILL_DATABASE_URL` must point at a throwaway loopback
 * database (same convention as the other `.integration.test.ts` files). The
 * fixture refuses to run if the database holds tables outside the `mesh`,
 * `ops`, `core` and `timescaledb_information` fixture schemas.
 *
 * Revert lines each test is built to catch are named in the test comments.
 */

const databaseUrl = process.env['TEST_BACKFILL_DATABASE_URL'];
const skip = databaseUrl
  ? false
  : 'real PostgreSQL required: set TEST_BACKFILL_DATABASE_URL to a throwaway loopback database';

const backendRoot = fileURLToPath(new URL('../..', import.meta.url));
const toolPath = fileURLToPath(new URL('./backfillPacketIdentity.ts', import.meta.url));

type Fixture = pg.Pool;

/**
 * Build a wire-shaped packet: route type 1 (no transport codes), zero path
 * hashes, one-byte hash size. `canonicalFromRawHex` hashes `[byte0 & 0xFC]`
 * followed by everything after the path byte, so the expected packet_hash is
 * reproducible from the same bytes without importing the tool's private
 * helper.
 */
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
CREATE SCHEMA core;

CREATE TABLE mesh.packets (
  time TIMESTAMPTZ NOT NULL,
  packet_hash TEXT NOT NULL,
  packet_uid BYTEA,
  observation_id UUID,
  rx_node_id TEXT,
  src_node_id TEXT,
  raw_hex TEXT,
  identity_version TEXT,
  identity_provenance TEXT,
  network TEXT DEFAULT 'ukmesh',
  topic TEXT DEFAULT 'meshcore/EMA/OBS/packets',
  topic_prefix TEXT DEFAULT 'meshcore',
  iata TEXT,
  packet_type INTEGER DEFAULT 5,
  route_type INTEGER,
  hop_count INTEGER,
  rssi INTEGER,
  snr DOUBLE PRECISION,
  payload JSONB DEFAULT '{}'::jsonb,
  advert_count INTEGER,
  path_hashes TEXT[],
  path_hash_size_bytes INTEGER,
  transport_codes TEXT[],
  region_scope TEXT,
  visibility_ok BOOLEAN DEFAULT TRUE,
  is_private BOOLEAN DEFAULT FALSE
);
CREATE INDEX packets_packet_hash_idx ON mesh.packets (packet_hash);

CREATE TABLE mesh.packet_decryptions (
  packet_hash TEXT PRIMARY KEY,
  decrypted JSONB NOT NULL DEFAULT '{}'::jsonb,
  summary TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE mesh.message_tags (
  packet_hash TEXT PRIMARY KEY,
  packet_time TIMESTAMPTZ NOT NULL,
  tagged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  model TEXT,
  latency_ms INTEGER,
  tags JSONB NOT NULL,
  confidence JSONB
);

CREATE TABLE core.node_identity_aliases (source_node_id TEXT PRIMARY KEY, canonical_node_id TEXT);

CREATE TABLE ops.public_visibility_state (singleton BOOLEAN PRIMARY KEY, generation BIGINT NOT NULL);
CREATE TABLE ops.packet_visibility_materialization_state (singleton BOOLEAN PRIMARY KEY, visibility_generation BIGINT NOT NULL);
CREATE TABLE ops.privacy_rematerialization_queue (target TEXT, status TEXT, node_id TEXT, network TEXT);
INSERT INTO ops.public_visibility_state VALUES (TRUE, 1);
INSERT INTO ops.packet_visibility_materialization_state VALUES (TRUE, 1);

-- Mirrors migrations 130, 131 and 134. The legacy last_hash/last_rx_node_id
-- columns are still present in production and are declared here so the pre-fix
-- tool can be run against the same fixture to prove these tests fail when
-- reverted.
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

async function installChunkCatalog(pool: Fixture): Promise<void> {
  const existing = await pool.query<{ relkind: string }>(
    `SELECT c.relkind::text AS relkind
       FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'timescaledb_information' AND c.relname = 'chunks'`,
  );
  if (existing.rowCount === 0) {
    // Plain PostgreSQL: stand in for the TimescaleDB catalogue view the tool
    // reads. This is the table shape the tool depends on, not a mock of it.
    await pool.query(`
      CREATE SCHEMA timescaledb_information;
      CREATE TABLE timescaledb_information.chunks (
        chunk_schema TEXT, chunk_name TEXT, hypertable_schema TEXT, hypertable_name TEXT,
        is_compressed BOOLEAN, range_start TIMESTAMPTZ
      );
      INSERT INTO timescaledb_information.chunks
        VALUES ('mesh', 'packets', 'mesh', 'packets', FALSE, '2026-01-01T00:00:00Z');
    `);
    return;
  }
  if (existing.rows[0]!.relkind !== 'r') {
    // Real TimescaleDB: make mesh.packets a genuine single-chunk hypertable.
    await pool.query(`SELECT create_hypertable('mesh.packets', 'time', if_not_exists => TRUE)`);
  }
}

async function prepare(t: TestContext): Promise<Fixture> {
  assert.ok(databaseUrl, 'TEST_BACKFILL_DATABASE_URL is required');
  const endpoint = new URL(databaseUrl);
  assert.ok(
    ['localhost', '127.0.0.1', '::1', '[::1]'].includes(endpoint.hostname),
    'only an isolated loopback test database is permitted',
  );
  const pool = new pg.Pool({ connectionString: databaseUrl, max: 2 });
  t.after(async () => { await pool.end(); });

  const strays = await pool.query<{ table_schema: string; table_name: string }>(
    `SELECT table_schema, table_name FROM information_schema.tables
      WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'mesh', 'ops', 'core')
        AND table_schema NOT LIKE 'pg\\_%'
        AND table_schema NOT LIKE '\\_timescaledb%'
        AND table_schema NOT LIKE 'timescaledb\\_%'`,
  );
  assert.equal(strays.rowCount, 0, 'refusing to run: the target database already holds other tables');

  const chunks = await pool.query<{ relkind: string }>(
    `SELECT c.relkind::text AS relkind
       FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'timescaledb_information' AND c.relname = 'chunks'`,
  );
  if (chunks.rows[0]?.relkind === 'r') {
    await pool.query('DROP SCHEMA IF EXISTS timescaledb_information CASCADE');
  }
  await pool.query(`
    DROP SCHEMA IF EXISTS mesh CASCADE;
    DROP SCHEMA IF EXISTS ops CASCADE;
    DROP SCHEMA IF EXISTS core CASCADE;
  `);
  await pool.query(FIXTURE_SQL);
  await installChunkCatalog(pool);
  return pool;
}

type BackfillSummary = {
  dryRun: boolean;
  selected: number;
  resolved: number;
  unresolved: number;
  mismatch: number;
};

/**
 * The five counters the original assertions pinned. The summary now also
 * carries per-chunk results and the completion claim; those are asserted by
 * backfillIdentity.compressed.test.ts.
 */
function counters(summary: BackfillSummary | undefined) {
  assert.ok(summary, 'the CLI must print a summary');
  return {
    dryRun: summary.dryRun,
    selected: summary.selected,
    resolved: summary.resolved,
    unresolved: summary.unresolved,
    mismatch: summary.mismatch,
  };
}

type BackfillRun = {
  code: number;
  stdout: string;
  stderr: string;
  summary: BackfillSummary | undefined;
};

/** Run the shipped CLI so the SQL, transaction and checkpoint writes are real. */
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
    const timer = setTimeout(() => child.kill('SIGKILL'), 90_000);
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
  row: {
    time: string;
    hash: string;
    observationId?: string;
    rxNodeId: string;
    rawHex: string | null;
    uid?: Buffer | null;
    srcNodeId?: string | null;
    advertCount?: number | null;
    packetType?: number;
  },
): Promise<void> {
  await pool.query(
    `INSERT INTO mesh.packets
       (time, packet_hash, packet_uid, observation_id, rx_node_id, src_node_id, raw_hex,
        packet_type, advert_count, payload)
     VALUES ($1::timestamptz, $2, $3, $4::uuid, $5, $6, $7, $8, $9, '{}'::jsonb)`,
    [
      row.time,
      row.hash,
      row.uid ?? null,
      row.observationId ?? randomUUID(),
      row.rxNodeId,
      row.srcNodeId ?? null,
      row.rawHex,
      row.packetType ?? 5,
      row.advertCount ?? null,
    ],
  );
}

/**
 * Defect 1 — cursor uniqueness and precision.
 *
 * Revert to prove: backfillPacketIdentity.ts:133-140 (legacy
 * `(time, packet_hash, rx_node_id)` predicate), :147 (`ORDER BY time,
 * packet_hash`) and :143 (`SELECT time` instead of `time::text`). The five rows
 * below share time, hash and receiver, so the legacy cursor stops after page
 * one and the microsecond timestamp truncates to milliseconds in the Date
 * cursor.
 */
test(
  'identity backfill visits every observation when duplicate (time, hash, rx) rows share a page boundary',
  { skip, timeout: 180_000 },
  async (t) => {
    const pool = await prepare(t);
    const duplicate = wireFixture(0x11);
    const duplicateTime = '2026-11-02 09:00:00+00'; // millisecond-exact
    const rxNodeId = 'AA'.repeat(32);
    for (let i = 0; i < 5; i += 1) {
      await insertPacket(pool, {
        time: duplicateTime,
        hash: duplicate.hash,
        rxNodeId,
        rawHex: duplicate.rawHex,
      });
    }
    const precise = wireFixture(0x22);
    const preciseTime = '2026-11-02 09:00:01.123456+00';
    const preciseObservation = randomUUID();
    await insertPacket(pool, {
      time: preciseTime,
      hash: precise.hash,
      observationId: preciseObservation,
      rxNodeId,
      rawHex: precise.rawHex,
    });

    const run = await runBackfill(['--batch-size', '2']);
    assert.equal(run.code, 0, run.stderr);
    assert.deepEqual(counters(run.summary), {
      dryRun: false,
      selected: 6,
      resolved: 6,
      unresolved: 0,
      mismatch: 0,
    });

    const resolved = await pool.query<{ n: string }>(
      `SELECT count(*)::text AS n FROM mesh.packets WHERE packet_uid IS NOT NULL`,
    );
    assert.equal(resolved.rows[0]!.n, '6', 'every row must be resolved exactly once');
    const duplicateRows = await pool.query<{ n: string }>(
      `SELECT count(*)::text AS n FROM mesh.packets WHERE packet_hash = $1 AND packet_uid IS NOT NULL`,
      [duplicate.hash],
    );
    assert.equal(duplicateRows.rows[0]!.n, '5', 'all duplicate-key observations must be visited');

    const state = await pool.query<{
      last_time: string; last_observation_id: string; resolved: string; batch_count: string;
    }>(`
      SELECT to_char(last_time AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS last_time,
             last_observation_id::text AS last_observation_id,
             resolved::text AS resolved,
             batch_count::text AS batch_count
        FROM ops.packet_identity_backfill_state
    `);
    assert.equal(state.rows[0]!.batch_count, '3', 'six rows at batch size two is three batches, no revisits');
    assert.equal(state.rows[0]!.resolved, '6');
    assert.equal(state.rows[0]!.last_observation_id, preciseObservation);
    assert.equal(
      state.rows[0]!.last_time,
      '2026-11-02 09:00:01.123456',
      'the checkpoint must carry the timestamp at full precision',
    );
  },
);

/**
 * Defect 2 — affected-row accounting and partial-run status.
 *
 * Revert to prove: backfillPacketIdentity.ts:222 (`resolvedCount =
 * update.rowCount ?? 0`) back to `resolved.length` at :238, and :262
 * (`[partial ? 'running' : 'packets_complete']`) back to an unconditional
 * `'packets_complete'`.
 *
 * The trigger makes one validated row ineligible between SELECT and UPDATE —
 * the concurrent-writer window the RETURNING count exists to measure — so the
 * pre-fix counter reports a row it never wrote.
 */
test(
  'identity backfill counts rows PostgreSQL actually updated and leaves a --limit run running',
  { skip, timeout: 180_000 },
  async (t) => {
    const pool = await prepare(t);
    const a = wireFixture(0x31);
    const b = wireFixture(0x32);
    const c = wireFixture(0x33);
    const d = wireFixture(0x34);
    const e = wireFixture(0x35);

    await insertPacket(pool, { time: '2026-11-02 10:00:00+00', hash: a.hash, rxNodeId: 'A1'.repeat(32), rawHex: a.rawHex });
    await insertPacket(pool, { time: '2026-11-02 10:00:01+00', hash: b.hash, rxNodeId: 'A2'.repeat(32), rawHex: b.rawHex });
    await insertPacket(pool, { time: '2026-11-02 10:00:02+00', hash: c.hash, rxNodeId: 'A3'.repeat(32), rawHex: c.rawHex });
    await insertPacket(pool, { time: '2026-11-02 10:00:03+00', hash: d.hash, rxNodeId: 'A4'.repeat(32), rawHex: '0100ff' });
    await insertPacket(pool, { time: '2026-11-02 10:00:04+00', hash: e.hash, rxNodeId: 'A5'.repeat(32), rawHex: '' });

    await pool.query(`
      CREATE FUNCTION mesh.pa_skip_concurrently_resolved() RETURNS TRIGGER LANGUAGE plpgsql AS $fn$
      BEGIN
        IF NEW.identity_provenance = 'historical-raw-hex-recompute' AND OLD.packet_hash = '${b.hash}' THEN
          RETURN NULL;
        END IF;
        RETURN NEW;
      END $fn$;
      CREATE TRIGGER pa_skip_concurrently_resolved
        BEFORE UPDATE ON mesh.packets
        FOR EACH ROW EXECUTE FUNCTION mesh.pa_skip_concurrently_resolved();
    `);

    const run = await runBackfill(['--batch-size', '10']);
    assert.equal(run.code, 0, run.stderr);
    assert.deepEqual(counters(run.summary), {
      dryRun: false,
      selected: 5,
      resolved: 2,
      unresolved: 1,
      mismatch: 1,
    });

    const written = await pool.query<{ n: string }>(
      `SELECT count(*)::text AS n FROM mesh.packets WHERE packet_uid IS NOT NULL`,
    );
    assert.equal(written.rows[0]!.n, '2', 'only the two rows PostgreSQL actually updated hold a UID');
    const skipped = await pool.query<{ uid: Buffer | null }>(
      `SELECT packet_uid AS uid FROM mesh.packets WHERE packet_hash = $1`,
      [b.hash],
    );
    assert.equal(skipped.rows[0]!.uid, null);

    const state = await pool.query<{ resolved: string; unresolved: string; ambiguous: string }>(
      `SELECT resolved::text AS resolved, unresolved::text AS unresolved, ambiguous::text AS ambiguous
         FROM ops.packet_identity_backfill_state`,
    );
    assert.equal(state.rows[0]!.resolved, written.rows[0]!.n, 'the counter must match the rows actually written');
    assert.equal(state.rows[0]!.resolved, '2');
    assert.equal(state.rows[0]!.unresolved, '1');
    assert.equal(state.rows[0]!.ambiguous, '1');

    await pool.query(`
      UPDATE ops.packet_identity_backfill_state
         SET high_water = NULL, last_time = NULL, last_hash = NULL, last_rx_node_id = NULL,
             last_observation_id = NULL, status = 'idle',
             resolved = 0, unresolved = 0, ambiguous = 0, batch_count = 0
    `);
    const partial = await runBackfill(['--batch-size', '1', '--limit', '1']);
    assert.equal(partial.code, 0, partial.stderr);
    assert.equal(partial.summary?.selected, 1, '--limit 1 stops after one selected row');
    const status = await pool.query<{ status: string }>(
      `SELECT status FROM ops.packet_identity_backfill_state`,
    );
    // 134 replaced the ambiguous 'running' after a --limit stop with an
    // explicit partial phase: the process has exited, so nothing is running.
    assert.equal(status.rows[0]!.status, 'packets_partial', 'a limited run is not a completed phase');
  },
);

/**
 * Defect 3 — identity grouping parity for statistics and tags.
 *
 * Revert to prove: backend/src/db/index.ts `packet_stats` grouping
 * (`GROUP BY packetIdentityKeySql('packets')`) back to `GROUP BY packet_hash`,
 * and the `packet_tags` CTE back to `LEFT JOIN mesh.message_tags mt ON
 * mt.packet_hash = rp.packet_hash`. The representative row carries hash H2
 * while the tags live on H1; both belong to one identity with three
 * observations.
 */
test(
  'recent packet reads group statistics and tags on the same identity as the representative',
  { skip, timeout: 180_000 },
  async (t) => {
    const pool = await prepare(t);
    const identity = Buffer.from('ab'.repeat(32), 'hex');
    const h1 = 'A1'.repeat(32);
    const h2 = 'B2'.repeat(32);
    const observers = ['11'.repeat(32), '22'.repeat(32), '33'.repeat(32)];

    // Insert directly inside the reader's 5-minute window. Updating the
    // partition key afterwards moved rows between chunks on a hypertable and
    // tripped the destination chunk's range constraint (23514).
    const t0 = new Date(Date.now() - 120_000);
    const at = (offsetMs: number) => new Date(t0.getTime() + offsetMs).toISOString();
    await insertPacket(pool, {
      time: at(0), hash: h1, uid: identity, rxNodeId: observers[0]!, rawHex: null,
    });
    await insertPacket(pool, {
      time: at(1_000), hash: h1, uid: identity, rxNodeId: observers[1]!, rawHex: null,
    });
    await insertPacket(pool, {
      // Highest representative rank (advert_count) and latest time.
      time: at(2_000), hash: h2, uid: identity, rxNodeId: observers[2]!, rawHex: null,
      advertCount: 7,
    });
    await pool.query(
      `INSERT INTO mesh.message_tags (packet_hash, packet_time, tags, confidence)
       VALUES ($1, $2, '{"topic":"coverage"}'::jsonb, '{"topic":0.9}'::jsonb)`,
      [h1, at(0)],
    );

    const readQuery = async <T extends pg.QueryResultRow>(
      text: string,
      params?: unknown[],
      signal?: AbortSignal,
    ): Promise<{ rows: T[] }> => {
      void signal;
      const result = await pool.query<T>(text, params as unknown[]);
      return { rows: result.rows };
    };

    const rows = await getRecentPackets(50, undefined, undefined, 'full', undefined, readQuery);
    assert.equal(rows.length, 1, 'one identity is one representative row');
    const row = rows[0] as unknown as {
      packet_hash: string;
      rx_count: number;
      tx_count: number;
      observer_node_ids: string[];
      tags: unknown;
      tag_confidence: unknown;
    };

    assert.equal(row.packet_hash, h2, 'the trained row is the representative');
    assert.equal(row.rx_count, 3, 'statistics span every observation of the identity');
    assert.equal(row.tx_count, 0);
    assert.deepEqual([...row.observer_node_ids].sort(), [...observers].sort());
    assert.deepEqual(row.tags, { topic: 'coverage' }, 'tags are resolved on the same identity as the statistics');
    assert.deepEqual(row.tag_confidence, { topic: 0.9 });
  },
);
