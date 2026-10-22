import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import fs from 'node:fs';
import test from 'node:test';
import { promisify } from 'node:util';
import pg from 'pg';
import { publicPacketPrivacySql } from '../api/utils/networkFilters.js';
import { convergePacketVisibility } from '../privacy/convergence.js';
import { processNextPrivacyRematerialization } from '../workers/privacy-rematerialization.js';
import { runMigrations } from './migrations.js';

// These tests create and own a new database. Never take DATABASE_URL from the
// application environment; require the explicit loopback-only test endpoint.
const databaseUrl = process.env['TEST_PRIVACY_DATABASE_URL'] ?? process.env['TEST_INGEST_DATABASE_URL'];
const local = databaseUrl && ['127.0.0.1', 'localhost', '[::1]'].includes(new URL(databaseUrl).hostname);
const execute = promisify(execFile);
const nodeId = 'AABBCC' + 'AA'.repeat(29);
const otherId = 'BB'.repeat(32);
const migration = fs.readFileSync(new URL('./migrations/062_prefix_visibility_statement_fence.sql', import.meta.url), 'utf8');
const rollback = fs.readFileSync(new URL('./rollback/062_prefix_visibility_statement_fence.sql', import.meta.url), 'utf8');

test('local TimescaleDB visibility-fence reproduction, regression and convergence', {
  skip: local ? false : 'explicit loopback TEST_PRIVACY_DATABASE_URL or TEST_INGEST_DATABASE_URL required',
  timeout: 300_000,
}, async (t) => {
  const admin = new pg.Pool({ connectionString: databaseUrl, max: 1 });
  const name = `privacy_fence_${process.pid}_${Date.now()}`;
  const readOnlyRole = `privacy_readonly_${process.pid}_${Date.now()}`;
  const url = new URL(databaseUrl!);
  url.pathname = `/${name}`;
  const pool = new pg.Pool({ connectionString: url.toString(), max: 4 });
  t.after(async () => {
    await pool.end();
    await admin.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
    await admin.query(`DROP ROLE IF EXISTS ${readOnlyRole}`);
    await admin.end();
  });
  await admin.query(`CREATE DATABASE "${name}"`);
  await pool.query(fs.readFileSync(new URL('./schema/base.sql', import.meta.url), 'utf8'));
  await runMigrations(pool);
  // The disposable fixture includes historical rows beyond normal retention.
  await pool.query(`SELECT alter_job(job_id, scheduled => FALSE)
    FROM timescaledb_information.jobs WHERE job_id >= 1000`);

  const pair = async () => (await pool.query<{ generation: string; materialized_generation: string | null }>(`
    SELECT p.generation::text, m.visibility_generation::text AS materialized_generation
      FROM public_visibility_state p LEFT JOIN packet_visibility_materialization_state m USING (singleton)
      WHERE p.singleton
  `)).rows[0];
  const reset = async () => {
    await pool.query(`TRUNCATE nodes, private_node_prefixes, node_identity_aliases,
      privacy_rematerialization_queue, packets, packet_paths CASCADE;
      UPDATE public_visibility_state SET generation = 1, updated_at = NOW();
      INSERT INTO packet_visibility_materialization_state VALUES (TRUE, 1, NOW())
      ON CONFLICT (singleton) DO UPDATE SET visibility_generation = 1, updated_at = NOW();`);
  };
  const addNode = async (id = nodeId, privateName = false) => {
    await pool.query('INSERT INTO nodes (node_id, name, network) VALUES ($1, $2, $3)',
      [id, privateName ? 'Fixture private 🚫' : 'Fixture public', 'ukmesh']);
  };
  const addPrefix = async () => {
    await pool.query(`INSERT INTO private_node_prefixes (node_id, network, prefix_size_bytes, prefix)
      VALUES ($1, 'ukmesh', 1, 'AA')`, [nodeId]);
  };
  const repair = async () => convergePacketVisibility(pool, { apply: true, expectedGeneration: (await pair()).generation });
  const flags = async (target: 'packets' | 'packet_paths') => (await pool.query(`
    SELECT packet_hash, is_private, visibility_ok FROM ${target} ORDER BY packet_hash
  `)).rows;
  const snapshot = async () => (await pool.query(`SELECT
    (SELECT jsonb_agg(to_jsonb(p) ORDER BY packet_hash) FROM packets p) AS packets,
    (SELECT jsonb_agg(to_jsonb(p) ORDER BY packet_hash) FROM packet_paths p) AS paths,
    (SELECT jsonb_agg(to_jsonb(q) ORDER BY id) FROM privacy_rematerialization_queue q) AS queue,
    (SELECT to_jsonb(v) FROM public_visibility_state v) AS visibility,
    (SELECT to_jsonb(m) FROM packet_visibility_materialization_state m) AS materialization`)).rows[0];

  await t.test('reproduces the shipped no-row FK cascade, then proves rollback and reapply', async () => {
    await reset();
    // The rollback is executable SQL, not a description. It restores 042.
    await pool.query(rollback);
    await addNode();
    assert.deepEqual(await pair(), { generation: '1', materialized_generation: '1' });
    await pool.query('DELETE FROM nodes WHERE node_id = $1', [nodeId]);
    assert.deepEqual(await pair(), { generation: '2', materialized_generation: '1' });
    assert.equal((await pool.query('SELECT COUNT(*)::text AS count FROM private_node_prefixes')).rows[0].count, '0');
    t.diagnostic('BASELINE REPRO: empty FK-cascade DELETE changes 1/1 -> 2/1 with zero prefix rows');
    await pool.query("INSERT INTO packets(packet_hash, topic) VALUES ('upgrade-preserved', 'fixture')");
    await pool.query("DELETE FROM schema_migrations WHERE name = '062_prefix_visibility_statement_fence.sql'");
    assert.deepEqual(await runMigrations(pool), ['062_prefix_visibility_statement_fence.sql']);
    assert.deepEqual(await flags('packets'), [{ packet_hash: 'upgrade-preserved', is_private: false, visibility_ok: true }]);
    // Neither migration nor rollback certifies an existing gap.
    assert.deepEqual(await pair(), { generation: '2', materialized_generation: '1' });
    await reset();
    await addNode();
    await pool.query('DELETE FROM nodes WHERE node_id = $1', [nodeId]);
    assert.deepEqual(await pair(), { generation: '1', materialized_generation: '1' });
    await runMigrations(pool); // checksum verification and normal idempotent runner
    // Also prove the SQL is transactional: a failed migration transaction
    // cannot leave half of the trigger replacement installed.
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(rollback.replace(/^BEGIN;|^COMMIT;/gm, ''));
      await assert.rejects(client.query(`${migration}\nSELECT 1 / 0;`), /division by zero/);
    } finally { await client.query('ROLLBACK'); client.release(); }
    await addNode();
    await pool.query('DELETE FROM nodes WHERE node_id = $1', [nodeId]);
    assert.deepEqual(await pair(), { generation: '1', materialized_generation: '1' });
  });

  await t.test('empty direct INSERT, UPDATE, DELETE and public multi-node cascades do not invalidate', async () => {
    await reset();
    await addNode(); await addNode(otherId);
    await pool.query(`INSERT INTO private_node_prefixes (node_id, network, prefix_size_bytes, prefix)
      SELECT $1, 'ukmesh', 1, 'AA' WHERE FALSE`, [nodeId]);
    await pool.query(`UPDATE private_node_prefixes SET prefix = 'FF' WHERE FALSE`);
    await pool.query('DELETE FROM private_node_prefixes WHERE FALSE');
    await pool.query('DELETE FROM nodes');
    assert.deepEqual(await pair(), { generation: '1', materialized_generation: '1' });
  });

  await t.test('real direct prefix insert/update/delete and TRUNCATE fence until full convergence', async () => {
    await reset(); await addNode();
    for (const operation of [
      addPrefix,
      () => pool.query("UPDATE private_node_prefixes SET prefix = 'AB'"),
      () => pool.query('DELETE FROM private_node_prefixes'),
      () => pool.query('TRUNCATE private_node_prefixes'),
    ]) {
      const before = await pair();
      await operation();
      const after = await pair();
      assert.equal(BigInt(after.generation), BigInt(before.generation) + 1n);
      assert.equal(after.materialized_generation, before.materialized_generation);
      const result = await repair();
      assert.equal(result.after?.needsConvergence, false);
    }
  });

  await t.test('a cascade that really removes a prefix remains fail closed', async () => {
    for (const privateName of [false, true]) {
      await reset(); await addNode(nodeId, privateName);
      if (!privateName) await addPrefix();
      await repair();
      const before = await pair();
      await pool.query('DELETE FROM nodes WHERE node_id = $1', [nodeId]);
      const after = await pair();
      assert.equal(BigInt(after.generation), BigInt(before.generation) + 1n);
      assert.equal(after.materialized_generation, before.materialized_generation);
      assert.equal((await repair()).after?.needsConvergence, false);
    }
  });

  await t.test('node privacy transitions keep their queue gate and never certify an existing prefix gap', async () => {
    await reset(); await addNode();
    await pool.query(`INSERT INTO packets(packet_hash,topic,network,src_node_id) VALUES ('private','fixture','ukmesh',$1)`, [nodeId]);
    await addPrefix(); // independent direct mutation, deliberately not queued
    await pool.query("UPDATE nodes SET name = 'Fixture private 🚫' WHERE node_id = $1", [nodeId]);
    const before = await pair();
    assert.equal(before.generation, '2');
    assert.equal(before.materialized_generation, '1');
    assert.equal((await pool.query(`SELECT COUNT(*)::text AS count FROM packets p WHERE ${publicPacketPrivacySql('p')}`)).rows[0].count, '0');
    assert.equal(await processNextPrivacyRematerialization(pool, { chunkPauseMs: 0 }), true);
    assert.equal(await processNextPrivacyRematerialization(pool, { chunkPauseMs: 0 }), true);
    const after = await pair();
    assert.equal(after.generation, '4');
    assert.equal(after.materialized_generation, '1', 'scoped passes cannot repair an unrelated gap');
    await pool.query(`INSERT INTO node_identity_aliases(source_node_id, canonical_node_id, confidence, reason)
      VALUES ($1, $2, 'high', 'fixture')`, [otherId, nodeId]);
    assert.equal((await pair()).materialized_generation, '1', 'alias change cannot repair an unrelated gap');
    await repair();
    assert.deepEqual(await flags('packets'), [{ packet_hash: 'private', is_private: true, visibility_ok: false }]);
  });

  await t.test('healthy alias changes and completed node privacy work still advance the generation pair', async () => {
    await reset(); await addNode();
    await pool.query(`INSERT INTO node_identity_aliases(source_node_id, canonical_node_id, confidence, reason)
      VALUES ($1, $2, 'high', 'fixture')`, [otherId, nodeId]);
    assert.deepEqual(await pair(), { generation: '2', materialized_generation: '2' });
    await pool.query("UPDATE nodes SET name = 'Fixture private 🚫' WHERE node_id = $1", [nodeId]);
    assert.deepEqual(await pair(), { generation: '2', materialized_generation: '2' });
    assert.equal(await processNextPrivacyRematerialization(pool, { chunkPauseMs: 0 }), true);
    assert.equal(await processNextPrivacyRematerialization(pool, { chunkPauseMs: 0 }), true);
    assert.deepEqual(await pair(), { generation: '4', materialized_generation: '4' });
  });

  const seedStaleRows = async () => {
    await reset(); await addNode();
    for (const target of ['packets', 'packet_paths'] as const) {
      await pool.query(`INSERT INTO ${target}(time,packet_hash,topic,network,src_node_id,path_hashes,path_hash_size_bytes)
        VALUES
          (NOW(), 'direct', 'fixture', 'ukmesh', $1, ARRAY[]::text[], NULL),
          (NOW(), 'path1', 'fixture', 'northeast', NULL, ARRAY['AA'], 1),
          (NOW(), 'path2', 'fixture', 'teesside', NULL, ARRAY['AABB'], 2),
          (NOW() - INTERVAL '300 days', 'path3-old', 'fixture', 'ukmesh', NULL, ARRAY['AABBCC'], 3),
          (NOW(), 'lowercase', 'fixture', 'ukmesh', NULL, ARRAY['aabb'], 2),
          (NOW(), 'external', 'fixture', 'othernet', NULL, ARRAY['AABB'], 2),
          (NOW(), 'public', 'fixture', 'ukmesh', NULL, ARRAY[]::text[], NULL),
          (NOW(), 'malformed', 'fixture', 'ukmesh', NULL, ARRAY['GG'], 1),
          (NOW(), 'bad-width', 'fixture', 'ukmesh', NULL, ARRAY['AA'], 4)`, [nodeId]);
    }
    await pool.query("UPDATE nodes SET name = 'Fixture private 🚫' WHERE node_id = $1", [nodeId]);
    await pool.query(`UPDATE privacy_rematerialization_queue SET status = 'failed', attempts = 8,
      finished_at = NOW(), last_error = 'fixture exhausted' WHERE target = 'packet_paths'`);
    // Model stale historically materialized flags, including invalid framing.
    await pool.query('UPDATE packets SET is_private = FALSE, visibility_ok = TRUE');
    await pool.query('UPDATE packet_paths SET is_private = FALSE, visibility_ok = TRUE');
  };

  await t.test('dry-run is read-only, full convergence repairs both stores and pending/exhausted work, repeated apply is inert', async () => {
    await seedStaleRows();
    const before = await snapshot();
    const inventory = await convergePacketVisibility(pool);
    assert.equal(inventory.mode, 'dry-run');
    assert.equal(inventory.before.needsConvergence, true);
    assert.deepEqual(inventory.before.tables.map((row) => row.mismatches), ['7', '7']);
    assert.deepEqual(await snapshot(), before, 'dry-run must not modify flags, queue, generations or timestamps');
    // Run the actual CLI with a DB role that cannot write anything. Its
    // default must succeed, proving that inventory cannot silently mutate.
    await pool.query(`CREATE ROLE ${readOnlyRole} LOGIN PASSWORD 'privacy_test_readonly'`);
    await pool.query(`GRANT CONNECT ON DATABASE "${name}" TO ${readOnlyRole}`);
    await pool.query(`GRANT USAGE ON SCHEMA public TO ${readOnlyRole}`);
    await pool.query(`GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${readOnlyRole}`);
    const readOnlyURL = new URL(url); readOnlyURL.username = readOnlyRole; readOnlyURL.password = 'privacy_test_readonly';
    const cli = await execute(process.execPath, ['--import', 'tsx', 'src/tools/convergePacketVisibility.ts'], {
      cwd: new URL('../../', import.meta.url), env: { ...process.env, DATABASE_URL: readOnlyURL.toString() },
    });
    assert.equal(JSON.parse(cli.stdout).mode, 'dry-run');
    assert.deepEqual(await snapshot(), before);
    const result = await repair();
    assert.deepEqual(result.updated, { packets: 7, packet_paths: 7, queue: 2 });
    const expected = [
      ['bad-width', false, false], ['direct', true, false], ['external', false, true],
      ['lowercase', true, false], ['malformed', false, false], ['path1', true, false],
      ['path2', true, false], ['path3-old', true, false], ['public', false, true],
    ].map(([packet_hash, is_private, visibility_ok]) => ({ packet_hash, is_private, visibility_ok }));
    assert.deepEqual(await flags('packets'), expected);
    assert.deepEqual(await flags('packet_paths'), expected);
    assert.deepEqual((await pool.query(`SELECT packet_hash FROM packets p WHERE ${publicPacketPrivacySql('p')} ORDER BY packet_hash`)).rows,
      [{ packet_hash: 'external' }, { packet_hash: 'public' }]);
    const converged = await snapshot();
    assert.deepEqual((await repair()).updated, { packets: 0, packet_paths: 0, queue: 0 });
    assert.deepEqual(await snapshot(), converged);
  });

  await t.test('stale reviewed generation, active processing and missing public state abort without changes', async () => {
    await seedStaleRows();
    const before = await snapshot();
    await assert.rejects(convergePacketVisibility(pool, { apply: true, expectedGeneration: '999' }), /VISIBILITY_GENERATION_CHANGED/);
    assert.deepEqual(await snapshot(), before);
    await pool.query("UPDATE privacy_rematerialization_queue SET status = 'processing' WHERE target = 'packets'");
    const processing = await snapshot();
    await assert.rejects(repair(), /PRIVACY_REMATERIALIZATION_PROCESSING/);
    assert.deepEqual(await snapshot(), processing);
    await pool.query('DELETE FROM public_visibility_state');
    const missing = await snapshot();
    await assert.rejects(convergePacketVisibility(pool, { apply: true, expectedGeneration: '1' }), /VISIBILITY_GENERATION_CHANGED/);
    assert.deepEqual(await snapshot(), missing);
    await pool.query('INSERT INTO public_visibility_state(singleton,generation) VALUES (TRUE,1)');
  });

  await t.test('legacy unknown path sizes and NULL hops become hidden rather than NULL visibility', async () => {
    await seedStaleRows();
    const legacy = await pool.connect();
    try {
      // Model rows written before the classifiers existed. Timescale rejects
      // ALTER ... DISABLE TRIGGER on columnstore-enabled hypertables, so use
      // a transaction-local replica role only on this owned fixture client.
      await legacy.query('BEGIN');
      await legacy.query('SET LOCAL session_replication_role = replica');
      for (const target of ['packets', 'packet_paths'] as const) {
        await legacy.query(`INSERT INTO ${target}(time,packet_hash,topic,network,path_hashes,path_hash_size_bytes)
          VALUES (NOW(),'unknown-size','fixture','ukmesh',ARRAY['AA'],NULL),
                 (NOW(),'null-hop','fixture','ukmesh',ARRAY[NULL]::text[],1)`);
      }
      await legacy.query('COMMIT');
    } finally { await legacy.query('ROLLBACK'); legacy.release(); }
    assert.equal((await repair()).after?.needsConvergence, false);
    for (const target of ['packets', 'packet_paths'] as const) {
      assert.deepEqual((await flags(target)).filter((row) => ['unknown-size', 'null-hop'].includes(row.packet_hash)), [
        { packet_hash: 'null-hop', is_private: false, visibility_ok: false },
        { packet_hash: 'unknown-size', is_private: false, visibility_ok: false },
      ]);
    }
  });

  await t.test('missing materialization state stays fenced through aliases and scoped passes, full repair recreates it', async () => {
    await seedStaleRows();
    await pool.query('DELETE FROM packet_visibility_materialization_state');
    await pool.query(`INSERT INTO node_identity_aliases(source_node_id, canonical_node_id, confidence, reason)
      VALUES ($1, $2, 'high', 'fixture')`, [otherId, nodeId]);
    assert.equal((await pair()).materialized_generation, null);
    await pool.query("UPDATE privacy_rematerialization_queue SET status = 'pending', attempts = 0");
    await processNextPrivacyRematerialization(pool, { chunkPauseMs: 0 });
    await processNextPrivacyRematerialization(pool, { chunkPauseMs: 0 });
    assert.equal((await pair()).materialized_generation, null);
    assert.equal((await repair()).after?.needsConvergence, false);
  });

  await t.test('a policy change after dry-run invalidates the reviewed generation before any repair writes', async () => {
    await seedStaleRows();
    const writer = await pool.connect();
    const beforeFlags = await flags('packets');
    const reviewed = (await pair()).generation;
    await writer.query('BEGIN');
    await writer.query("UPDATE private_node_prefixes SET prefix = 'FF' WHERE prefix_size_bytes = 1");
    let starting!: () => void;
    const lockAttempt = new Promise<void>((resolve) => { starting = resolve; });
    const adaptedPool = { async connect() {
      const client = await pool.connect();
      return { query(text: string, values?: unknown[]) {
        if (text.startsWith('LOCK TABLE nodes')) starting();
        return client.query(text, values);
      }, release() { client.release(); } };
    } };
    const rejection = assert.rejects(convergePacketVisibility(adaptedPool as never,
      { apply: true, expectedGeneration: reviewed }), /VISIBILITY_GENERATION_CHANGED/);
    try { await lockAttempt; await writer.query('COMMIT'); await rejection; }
    finally { await writer.query('ROLLBACK'); writer.release(); }
    assert.deepEqual(await flags('packets'), beforeFlags);
    assert.deepEqual(await pair(), { generation: '2', materialized_generation: '1' });
  });

  await t.test('parallel-plan readback workaround is transaction local for dry-run and apply', async () => {
    await seedStaleRows();
    const client = await pool.connect();
    await client.query(`SET max_parallel_workers_per_gather = 2; SET min_parallel_table_scan_size = 0;
      SET parallel_setup_cost = 0; SET parallel_tuple_cost = 0; SET debug_parallel_query = on`);
    const adaptedPool = { async connect() {
      return { query: client.query.bind(client), release() {} };
    } };
    try {
      assert.equal((await convergePacketVisibility(adaptedPool as never)).mode, 'dry-run');
      assert.equal((await client.query('SHOW max_parallel_workers_per_gather')).rows[0].max_parallel_workers_per_gather, '2');
      assert.equal((await convergePacketVisibility(adaptedPool as never,
        { apply: true, expectedGeneration: (await pair()).generation })).after?.needsConvergence, false);
      assert.equal((await client.query('SHOW max_parallel_workers_per_gather')).rows[0].max_parallel_workers_per_gather, '2');
    } finally { await client.query('RESET ALL'); client.release(); }
  });

  await t.test('any write or readback failure rolls back flags, queue and generation together', async () => {
    await seedStaleRows();
    for (const stage of ['path-write', 'readback', 'queue', 'publish'] as const) {
      const before = await snapshot();
      const definition = stage === 'readback'
        ? "NEW.is_private := FALSE; NEW.visibility_ok := TRUE; RETURN NEW;"
        : "RAISE EXCEPTION 'fixture injected failure';";
      await pool.query(`CREATE OR REPLACE FUNCTION fixture_failure() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN ${definition} END $$`);
      const target = stage === 'path-write' || stage === 'readback' ? 'packet_paths'
        : stage === 'queue' ? 'privacy_rematerialization_queue' : 'packet_visibility_materialization_state';
      await pool.query(`CREATE TRIGGER fixture_failure BEFORE UPDATE ON ${target} FOR EACH ROW EXECUTE FUNCTION fixture_failure()`);
      try {
        await assert.rejects(repair(), stage === 'readback' ? /PRIVACY_CONVERGENCE_READBACK_FAILED/ : /fixture injected failure/);
        assert.deepEqual(await snapshot(), before, `all changes must roll back at ${stage}`);
      } finally { await pool.query(`DROP TRIGGER fixture_failure ON ${target}`); }
    }
  });

  await t.test('maintenance locks exclude concurrent node/prefix/queue/packet writers and a second repair', async () => {
    await seedStaleRows();
    const lockHolder = await pool.connect();
    await lockHolder.query('BEGIN');
    // Take the exact policy/data lock set on the tool connection and pause
    // before inspection. Other sessions must fail rather than interleave.
    let reached!: () => void;
    let resume!: () => void;
    const reachedLock = new Promise<void>((resolve) => { reached = resolve; });
    const paused = new Promise<void>((resolve) => { resume = resolve; });
    const adaptedPool = { async connect() {
      return {
        async query(text: string, values?: unknown[]) {
          const result = await lockHolder.query(text, values);
          if (text.startsWith('LOCK TABLE nodes')) { reached(); await paused; }
          return result;
        }, release() {},
      };
    } };
    // BEGIN above is only for setting up this owned client; end it before the
    // command's own transaction, which is the behavior under test.
    await lockHolder.query('ROLLBACK');
    const run = convergePacketVisibility(adaptedPool as never, { apply: true, expectedGeneration: (await pair()).generation });
    try {
      await reachedLock;
      for (const statement of [
        "UPDATE nodes SET name = 'competing public'",
        "DELETE FROM private_node_prefixes",
        "UPDATE privacy_rematerialization_queue SET status = 'processing'",
        "INSERT INTO packets(packet_hash,topic) VALUES ('competing','fixture')",
        "UPDATE packet_paths SET visibility_ok = TRUE",
      ]) {
        const writer = await pool.connect();
        try {
          await writer.query('BEGIN'); await writer.query("SET LOCAL lock_timeout = '100ms'");
          await assert.rejects(writer.query(statement), /lock timeout/);
    } finally { await writer.query('ROLLBACK'); writer.release(); }
      }
      await assert.rejects(convergePacketVisibility(pool, {
        apply: true, expectedGeneration: '1', statementTimeoutMs: 1_000,
      }), /statement timeout/);
    } finally { resume(); await run; lockHolder.release(); }
  });

  await t.test('compressed historical packets and paths above the old decompression budget converge completely', async () => {
    await reset(); await addNode();
    const fixtureRows = 100_001;
    for (const target of ['packets', 'packet_paths'] as const) {
      if (target === 'packet_paths') await pool.query(`ALTER TABLE packet_paths SET
        (timescaledb.compress, timescaledb.compress_segmentby = 'network', timescaledb.compress_orderby = 'time DESC')`);
      await pool.query(`INSERT INTO ${target}(time,packet_hash,topic,network,src_node_id)
        SELECT NOW() - INTERVAL '300 days', 'compressed-' || n, 'fixture', 'ukmesh', $1
        FROM generate_series(1,$2::int) n`, [nodeId, fixtureRows]);
      const chunks = await pool.query<{ name: string }>(`SELECT format('%I.%I', chunk_schema, chunk_name) AS name
        FROM timescaledb_information.chunks WHERE hypertable_name = $1`, [target]);
      for (const chunk of chunks.rows) await pool.query('SELECT compress_chunk($1::regclass)', [chunk.name]);
      const compressed = await pool.query(`SELECT bool_and(is_compressed) AS compressed
        FROM timescaledb_information.chunks WHERE hypertable_name = $1`, [target]);
      assert.equal(compressed.rows[0].compressed, true);
    }
    await addPrefix();
    const limited = await pool.connect();
    try {
      await limited.query('BEGIN');
      await limited.query('SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 100000');
      await assert.rejects(limited.query('UPDATE packets SET is_private = TRUE'), /decompress|tuple|limit/i);
    } finally { await limited.query('ROLLBACK'); limited.release(); }
    const result = await repair();
    assert.deepEqual(result.updated, { packets: fixtureRows, packet_paths: fixtureRows, queue: 0 });
    for (const target of ['packets', 'packet_paths'] as const) {
      assert.equal((await pool.query(`SELECT COUNT(*)::text AS count FROM ${target}
        WHERE is_private AND NOT visibility_ok`)).rows[0].count, String(fixtureRows));
    }
    assert.equal(result.after?.needsConvergence, false);
  });
});
