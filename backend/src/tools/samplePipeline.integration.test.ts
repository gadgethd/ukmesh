import { readPublicResponseGeneration } from '../privacy/responseGeneration.js';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import pg from 'pg';
import { BASE_SCHEMA_BOOTSTRAP_APPROVAL, BASE_SCHEMA_BOOTSTRAP_ENV, runMigrations } from '../db/migrations.js';
import { publicPacketPrivacySql } from '../platform/query/networkFilters.js';
import { generateSampleDataset } from './generateSampleDataset.js';
import { loadSampleDataset } from './loadSampleDataset.js';
import { prepareSampleDataset } from './prepareSampleDataset.js';
import { assertLocalDatabaseUrl } from './sampleData.js';
import { refreshSampleRecent } from './refreshSampleRecent.js';
import { withHeavyWorkAdmission } from '../analysis/heavyWorkAdmission.js';
import { setTimeout as delay } from 'node:timers/promises';

const endpoint = process.env['TEST_SAMPLE_DATABASE_URL'];

test('synthetic loader and derived-state preparation work on the complete migrated schema', {
  skip: endpoint ? false : 'owned TEST_SAMPLE_DATABASE_URL required', timeout: 180_000,
}, async (t) => {
  assertLocalDatabaseUrl(endpoint!);
  const admin = new pg.Pool({ connectionString: endpoint, max: 1 });
  const name = `sample_pipeline_${process.pid}_${Date.now()}`;
  const url = new URL(endpoint!);
  url.pathname = `/${name}`;
  await admin.query(`CREATE DATABASE ${name}`);
  const pool = new pg.Pool({ connectionString: url.href, max: 2 });
  const directory = await mkdtemp(path.join(os.tmpdir(), 'ukmesh-synthetic-'));
  const previous = process.env[BASE_SCHEMA_BOOTSTRAP_ENV];
  t.after(async () => {
    if (previous === undefined) delete process.env[BASE_SCHEMA_BOOTSTRAP_ENV];
    else process.env[BASE_SCHEMA_BOOTSTRAP_ENV] = previous;
    await pool.end();
    await admin.query(`DROP DATABASE ${name} WITH (FORCE)`);
    await admin.end();
    await rm(directory, { recursive: true, force: true });
  });
  await pool.query(await readFile(new URL('../db/schema/base.sql', import.meta.url), 'utf8'));
  await admin.query(await readFile(new URL('../../../scripts/bootstrap-backend-roles.sql', import.meta.url), 'utf8'));
  await admin.query(await readFile(new URL('../../../scripts/bootstrap-reader-roles.sql', import.meta.url), 'utf8'));
  process.env[BASE_SCHEMA_BOOTSTRAP_ENV] = BASE_SCHEMA_BOOTSTRAP_APPROVAL;
  await runMigrations(pool);
  assert.equal((await pool.query("SELECT relkind FROM pg_class WHERE oid='public.packets'::regclass")).rows[0].relkind, 'v');
  await generateSampleDataset(['--data-dir', directory, '--count', '25']);
  const args = ['--data-dir', directory, '--database-url', url.href];
  await loadSampleDataset([...args, '--reset', '--fast']);
  assert.equal((await pool.query('SELECT COUNT(*)::int AS n FROM mesh.packets')).rows[0].n, 25);
  assert.equal((await pool.query('SELECT COUNT(*)::int AS n FROM mesh.packets WHERE raw_hex IS NOT NULL AND observation_id IS NOT NULL')).rows[0].n, 25);
  assert.equal((await pool.query(`SELECT COUNT(*)::int AS n FROM mesh.packets p WHERE ${publicPacketPrivacySql('p')}`)).rows[0].n, 0,
    'bulk import must not certify public visibility');
  // Observe real competing owners without replacing the production pool or
  // lock implementation. Every barrier is released before fixture disposal.
  const waitFor = async (read: () => Promise<boolean>, description: string) => {
    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline) {
      if (await read()) return;
      await delay(10);
    }
    assert.fail(description);
  };
  const admissionClaims = async (fingerprint: string) => Number((await pool.query(`
    SELECT COUNT(*)::int AS n FROM pg_stat_activity
    WHERE datname = current_database() AND pid <> pg_backend_pid()
      AND md5(query) = $1
  `, [fingerprint])).rows[0].n);
  const blockedPreparers = async (blockerPid: number) => (await pool.query(`
    SELECT DISTINCT activity.pid
    FROM pg_stat_activity activity
    JOIN pg_locks locks ON locks.pid = activity.pid
    WHERE activity.datname = current_database() AND activity.pid <> pg_backend_pid()
      AND locks.locktype = 'relation' AND NOT locks.granted
      AND locks.relation = 'mesh.ingest_observation_ledger'::regclass
      AND $1::integer = ANY(pg_blocking_pids(activity.pid))
  `, [blockerPid])).rows;
  const assertCoverage = async (cutoffLowerBound: number) => {
    const ready = await readPublicResponseGeneration((sql) => pool.query(sql));
    const state = (await pool.query(`
      SELECT visibility_generation::int AS generation, covered_from, covered_through
      FROM mesh.multibyte_path_fact_state WHERE singleton
    `)).rows[0];
    assert.equal(state.generation, ready);
    const statistics = (await pool.query(`
      SELECT most_common_vals::text AS generations, most_common_freqs
      FROM pg_stats WHERE schemaname = 'mesh' AND tablename = 'multibyte_path_facts'
        AND attname = 'visibility_generation'
    `)).rows;
    assert.equal(statistics.length, 1, 'chart handoff requires analyzed fact-generation statistics');
    assert.equal(statistics[0].generations, `{${ready}}`,
      'fact statistics must describe the final published generation, not the pre-convergence generation');
    assert.deepEqual(statistics[0].most_common_freqs, [1]);
    assert.ok(new Date(state.covered_through).getTime() >= cutoffLowerBound,
      'coverage must include the independently recorded pre-call cutoff');
    const first = (await pool.query('SELECT MIN(time) AS first FROM mesh.packets')).rows[0].first as Date;
    assert.ok(new Date(state.covered_from).getTime() <= Math.min(first.getTime(), Date.now() - 8 * 86_400_000),
      'coverage must include retained history and at least the original eight-day window');
  };

  await t.test('fixture and independent chart owners exclude each other with the unchanged max2 pool', async () => {
    let releaseOpeningChart!: () => void;
    let markOpeningChart!: () => void;
    const openingChartReady = new Promise<void>(resolve => { markOpeningChart = resolve; });
    const openingChartGate = new Promise<void>(resolve => { releaseOpeningChart = resolve; });
    const openingChart = withHeavyWorkAdmission({ pool, workload: 'chart-refresh:ukmesh', task: async () => {
      markOpeningChart(); await openingChartGate;
    } });
    const blocker = new pg.Client({ connectionString: url.href, statement_timeout: 5_000 });
    let blockerConnected = false;
    let preparing: Promise<void> | undefined;
    let closingChart: Promise<void> | undefined;
    let closingChartEntered = false;
    const cutoffLowerBound = Date.now();
    try {
      await Promise.race([openingChartReady, openingChart.then(() => { throw new Error('opening chart ended before its barrier'); })]);
      const holders = (await pool.query(`
        SELECT DISTINCT activity.pid, md5(activity.query) AS fingerprint
        FROM pg_locks locks JOIN pg_stat_activity activity ON activity.pid = locks.pid
        WHERE locks.locktype = 'advisory' AND locks.granted
          AND locks.database = (SELECT oid FROM pg_database WHERE datname = current_database())
          AND activity.datname = current_database() AND activity.pid <> pg_backend_pid()
      `)).rows;
      assert.equal(holders.length, 1, 'the opening chart must own the unique granted advisory claim in this fixture');
      const fingerprint = String(holders[0].fingerprint);
      const before = (await pool.query(`SELECT generation::text FROM ops.public_visibility_state WHERE singleton`)).rows;
      const beforeRows = (await pool.query(`SELECT
        (SELECT COUNT(*)::int FROM mesh.ingest_observation_ledger) AS ledger,
        (SELECT COUNT(*)::int FROM mesh.packet_paths) AS paths,
        (SELECT COUNT(*)::int FROM mesh.multibyte_path_facts) AS facts`)).rows;
      preparing = prepareSampleDataset(url.href);
      void preparing.catch(() => {}); // Retain failure for the awaited result while barriers settle.
      await waitFor(async () => await admissionClaims(fingerprint) >= 2,
        'fixture preparation never joined the independently held analytical admission');
      assert.deepEqual((await pool.query(`SELECT generation::text FROM ops.public_visibility_state WHERE singleton`)).rows, before);
      assert.deepEqual((await pool.query(`SELECT
        (SELECT COUNT(*)::int FROM mesh.ingest_observation_ledger) AS ledger,
        (SELECT COUNT(*)::int FROM mesh.packet_paths) AS paths,
        (SELECT COUNT(*)::int FROM mesh.multibyte_path_facts) AS facts`)).rows, beforeRows,
      'preparation must not mutate derived inputs before admission');

      await blocker.connect();
      blockerConnected = true;
      const blockerPid = Number((await blocker.query('SELECT pg_backend_pid() AS pid')).rows[0].pid);
      await blocker.query('BEGIN');
      await blocker.query('LOCK TABLE mesh.ingest_observation_ledger IN ACCESS EXCLUSIVE MODE');
      releaseOpeningChart();
      await openingChart;
      await waitFor(async () => (await blockedPreparers(blockerPid)).length === 1, 'preparation failed to reach the owned work barrier');
      closingChart = withHeavyWorkAdmission({ pool, workload: 'chart-refresh:ukmesh', task: async () => {
        closingChartEntered = true;
        await assertCoverage(cutoffLowerBound);
      } });
      void closingChart.catch(() => {});
      await waitFor(async () => await admissionClaims(fingerprint) >= 2,
        'independent chart never attempted admission while fixture work held it');
      assert.equal(closingChartEntered, false, 'chart work must remain outside active fixture preparation');
      await blocker.query('ROLLBACK');
      await preparing;
      await closingChart;
      assert.equal(closingChartEntered, true);
      await assertCoverage(cutoffLowerBound);
    } finally {
      releaseOpeningChart();
      if (blockerConnected) await blocker.query('ROLLBACK').catch(() => {});
      try { await blocker.end(); }
      finally { await Promise.allSettled([openingChart, ...(preparing ? [preparing] : []), ...(closingChart ? [closingChart] : [])]); }
    }
  });
  assert.equal((await pool.query(`SELECT COUNT(*)::int AS n FROM mesh.packets p WHERE ${publicPacketPrivacySql('p')}`)).rows[0].n, 25);
  assert.equal((await pool.query('SELECT COUNT(*)::int AS n FROM mesh.multibyte_path_facts WHERE fully_decoded AND visibility_ok')).rows[0].n, 25);
  assert.ok(await readPublicResponseGeneration((sql) => pool.query(sql)) > 0);
  // A policy change after loading must remain authoritative. Preparation uses
  // the actual generation and the existing privacy implementation.
  await pool.query("UPDATE core.nodes SET name='Synthetic private 🚫' WHERE node_id LIKE 'AABB%'");
  await assert.rejects(readPublicResponseGeneration((sql) => pool.query(sql)), /GENERATION_UNAVAILABLE/,
    'pending rematerialization must fence derived HTTP responses before generation advances');
  await prepareSampleDataset(url.href);
  assert.equal((await pool.query(`SELECT COUNT(*)::int AS n FROM mesh.packets p WHERE ${publicPacketPrivacySql('p')}`)).rows[0].n, 0);
  // Reset must reach base relations, remove derived rows and permit a normal
  // trigger-maintained reload without relying on search_path.
  await loadSampleDataset([...args, '--reset']);
  await prepareSampleDataset(url.href);
  assert.equal((await pool.query('SELECT COUNT(*)::int AS n FROM mesh.packet_paths')).rows[0].n, 25);
  assert.equal((await pool.query('SELECT COUNT(*)::int AS n FROM mesh.packets WHERE packet_uid IS NOT NULL')).rows[0].n, 25);
  assert.equal((await pool.query('SELECT COUNT(*)::int AS n FROM mesh.ingest_observation_ledger')).rows[0].n, 25);
  assert.equal((await pool.query('SELECT COUNT(*)::int AS n FROM core.private_node_prefixes')).rows[0].n, 0);
  assert.equal((await pool.query(`SELECT COUNT(*)::int AS n FROM mesh.packets p WHERE ${publicPacketPrivacySql('p')}`)).rows[0].n, 25);
  await refreshSampleRecent(url.href);
  await refreshSampleRecent(url.href);
  assert.equal((await pool.query('SELECT COUNT(*)::int AS n FROM mesh.packets')).rows[0].n, 75,
    'the recent overlay must refresh a fixed set, not accumulate new packets');
  await t.test('a cancelled owned preparation releases admission to the independent chart owner', async () => {
    const blocker = new pg.Client({ connectionString: url.href, statement_timeout: 5_000 });
    let blockerConnected = false;
    let preparing: Promise<void> | undefined;
    try {
      await blocker.connect(); blockerConnected = true;
      const blockerPid = Number((await blocker.query('SELECT pg_backend_pid() AS pid')).rows[0].pid);
      await blocker.query('BEGIN');
      await blocker.query('LOCK TABLE mesh.ingest_observation_ledger IN ACCESS EXCLUSIVE MODE');
      preparing = prepareSampleDataset(url.href);
      void preparing.catch(() => {});
      await waitFor(async () => (await blockedPreparers(blockerPid)).length === 1, 'preparation did not reach its owned cancellation barrier');
      const targets = await blockedPreparers(blockerPid);
      assert.equal(targets.length, 1, 'only the exact disposable fixture preparation may be cancelled');
      assert.equal((await pool.query('SELECT pg_cancel_backend($1) AS cancelled', [targets[0].pid])).rows[0].cancelled, true);
      await assert.rejects(preparing, /canceling statement due to user request/);
      let chartEntered = false;
      await withHeavyWorkAdmission({ pool, workload: 'chart-refresh:ukmesh', task: async () => {
        chartEntered = true;
        assert.equal((await pool.query('SELECT 1 AS ok')).rows[0].ok, 1);
      } });
      assert.equal(chartEntered, true, 'failed fixture work must release its physical admission session');
    } finally {
      if (blockerConnected) await blocker.query('ROLLBACK').catch(() => {});
      try { await blocker.end(); }
      finally { if (preparing) await Promise.allSettled([preparing]); }
    }
  });

  assert.equal((await pool.query(`SELECT COUNT(*)::int AS n FROM mesh.packets p
    WHERE p.time > NOW() - INTERVAL '5 minutes' AND payload->>'_local_fixture' = 'recent-v1'
      AND ${publicPacketPrivacySql('p')}`)).rows[0].n, 50);
});
