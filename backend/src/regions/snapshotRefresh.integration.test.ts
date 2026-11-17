import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import pg from 'pg';
import { bootstrapBaseSchemaFixture, POST_MOVE_SEARCH_PATH } from '../db/fixtures/baseSchema.js';
import { BASE_SCHEMA_BOOTSTRAP_ENV } from '../db/migrations.js';
import { generateSampleDataset } from '../tools/generateSampleDataset.js';
import { loadSampleDataset } from '../tools/loadSampleDataset.js';
import { prepareSampleDataset } from '../tools/prepareSampleDataset.js';
import { rebuildRegionParticipation } from '../tools/rebuildRegionParticipation.js';
import { assertLocalDatabaseUrl } from '../tools/sampleData.js';
import { readPublicResponseGeneration } from '../privacy/responseGeneration.js';
import { regionRebuildSettings } from './rebuildSettings.js';
import { RegionSnapshotRefresher } from './snapshotRefresh.js';
import { runRegionSnapshotChild } from './snapshotChild.js';
import { withHeavyWorkAdmission } from '../analysis/heavyWorkAdmission.js';
import { convergePacketVisibility } from '../privacy/convergence.js';
import { pathToFileURL } from 'node:url';

const endpoint = process.env['TEST_SAMPLE_DATABASE_URL'];
test('region repair follows real privacy convergence, certifies complete empty results and rejects partial publication', {
  skip: endpoint ? false : 'owned TEST_SAMPLE_DATABASE_URL required', timeout: 180_000,
}, async t => {
  assertLocalDatabaseUrl(endpoint!);
  t.mock.method(console, 'log', () => undefined);
  t.mock.method(console, 'error', () => undefined);
  const admin = new pg.Pool({ connectionString: endpoint, max: 1 });
  const name = `region_snapshot_${process.pid}_${Date.now()}`;
  const url = new URL(endpoint!); url.pathname = `/${name}`;
  await admin.query(`CREATE DATABASE ${name}`);
  const pool = new pg.Pool({ connectionString: url.href, max: 3, options: POST_MOVE_SEARCH_PATH });
  const directory = await mkdtemp(path.join(os.tmpdir(), 'ukmesh-region-fixture-'));
  const environment = ['DATABASE_URL','MESHCORE_WORKER_DATABASE_URL','REGION_SCOPE_ALLOW_EMPTY','REGION_SCOPE_MAX_PACKETS','REGION_SCOPE_MAX_NODES','DATABASE_ANALYTICS_RANDOM_PAGE_COST','DATABASE_ANALYTICS_EFFECTIVE_CACHE_SIZE',BASE_SCHEMA_BOOTSTRAP_ENV];
  const previous = new Map(environment.map(key => [key, process.env[key]]));
  t.after(async () => {
    for (const [key, value] of previous) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
    await pool.end(); await admin.query(`DROP DATABASE ${name} WITH (FORCE)`); await admin.end();
    await rm(directory, { recursive: true, force: true });
  });
  await bootstrapBaseSchemaFixture(pool);
  await generateSampleDataset(['--data-dir', directory, '--count', '25']);
  await loadSampleDataset(['--data-dir', directory, '--database-url', url.href, '--reset', '--fast']);
  await prepareSampleDataset(url.href);
  process.env['DATABASE_URL'] = url.href;
  process.env['MESHCORE_WORKER_DATABASE_URL'] = url.href;
  process.env['REGION_SCOPE_ALLOW_EMPTY'] = '1';
  process.env['DATABASE_ANALYTICS_RANDOM_PAGE_COST'] = '4';
  process.env['DATABASE_ANALYTICS_EFFECTIVE_CACHE_SIZE'] = '512MB';
  const plannerSessions: Array<{ random_page_cost: string; effective_cache_size: string }> = [];
  const connect = pg.Pool.prototype.connect as (...args: unknown[]) => any;
  t.mock.method(pg.Pool.prototype, 'connect', function(this: pg.Pool, ...args: unknown[]) {
    const pending = connect.apply(this, args);
    if (args.length || this.options.application_name !== 'meshcore-regions-rebuild') return pending;
    return pending.then(async (client: pg.PoolClient) => {
      try {
        const result = await client.query("SELECT current_setting('random_page_cost') AS random_page_cost, current_setting('effective_cache_size') AS effective_cache_size");
        plannerSessions.push(result.rows[0]); return client;
      } catch (error) { client.release(true); throw error; }
    });
  });
  const query = (sql: string, params?: unknown[]) => pool.query(sql, params);
  const certificate = async () => (await pool.query(`SELECT visibility_generation::text, scoped_packets, decoded_packets, window_days FROM core.region_scope_run WHERE id`)).rows[0];
  const fingerprint = async () => (await pool.query(`SELECT md5(COALESCE(string_agg(value, '' ORDER BY value),'')) AS value FROM (
    SELECT to_jsonb(r)::text AS value FROM core.region_scope_stats r
    UNION ALL SELECT to_jsonb(r)::text FROM core.region_scope_members r
    UNION ALL SELECT to_jsonb(r)::text FROM core.repeater_scope_status r
    UNION ALL SELECT to_jsonb(r)::text FROM core.region_scope_unattributed r
    UNION ALL SELECT to_jsonb(r)::text FROM core.region_scope_run r) rows`)).rows[0].value;
  let runs = 0;
  const refresher = new RegionSnapshotRefresher({ query, settings: regionRebuildSettings(), run: async () => { runs++; await rebuildRegionParticipation(); return 'completed'; } });
  const signal = new AbortController().signal;
  assert.equal(await refresher.runOnce(signal), 'rebuilt');
  assert.ok(plannerSessions.length > 0, 'actual analytical admission session observed');
  for (const observed of plannerSessions) assert.deepEqual(observed,
    { random_page_cost: '4', effective_cache_size: '512MB' });
  const firstGeneration = await readPublicResponseGeneration(query);
  assert.equal((await certificate()).visibility_generation, String(firstGeneration));
  assert.equal(Number((await certificate()).scoped_packets), 13);
  assert.equal(Number((await pool.query('SELECT COUNT(*) AS n FROM core.region_scope_members')).rows[0].n), 2);
  const beforeCapacity = await fingerprint();
  process.env['REGION_SCOPE_MAX_PACKETS'] = '1';
  await assert.rejects(rebuildRegionParticipation(), /PACKET_CAPACITY/);
  assert.equal(await fingerprint(), beforeCapacity, 'capacity rejection preserves every committed snapshot table');
  delete process.env['REGION_SCOPE_MAX_PACKETS'];
  const publisher = await pool.connect();
  try {
    await publisher.query("SELECT pg_advisory_lock(hashtext('meshcore-region-snapshot-publish'))");
    await assert.rejects(rebuildRegionParticipation(), /PUBLISHER_BUSY/);
    assert.equal(await fingerprint(), beforeCapacity);
  } finally {
    await publisher.query("SELECT pg_advisory_unlock(hashtext('meshcore-region-snapshot-publish'))"); publisher.release();
  }
  // Real privacy triggers enqueue rematerialization. A pending generation
  // withholds the producer until the existing convergence tool certifies it.
  const blocker = await pool.connect();
  let raced: Promise<unknown> | undefined;
  try {
    await blocker.query('BEGIN');
    await blocker.query('LOCK core.region_scope_stats IN SHARE MODE');
    // Pause the real producer after it has read the source, at publication.
    raced = rebuildRegionParticipation().then(() => undefined, error => error);
    let waiting = false;
    for (let attempt = 0; attempt < 60; attempt++) {
      waiting = (await pool.query(`SELECT EXISTS (SELECT 1 FROM pg_stat_activity
        WHERE application_name='meshcore-regions-rebuild'
          AND query='DELETE FROM core.region_scope_stats' AND wait_event_type='Lock') AS waiting`)).rows[0].waiting;
      if (waiting) break;
      await delay(25);
    }
    assert.equal(waiting, true, 'the publication transaction reaches the held table lock');
    await pool.query("UPDATE core.nodes SET name='Synthetic private 🚫' WHERE node_id LIKE 'AABB%'");
    await blocker.query('COMMIT');
    assert.match(String(await raced), /GENERATION_UNAVAILABLE|VISIBILITY_CHANGED/);
    assert.equal(await fingerprint(), beforeCapacity, 'a concurrent privacy change rolls back all snapshot writes');
  } finally {
    await blocker.query('ROLLBACK'); blocker.release();
    await raced;
  }
  await assert.rejects(readPublicResponseGeneration(query), /GENERATION_UNAVAILABLE/);
  assert.equal(await refresher.runOnce(signal), 'unavailable');
  assert.equal(runs, 1);
  assert.equal(await fingerprint(), beforeCapacity);
  await prepareSampleDataset(url.href);
  const privateGeneration = await readPublicResponseGeneration(query);
  assert.ok(privateGeneration > firstGeneration);
  assert.equal(await refresher.runOnce(signal), 'rebuilt');
  const empty = await certificate();
  assert.equal(empty.visibility_generation, String(privateGeneration));
  assert.equal(Number(empty.scoped_packets), 0);
  assert.equal(Number(empty.decoded_packets), 0);
  assert.equal(Number((await pool.query('SELECT COUNT(*) AS n FROM core.region_scope_members')).rows[0].n), 0);
  // A valid public corpus can become available again without an HTTP rebuild.
  await pool.query("UPDATE core.nodes SET name='Synthetic west' WHERE node_id LIKE 'AABB%'");
  await prepareSampleDataset(url.href);
  assert.equal(await refresher.runOnce(signal), 'rebuilt');
  assert.equal(Number((await certificate()).scoped_packets), 13);
  assert.equal(Number((await pool.query('SELECT COUNT(*) AS n FROM core.region_scope_members')).rows[0].n), 2);
  // Corrupting the decode inputs must not convert a nonempty corpus into a
  // successful empty publication, even with the automatic empty-result opt-in.
  const previousSnapshot = await fingerprint();
  await pool.query("UPDATE mesh.packets SET raw_hex='FF' WHERE route_type IN (0,3)");
  await assert.rejects(rebuildRegionParticipation(), /no scoped packets decoded/);
  assert.equal(await fingerprint(), previousSnapshot);
  assert.equal(await refresher.runOnce(signal), 'fresh');


  const waitFor = async (read: () => Promise<boolean>, description: string) => {
    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline) {
      if (await read()) return;
      await delay(10);
    }
    assert.fail(description);
  };
  const regionActivity = async () => (await pool.query<{ pid: number; waiting: boolean; claims: number }>(`
    SELECT activity.pid, activity.wait_event_type = 'Lock' AS waiting,
      (SELECT COUNT(*)::int FROM pg_locks locks
        WHERE locks.pid = activity.pid AND locks.locktype = 'advisory' AND locks.granted) AS claims
    FROM pg_stat_activity activity
    WHERE activity.datname = current_database() AND activity.application_name = 'meshcore-regions-rebuild'
  `)).rows;
  const childScript = path.join(directory, 'region-child.mjs');
  // Existing snapshotChild.script seam; execute the actual source export through
  // the installed loader without requiring an unrelated stale dist build.
  await writeFile(childScript, `import { tsImport } from ${JSON.stringify(new URL('../../node_modules/tsx/dist/esm/api/index.mjs', import.meta.url).href)};
const { rebuildRegionParticipation } = await tsImport(${JSON.stringify(new URL('../tools/rebuildRegionParticipation.ts', import.meta.url).href)}, { parentURL: import.meta.url });
await rebuildRegionParticipation();
`);
  const childOptions = { script: pathToFileURL(childScript), timeoutMs: 30_000,
    env: { DATABASE_URL: url.href, MESHCORE_WORKER_DATABASE_URL: url.href } };
  // Run the actual source CLI entrypoint and its fixed background argument,
  // rather than relying on a dist build from a different candidate.
  const backgroundOptions = { ...childOptions,
    script: new URL('../tools/rebuildRegionParticipation.ts', import.meta.url),
    env: { ...childOptions.env, NODE_OPTIONS: `${process.env['NODE_OPTIONS'] ?? ''} --import=${new URL('../../node_modules/tsx/dist/loader.mjs', import.meta.url).href}`.trim() },
  };

  await t.test('the actual region export waits for shared admission and reads the new generation after admission', async () => {
    const before = await fingerprint();
    const beforeGeneration = await readPublicResponseGeneration(query);
    let entered = false, release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const holding = new AbortController();
    const held = withHeavyWorkAdmission({ pool, workload: 'fixture-prepare', signal: holding.signal, task: async () => {
      entered = true; await gate;
    } }).then(() => undefined, error => error);
    let running: Promise<unknown> | undefined;
    try {
      await waitFor(async () => entered, 'the external fixture owner acquires heavy admission');
      running = rebuildRegionParticipation().then(() => undefined, error => error);
      await waitFor(async () => (await regionActivity()).length === 1, 'the region child connects with one admission client');
      const waiting = await regionActivity();
      assert.equal(waiting[0]!.claims, 0, 'private publisher ownership starts only after shared admission');
      assert.equal(await fingerprint(), before, 'no snapshot changes while admission is unavailable');
      await pool.query("UPDATE core.nodes SET name='Synthetic private 🚫' WHERE node_id LIKE 'AABB%'");
      const desired = (await pool.query('SELECT generation::text FROM ops.public_visibility_state WHERE singleton')).rows[0].generation;
      await convergePacketVisibility(pool, { apply: true, expectedGeneration: desired });
      const current = await readPublicResponseGeneration(query);
      assert.ok(current > beforeGeneration);
      release();
      assert.equal(await held, undefined);
      assert.equal(await running, undefined);
      assert.equal((await certificate()).visibility_generation, String(current));
      assert.equal(Number((await certificate()).scoped_packets), 0, 'current private corpus is certified empty');
      await waitFor(async () => (await regionActivity()).length === 0, 'owned region pool closes after completion');
    } finally {
      release(); holding.abort(new Error('owned admission holder cleanup')); await held; await running;
    }
  });

  await t.test('a failed task discards a session retaining the existing private publisher claim', async () => {
    const expected = new Error('owned dirty-session failure');
    let dirtyPid = 0;
    await assert.rejects(withHeavyWorkAdmission({ pool, workload: 'region-snapshot', task: async session => {
      dirtyPid = Number((await session.query('SELECT pg_backend_pid() AS pid')).rows[0].pid);
      assert.equal((await session.query("SELECT pg_try_advisory_lock(hashtext('meshcore-region-snapshot-publish')) AS locked")).rows[0].locked, true);
      assert.equal(Number((await session.query("SELECT COUNT(*)::int AS n FROM pg_locks WHERE pid = pg_backend_pid() AND locktype = 'advisory' AND granted")).rows[0].n), 2);
      throw expected; // Deliberately leave the nested session claim for its owning helper.
    } }), error => error === expected);
    await waitFor(async () => !(await pool.query('SELECT 1 FROM pg_stat_activity WHERE pid=$1', [dirtyPid])).rowCount,
      'failed admission session is destroyed rather than returned idle');
    await withHeavyWorkAdmission({ pool, workload: 'region-snapshot', task: async session => {
      assert.notEqual(Number((await session.query('SELECT pg_backend_pid() AS pid')).rows[0].pid), dirtyPid);
      const privateLock = await session.query("SELECT pg_try_advisory_lock(hashtext('meshcore-region-snapshot-publish')) AS locked");
      assert.equal(privateLock.rows[0].locked, true, 'both shared and private claims are reacquired');
      assert.equal((await session.query("SELECT pg_advisory_unlock(hashtext('meshcore-region-snapshot-publish')) AS unlocked")).rows[0].unlocked, true);
    } });
  });

  await t.test('a busy background child defers without publication and the next poll publishes the fresh generation', async () => {
    const before = await fingerprint();
    const beforeGeneration = await readPublicResponseGeneration(query);
    let release!: () => void, entered = false, holderPid = 0;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const held = withHeavyWorkAdmission({ pool, workload: 'fixture-prepare', task: async session => {
      holderPid = Number((await session.query('SELECT pg_backend_pid() AS pid')).rows[0].pid);
      entered = true; await gate;
    } }).then(() => undefined, error => error);
    const shutdown = new AbortController();
    const refresh = new RegionSnapshotRefresher({ query, settings: regionRebuildSettings(),
      run: signal => runRegionSnapshotChild(signal, backgroundOptions) });
    let running: Promise<unknown> | undefined;
    try {
      await waitFor(async () => entered, 'the real fixture owner holds admission');
      // Two real privacy transitions guarantee a new ready generation while
      // leaving the final corpus private/empty, including the earlier bad raw.
      await pool.query("UPDATE core.nodes SET name='Synthetic west' WHERE node_id LIKE 'AABB%'");
      await pool.query("UPDATE core.nodes SET name='Synthetic private 🚫' WHERE node_id LIKE 'AABB%'");
      const desired = (await pool.query('SELECT generation::text FROM ops.public_visibility_state WHERE singleton')).rows[0].generation;
      await convergePacketVisibility(pool, { apply: true, expectedGeneration: desired });
      const current = await readPublicResponseGeneration(query);
      assert.ok(current > beforeGeneration);
      running = refresh.runOnce(shutdown.signal);
      assert.equal(await running, 'deferred', 'busy child closes before the external owner releases');
      assert.equal(await fingerprint(), before, 'deferral preserves every published table and certificate');
      assert.equal((await regionActivity()).length, 0, 'the deferred child leaves no owned session');
      assert.equal(Number((await pool.query("SELECT COUNT(*)::int AS n FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND granted", [holderPid])).rows[0].n), 1,
        'the external owner still owns the shared claim');
      release();
      assert.equal(await held, undefined);
      running = refresh.runOnce(shutdown.signal);
      assert.equal(await running, 'rebuilt');
      assert.equal((await certificate()).visibility_generation, String(current));
      assert.equal(Number((await certificate()).scoped_packets), 0);
      assert.equal(await refresh.runOnce(shutdown.signal), 'fresh');
      assert.equal((await regionActivity()).length, 0);
    } finally {
      shutdown.abort(new Error('owned background deferral cleanup')); release(); await held; await running;
    }
  });

  await t.test('a private publisher refusal remains a real background failure', async () => {
    const before = await fingerprint();
    const publisher = await pool.connect();
    try {
      await publisher.query("SELECT pg_advisory_lock(hashtext('meshcore-region-snapshot-publish'))");
      await assert.rejects(runRegionSnapshotChild(new AbortController().signal, backgroundOptions), /REGION_SNAPSHOT_CHILD_FAILED.*PUBLISHER_BUSY/s);
      assert.equal(await fingerprint(), before);
      assert.equal((await regionActivity()).length, 0);
    } finally {
      try { assert.equal((await publisher.query("SELECT pg_advisory_unlock(hashtext('meshcore-region-snapshot-publish')) AS unlocked")).rows[0].unlocked, true); }
      finally { publisher.release(); }
    }
  });

  await t.test('cancelling the actual child at publication releases both same-session claims and rolls back', async () => {
    const before = await fingerprint();
    const blocker = await pool.connect();
    const shutdown = new AbortController();
    let running: Promise<unknown> | undefined;
    try {
      await blocker.query('BEGIN');
      await blocker.query('LOCK core.region_scope_stats IN SHARE MODE');
      const blockerPid = Number((await blocker.query('SELECT pg_backend_pid() AS pid')).rows[0].pid);
      const relationOid = Number((await pool.query("SELECT 'core.region_scope_stats'::regclass::oid AS oid")).rows[0].oid);
      const blockedPublishers = async () => (await pool.query<{ pid: number; claims: number }>(`
        SELECT DISTINCT activity.pid,
          (SELECT COUNT(*)::int FROM pg_locks owned
            WHERE owned.pid = activity.pid AND owned.locktype = 'advisory' AND owned.granted) AS claims
        FROM pg_stat_activity activity
        JOIN pg_locks waiting ON waiting.pid = activity.pid
        WHERE activity.datname = current_database() AND activity.application_name = 'meshcore-regions-rebuild'
          AND waiting.locktype = 'relation' AND NOT waiting.granted AND waiting.relation = $2::oid
          AND $1::integer = ANY(pg_blocking_pids(activity.pid))
      `, [blockerPid, relationOid])).rows;
      running = runRegionSnapshotChild(shutdown.signal, backgroundOptions).then(() => undefined, error => error);
      await waitFor(async () => (await blockedPublishers()).length === 1,
        'one actual child session reaches its owned publication relation barrier');
      const activity = await regionActivity();
      assert.ok(activity.length <= 2, 'the real child retains max2 physical clients');
      const publishing = await blockedPublishers();
      assert.equal(publishing.length, 1);
      assert.equal(publishing[0]!.claims, 2, 'publisher transaction and both claims share one physical session');
      const reason = new Error('owned region publication cancelled');
      shutdown.abort(reason);
      assert.equal(await running, reason, 'parent settles the real child before returning cancellation');
      await blocker.query('ROLLBACK');
      await waitFor(async () => (await regionActivity()).length === 0, 'child sessions and both claims are gone');
      assert.equal(await fingerprint(), before, 'cancelled publication never commits partial snapshot tables');
      await rebuildRegionParticipation();
      assert.equal((await certificate()).visibility_generation, String(await readPublicResponseGeneration(query)));
    } finally {
      shutdown.abort(new Error('owned region test cleanup'));
      try { await blocker.query('ROLLBACK'); }
      finally { blocker.release(); await running; }
    }
  });

  await t.test('an admitted background child keeps its deadline and rolls back both claims', async () => {
    const before = await fingerprint();
    const blocker = await pool.connect();
    let running: Promise<unknown> | undefined;
    try {
      await blocker.query('BEGIN');
      await blocker.query('LOCK core.region_scope_stats IN SHARE MODE');
      const blockerPid = Number((await blocker.query('SELECT pg_backend_pid() AS pid')).rows[0].pid);
      const relationOid = Number((await pool.query("SELECT 'core.region_scope_stats'::regclass::oid AS oid")).rows[0].oid);
      running = runRegionSnapshotChild(new AbortController().signal, { ...backgroundOptions, timeoutMs: 5_000 }).then(() => undefined, error => error);
      await waitFor(async () => (await pool.query(`SELECT EXISTS (
        SELECT 1 FROM pg_stat_activity activity JOIN pg_locks waiting ON waiting.pid=activity.pid
        WHERE activity.datname=current_database() AND activity.application_name='meshcore-regions-rebuild'
          AND waiting.locktype='relation' AND NOT waiting.granted AND waiting.relation=$2::oid
          AND $1::integer=ANY(pg_blocking_pids(activity.pid))) AS blocked`, [blockerPid, relationOid])).rows[0].blocked,
      'the admitted background child reaches the real publication barrier');
      const activity = await regionActivity();
      assert.ok(activity.length <= 2);
      assert.equal(activity.filter(row => row.claims === 2).length, 1);
      assert.match(String(await running), /REGION_SNAPSHOT_DEADLINE/);
      await blocker.query('ROLLBACK');
      await waitFor(async () => (await regionActivity()).length === 0,
        'the deadline settles the actual child and both claims after PostgreSQL observes disconnect');
      assert.equal(await fingerprint(), before);
    } finally {
      try { await blocker.query('ROLLBACK'); }
      finally { blocker.release(); await running; }
    }
    await rebuildRegionParticipation();
    assert.equal((await certificate()).visibility_generation, String(await readPublicResponseGeneration(query)));
  });

  await t.test('the child deadline still bounds an explicitly blocking manual rebuild', async () => {
    const before = await fingerprint();
    let entered = false, release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const holding = new AbortController();
    const held = withHeavyWorkAdmission({ pool, workload: 'chart-refresh:ukmesh', signal: holding.signal,
      task: async () => { entered = true; await gate; } }).then(() => undefined, error => error);
    let running: Promise<unknown> | undefined;
    try {
      await waitFor(async () => entered, 'the external chart owner acquires heavy admission');
      running = runRegionSnapshotChild(new AbortController().signal, { ...childOptions, timeoutMs: 5_000 }).then(() => undefined, error => error);
      await waitFor(async () => (await regionActivity()).length === 1, 'child waits on shared admission');
      assert.equal((await regionActivity())[0]!.claims, 0);
      const result = await running;
      assert.ok(result instanceof Error && /REGION_SNAPSHOT_DEADLINE/.test(result.message));
      await waitFor(async () => (await regionActivity()).length === 0, 'deadline settles child and its admission session');
      assert.equal(await fingerprint(), before);
    } finally {
      release(); holding.abort(new Error('owned admission holder cleanup')); await held; await running;
    }
    await rebuildRegionParticipation();
    assert.equal((await certificate()).visibility_generation, String(await readPublicResponseGeneration(query)));
  });
});
