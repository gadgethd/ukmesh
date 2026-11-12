import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import pg from 'pg';
import { bootstrapBaseSchemaFixture } from '../db/fixtures/baseSchema.js';
import { BASE_SCHEMA_BOOTSTRAP_ENV } from '../db/migrations.js';
import { generateSampleDataset } from '../tools/generateSampleDataset.js';
import { loadSampleDataset } from '../tools/loadSampleDataset.js';
import { prepareSampleDataset } from '../tools/prepareSampleDataset.js';
import { rebuildRegionParticipation } from '../tools/rebuildRegionParticipation.js';
import { assertLocalDatabaseUrl } from '../tools/sampleData.js';
import { readPublicResponseGeneration } from '../privacy/responseGeneration.js';
import { regionRebuildSettings } from './rebuildSettings.js';
import { RegionSnapshotRefresher } from './snapshotRefresh.js';

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
  const pool = new pg.Pool({ connectionString: url.href, max: 3 });
  const directory = await mkdtemp(path.join(os.tmpdir(), 'ukmesh-region-fixture-'));
  const environment = ['DATABASE_URL','MESHCORE_WORKER_DATABASE_URL','REGION_SCOPE_ALLOW_EMPTY','REGION_SCOPE_MAX_PACKETS','REGION_SCOPE_MAX_NODES',BASE_SCHEMA_BOOTSTRAP_ENV];
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
  const query = (sql: string, params?: unknown[]) => pool.query(sql, params);
  const certificate = async () => (await pool.query(`SELECT visibility_generation::text, scoped_packets, decoded_packets, window_days FROM core.region_scope_run WHERE id`)).rows[0];
  const fingerprint = async () => (await pool.query(`SELECT md5(COALESCE(string_agg(value, '' ORDER BY value),'')) AS value FROM (
    SELECT to_jsonb(r)::text AS value FROM core.region_scope_stats r
    UNION ALL SELECT to_jsonb(r)::text FROM core.region_scope_members r
    UNION ALL SELECT to_jsonb(r)::text FROM core.repeater_scope_status r
    UNION ALL SELECT to_jsonb(r)::text FROM core.region_scope_unattributed r
    UNION ALL SELECT to_jsonb(r)::text FROM core.region_scope_run r) rows`)).rows[0].value;
  let runs = 0;
  const refresher = new RegionSnapshotRefresher({ query, settings: regionRebuildSettings(), run: async () => { runs++; await rebuildRegionParticipation(); } });
  const signal = new AbortController().signal;
  assert.equal(await refresher.runOnce(signal), 'rebuilt');
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
});
