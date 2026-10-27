import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import pg from 'pg';
import { runMigrations } from '../db/migrations.js';
import { refreshNodeMapSuppressions } from '../nodes/mapSuppressions.js';
import { mapSuppressedNodes } from '../metrics.js';
import { createNodeRepository } from '../repositories/nodes.js';
import { networkFilters } from '../api/utils/networkFilters.js';
import { loadInitialMapNodes } from './initialStateNodes.js';

const databaseURL = process.env['TEST_MAP_DATABASE_URL'];
const scratch = databaseURL && new URL(databaseURL);
const safeScratch = scratch && scratch.pathname === '/map_stale_dedup_scratch'
  && ['localhost', '127.0.0.1'].includes(scratch.hostname);

test('map visibility, initial payload union, and migration on an isolated scratch database', {
  skip: safeScratch ? false : 'TEST_MAP_DATABASE_URL must name the local map_stale_dedup_scratch database',
  timeout: 60_000,
}, async (t) => {
  const admin = new pg.Pool({ connectionString: databaseURL, max: 1 });
  const name = `map_stale_dedup_scratch_${process.pid}_${Date.now()}`;
  await admin.query(`CREATE DATABASE "${name}"`);
  const url = new URL(databaseURL!);
  url.pathname = `/${name}`;
  const pool = new pg.Pool({ connectionString: url.toString(), max: 2 });
  const oldDatabaseURL = process.env['DATABASE_URL'];
  let closeDb: (() => Promise<void>) | undefined;
  try {
    await pool.query(readFileSync(new URL('../db/schema/base.sql', import.meta.url), 'utf8'));
    await runMigrations(pool);
    const migration = readFileSync(new URL('../db/migrations/067_node_map_suppressions.sql', import.meta.url), 'utf8');
    const count = async (label: string) => {
      const result = await pool.query<{ count: number }>('SELECT count(*)::integer AS count FROM node_map_suppressions');
      console.log(`[scratch ${name}] ${label}: SELECT count(*) FROM node_map_suppressions; => ${result.rows[0]!.count}`);
      return result.rows[0]!.count;
    };
    await t.test('migration is idempotent and starts empty', async () => {
      await pool.query(migration);
      await pool.query(migration);
      assert.equal(await count('migration applied three times'), 0);
    });

    const id = (prefix: string) => prefix.repeat(32);
    const fresh = id('AA'), suppressed = id('BB'), packetEndpoint = id('CC'), linkEndpoint = id('DD');
    const expired = id('EE'), otherSuppressed = id('AB'), otherFresh = id('AC'), pathFresh = id('AD');
    const privateNode = id('AE'), crossNetwork = id('AF'), nullIsland = id('BA'), otherRole = id('BC');
    await pool.query(`INSERT INTO nodes (node_id, public_key, name, network, role, lat, lon, last_seen)
      SELECT node_id, node_id, name, network, role, lat, -1, NOW() - age * INTERVAL '1 day'
      FROM (VALUES
        ($1, 'Same repeater', 'ukmesh', 2, 54.0, 1),
        ($2, 'Same repeater', 'ukmesh', 2, 54.0, 20),
        ($3, 'Lagged packet endpoint', 'ukmesh', 2, 55.0, 40),
        ($4, 'Lagged link endpoint', 'ukmesh', 2, 56.0, 40),
        ($5, 'Expired unreferenced node', 'ukmesh', 2, 57.0, 40),
        ($6, 'Other stale repeater', 'ukmesh', 2, 58.0, 20),
        ($7, 'Other active repeater', 'ukmesh', 2, 58.0, 1),
        ($8, 'Recent path evidence', 'ukmesh', 2, 59.0, 60),
        ($9, 'Private 🚫', 'ukmesh', 2, 60.0, 40),
        ($10, 'Other network', 'test', 2, 61.0, 40),
        ($11, 'Null island', 'ukmesh', 2, 0.0, 40),
        ($12, 'Other map-visible role', 'ukmesh', 0, 62.0, 1)
      ) AS fixture(node_id, name, network, role, lat, age)`,
    [fresh, suppressed, packetEndpoint, linkEndpoint, expired, otherSuppressed, otherFresh,
      pathFresh, privateNode, crossNetwork, nullIsland, otherRole]);
    await pool.query('UPDATE nodes SET last_path_evidence_at = NOW() WHERE node_id = $1', [pathFresh]);
    process.env['DATABASE_URL'] = url.toString();
    const db = await import('../db/index.js');
    closeDb = db.closeDb;
    const ids = (rows: { node_id: string }[]) => rows.map((row) => row.node_id).sort();
    const expectedMap = [fresh, otherFresh, pathFresh, otherRole].sort();

    await t.test('refresh stores evidence, sets gauge, and replaces its snapshot idempotently', async () => {
      assert.equal((await refreshNodeMapSuppressions(pool)).suppressedNodes, 2);
      assert.equal(await count('first refresh'), 2);
      await pool.query(migration);
      assert.equal(await count('migration reapplied with evidence'), 2);
      await refreshNodeMapSuppressions(pool);
      assert.equal(await count('repeat refresh'), 2);
      const evidence = await pool.query('SELECT * FROM node_map_suppressions WHERE node_id = $1', [suppressed]);
      assert.equal(evidence.rows[0].suppressed_by_node_id, fresh);
      assert.equal(evidence.rows[0].reason, 'stale-colocated');
      assert.equal(evidence.rows[0].distance_m, 0);
      assert.equal((await mapSuppressedNodes.get()).values[0]!.value, 2);
    });

    await t.test('REST and getNodes map opt-in share the same visibility contract', async () => {
      const repository = createNodeRepository((sql, params) => pool.query(sql, params));
      const rows = await repository.listPublicMapRows(['node_id'], networkFilters('ukmesh'),
        new Date().toISOString(), null, 100);
      assert.deepEqual(rows.map((row) => row['node_id']).sort(), expectedMap);
      assert.deepEqual(ids(await db.getNodes('ukmesh', undefined, 'slim', undefined, { mapVisible: true })), expectedMap);
    });

    await t.test('privacy index and inferred-path callers retain suppressed and expired nodes', async () => {
      const privacy = ids(await db.getNodes('ukmesh'));
      for (const retained of [suppressed, otherSuppressed, expired, privateNode]) assert.ok(privacy.includes(retained));
      const inferred = ids(await db.getNodes('ukmesh', undefined, 'slim'));
      for (const retained of [suppressed, otherSuppressed, expired]) assert.ok(inferred.includes(retained));
    });

    await t.test('WS payload excludes suppressed nodes and restores packet/link endpoints outside the window', async () => {
      const nodes = await loadInitialMapNodes(db.getNodes, 'ukmesh', undefined, [
        { src_node_id: packetEndpoint.toLowerCase(), rx_node_id: fresh.toLowerCase() },
        { src_node_id: privateNode }, { src_node_id: crossNetwork }, { src_node_id: nullIsland },
      ], [{ node_a_id: linkEndpoint, node_b_id: fresh }]);
      assert.deepEqual(ids(nodes), [...expectedMap, packetEndpoint, linkEndpoint].sort());
      assert.ok(!ids(nodes).includes(suppressed));
      assert.ok(!ids(nodes).includes(otherSuppressed));
    });

    await t.test('explicit initial references take precedence over suppression without bypassing privacy or scope', async () => {
      assert.deepEqual(ids(await loadInitialMapNodes(db.getNodes, 'ukmesh', undefined,
        [{ src_node_id: suppressed }, { src_node_id: privateNode }, { src_node_id: crossNetwork }], [])),
      [...expectedMap, suppressed].sort());
    });

    await t.test('failed replacement rolls back deletion and preserves the last successful gauge', async () => {
      await pool.query(`CREATE FUNCTION fail_map_suppression() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'injected suppression write failure'; END $$;
        CREATE TRIGGER fail_map_suppression BEFORE INSERT ON node_map_suppressions
          FOR EACH ROW EXECUTE FUNCTION fail_map_suppression()`);
      await assert.rejects(refreshNodeMapSuppressions(pool), /injected suppression write failure/);
      assert.equal(await count('failed refresh rolled back'), 2);
      assert.equal((await mapSuppressedNodes.get()).values[0]!.value, 2);
      await pool.query('DROP TRIGGER fail_map_suppression ON node_map_suppressions');
    });

    await t.test('recent path evidence and aging fresh neighbours remove obsolete suppressions', async () => {
      await pool.query('UPDATE nodes SET last_path_evidence_at = NOW() WHERE node_id = $1', [suppressed]);
      await pool.query(`UPDATE nodes SET last_seen = NOW() - INTERVAL '10 days' WHERE node_id = $1`, [otherFresh]);
      await refreshNodeMapSuppressions(pool);
      assert.equal(await count('obsolete suppressions cleared'), 0);
      assert.equal((await mapSuppressedNodes.get()).values[0]!.value, 0);
    });
  } finally {
    await closeDb?.();
    if (oldDatabaseURL === undefined) delete process.env['DATABASE_URL'];
    else process.env['DATABASE_URL'] = oldDatabaseURL;
    await pool.end();
    await admin.query(`DROP DATABASE "${name}" WITH (FORCE)`);
    await admin.end();
  }
});
