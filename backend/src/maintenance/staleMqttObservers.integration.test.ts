import assert from 'node:assert/strict';
import test from 'node:test';
import { cleanupInactiveNodes } from './staleMqttObservers.js';

// Optional, isolated PostgreSQL-WASM verification; never connects to a server.
// Point this at an independently installed PGlite module's file: URL.
const moduleUrl = process.env['TEST_NODE_CLEANUP_PGLITE_MODULE'];
const options = { skip: moduleUrl ? false : 'TEST_NODE_CLEANUP_PGLITE_MODULE is not configured' };

async function fixture() {
  const { PGlite } = await import(moduleUrl!);
  const db = await PGlite.create();
  await db.exec(`
    CREATE TABLE nodes (
      node_id TEXT PRIMARY KEY, role INTEGER, network TEXT,
      last_seen TIMESTAMPTZ, last_mqtt_observer_seen_at TIMESTAMPTZ,
      last_path_evidence_at TIMESTAMPTZ, last_predicted_online_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ
    );
    CREATE TABLE observer_region_observer_sightings (rx_node_id TEXT, iata TEXT, last_seen TIMESTAMPTZ);
    CREATE TABLE node_network_sightings (node_id TEXT, network TEXT, last_seen_at TIMESTAMPTZ);
    CREATE TABLE maintenance_removed_records (
      batch_id TEXT, source_table TEXT, record_data JSONB, reason TEXT
    );
    CREATE TABLE owner_grants (node_id TEXT, owner_id TEXT);
    CREATE TABLE packets (src_node_id TEXT, payload TEXT);
    INSERT INTO nodes (node_id, role, network, last_seen, created_at) VALUES
      ('companion', 1, 'ukmesh', NOW() - INTERVAL '31 days', NOW() - INTERVAL '90 days'),
      ('room-server', 3, 'ukmesh', NOW() - INTERVAL '31 days', NOW() - INTERVAL '90 days'),
      ('never-bridged', NULL, NULL, NOW() - INTERVAL '31 days', NOW() - INTERVAL '90 days'),
      ('repeater', 2, 'ukmesh', NOW() - INTERVAL '31 days', NOW() - INTERVAL '90 days'),
      ('recent', 1, 'ukmesh', NOW(), NOW() - INTERVAL '90 days'),
      ('mqtt-active', 1, 'ukmesh', NOW() - INTERVAL '31 days', NOW() - INTERVAL '90 days'),
      ('path-active', 3, 'ukmesh', NOW() - INTERVAL '31 days', NOW() - INTERVAL '90 days'),
      ('predicted-active', 3, 'ukmesh', NOW() - INTERVAL '31 days', NOW() - INTERVAL '90 days'),
      ('network-active', 1, 'ukmesh', NOW() - INTERVAL '31 days', NOW() - INTERVAL '90 days'),
      ('observer-active', 3, 'ukmesh', NOW() - INTERVAL '31 days', NOW() - INTERVAL '90 days'),
      ('new-node', 1, 'ukmesh', NULL, NOW()),
      ('test-node', 1, 'test', NOW() - INTERVAL '31 days', NOW() - INTERVAL '90 days'),
      ('unknown-age', NULL, NULL, NULL, NULL);
    UPDATE nodes SET last_mqtt_observer_seen_at = NOW() WHERE node_id = 'mqtt-active';
    UPDATE nodes SET last_path_evidence_at = NOW() WHERE node_id = 'path-active';
    UPDATE nodes SET last_predicted_online_at = NOW() WHERE node_id = 'predicted-active';
    INSERT INTO observer_region_observer_sightings VALUES
      ('companion', 'ABC', NOW() - INTERVAL '31 days'), ('recent', 'ABC', NOW()), ('observer-active', 'ABC', NOW());
    INSERT INTO node_network_sightings VALUES
      ('companion', 'ukmesh', NOW() - INTERVAL '31 days'), ('never-bridged', 'ukmesh', NOW() - INTERVAL '31 days'),
      ('recent', 'ukmesh', NOW()), ('network-active', 'ukmesh', NOW());
    INSERT INTO owner_grants VALUES ('companion', 'owner-1');
    INSERT INTO packets VALUES ('companion', 'retained history');
  `);
  let released = false;
  const cleanupPool = {
    async connect() {
      return {
        async query(text: string, values?: unknown[]) {
          const result = await db.query(text, values);
          return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length };
        },
        release() { released = true; },
      };
    },
  };
  const snapshot = async () => (await db.query(`
    SELECT jsonb_build_object(
      'nodes', (SELECT jsonb_agg(n ORDER BY node_id) FROM nodes n),
      'observers', (SELECT jsonb_agg(s ORDER BY rx_node_id) FROM observer_region_observer_sightings s),
      'networks', (SELECT jsonb_agg(s ORDER BY node_id) FROM node_network_sightings s),
      'archives', (SELECT jsonb_agg(a ORDER BY source_table, record_data::text) FROM maintenance_removed_records a)
    ) AS state
  `)).rows[0].state;
  return { db, cleanupPool, snapshot, released: () => released };
}

test('PostgreSQL archives inactive roles 1/3 and never-bridged nodes while retaining every fresh clock', options, async (t) => {
  const f = await fixture();
  t.after(() => f.db.close());
  const result = await cleanupInactiveNodes({ cleanupPool: f.cleanupPool, batchId: 'sql-batch' });
  assert.deepEqual(result, { batchId: 'sql-batch', candidates: 4, nodes: 4, observerSightings: 1, networkSightings: 2 });
  const remaining = await f.db.query('SELECT node_id FROM nodes ORDER BY node_id');
  assert.deepEqual(remaining.rows.map((row: { node_id: string }) => row.node_id), [
    'mqtt-active', 'network-active', 'new-node', 'observer-active', 'path-active', 'predicted-active', 'recent', 'test-node', 'unknown-age',
  ]);
  const archived = await f.db.query(`SELECT record_data->>'node_id' AS node_id
    FROM maintenance_removed_records WHERE source_table = 'nodes' ORDER BY node_id`);
  assert.deepEqual(archived.rows.map((row: { node_id: string }) => row.node_id), [
    'companion', 'never-bridged', 'repeater', 'room-server',
  ]);
  assert.equal((await f.db.query('SELECT COUNT(*)::int AS count FROM maintenance_removed_records')).rows[0].count, 7);
  assert.deepEqual((await f.db.query('SELECT * FROM owner_grants')).rows, [{ node_id: 'companion', owner_id: 'owner-1' }]);
  assert.deepEqual((await f.db.query('SELECT * FROM packets')).rows, [{ src_node_id: 'companion', payload: 'retained history' }]);
  assert.equal(f.released(), true);
});

for (const stage of ['archive', 'delete']) {
  test(`PostgreSQL rolls back the whole inactive cleanup on ${stage} failure`, options, async (t) => {
    const f = await fixture();
    t.after(() => f.db.close());
    await f.db.exec(`
      CREATE FUNCTION reject_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RAISE EXCEPTION 'injected ${stage} failure'; END $$;
      CREATE TRIGGER reject_cleanup BEFORE ${stage === 'archive' ? 'INSERT ON maintenance_removed_records' : 'DELETE ON nodes'}
      FOR EACH ROW EXECUTE FUNCTION reject_cleanup();
    `);
    const before = await f.snapshot();
    await assert.rejects(cleanupInactiveNodes({ cleanupPool: f.cleanupPool }), new RegExp(`injected ${stage} failure`));
    assert.deepEqual(await f.snapshot(), before);
    assert.equal(f.released(), true);
  });
}
