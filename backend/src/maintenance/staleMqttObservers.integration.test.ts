import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { cleanupInactiveNodes, cleanupStaleMqttObservers } from './staleMqttObservers.js';

// Optional, isolated PostgreSQL-WASM verification; never connects to a server.
// Point this at an independently installed PGlite module's file: URL.
const moduleUrl = process.env['TEST_NODE_CLEANUP_PGLITE_MODULE'];
const options = { skip: moduleUrl ? false : 'TEST_NODE_CLEANUP_PGLITE_MODULE is not configured' };

async function fixture() {
  const { PGlite } = await import(moduleUrl!);
  const db = await PGlite.create();
  await db.exec(`
    CREATE TABLE nodes (
      node_id TEXT PRIMARY KEY, name TEXT, role INTEGER, network TEXT,
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
    CREATE TABLE private_node_prefixes (
      node_id TEXT REFERENCES nodes(node_id) ON DELETE CASCADE,
      network TEXT, prefix_size_bytes INTEGER, prefix TEXT,
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (node_id, network, prefix_size_bytes)
    );
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

for (const cleanup of [cleanupInactiveNodes, cleanupStaleMqttObservers]) {
  test(`PostgreSQL ${cleanup.name} preserves private identity and packet privacy with the current node trigger`, options, async (t) => {
    const f = await fixture();
    t.after(() => f.db.close());
    await f.db.exec(`ALTER TABLE packets
      ADD COLUMN rx_node_id TEXT, ADD COLUMN network TEXT DEFAULT 'ukmesh',
      ADD COLUMN path_hashes TEXT[], ADD COLUMN path_hash_size_bytes INTEGER,
      ADD COLUMN is_private BOOLEAN DEFAULT FALSE, ADD COLUMN visibility_ok BOOLEAN DEFAULT TRUE;`);
    // Execute only the real trigger definition in this disposable fixture.
    // Never apply migrations to an application database.
    const migration = await readFile(new URL('../db/migrations/049_guard_packet_privacy_rewrite.sql', import.meta.url), 'utf8');
    const triggerFunction = migration.match(/CREATE OR REPLACE FUNCTION sync_private_node_prefixes\(\)[\s\S]*?^\$\$;/m)?.[0];
    assert.ok(triggerFunction, 'the production privacy trigger definition must be present');
    await f.db.exec(`${triggerFunction}
      CREATE TRIGGER nodes_private_prefix_materialization AFTER INSERT OR UPDATE OR DELETE ON nodes
        FOR EACH ROW EXECUTE FUNCTION sync_private_node_prefixes();
      INSERT INTO nodes (node_id, name, role, network, last_seen, last_mqtt_observer_seen_at, created_at)
        VALUES ('private-node', 'Quiet 🚫 node', 2, 'ukmesh', NOW() - INTERVAL '45 days',
                NOW() - INTERVAL '45 days', NOW() - INTERVAL '90 days');
      INSERT INTO packets (src_node_id, payload, is_private, visibility_ok)
        VALUES ('private-node', 'private history', TRUE, FALSE);
      INSERT INTO nodes (node_id, name, role, network, last_seen, last_mqtt_observer_seen_at, created_at) VALUES
        ('prefix-only', 'Index protects privacy', 2, 'ukmesh', NOW() - INTERVAL '45 days',
         NOW() - INTERVAL '45 days', NOW() - INTERVAL '90 days'),
        ('marker-only', 'Marker protects 🚫 privacy', 2, 'ukmesh', NOW() - INTERVAL '45 days',
         NOW() - INTERVAL '45 days', NOW() - INTERVAL '90 days');
      INSERT INTO private_node_prefixes (node_id, network, prefix_size_bytes, prefix)
        VALUES ('prefix-only', 'ukmesh', 1, 'AB');
      DELETE FROM private_node_prefixes WHERE node_id = 'marker-only';`);
    const privacyNodes = "SELECT * FROM nodes WHERE node_id IN ('private-node', 'prefix-only', 'marker-only') ORDER BY node_id";
    const before = (await f.db.query(privacyNodes)).rows;
    await cleanup({ cleanupPool: f.cleanupPool });
    const packet = await f.db.query("SELECT is_private, visibility_ok FROM packets WHERE src_node_id = 'private-node'");
    assert.deepEqual(packet.rows, [{ is_private: true, visibility_ok: false }],
      'maintenance must not turn a private deletion into consent to publish its packet history');
    assert.deepEqual((await f.db.query(privacyNodes)).rows, before);
    assert.equal((await f.db.query('SELECT COUNT(*)::int AS count FROM private_node_prefixes')).rows[0].count, 4);
    assert.equal((await f.db.query("SELECT COUNT(*)::int AS count FROM maintenance_removed_records WHERE record_data->>'node_id' IN ('private-node', 'prefix-only', 'marker-only')")).rows[0].count, 0);
  });
}

test('PostgreSQL inactive cleanup retains the exact threshold boundary', options, async (t) => {
  const f = await fixture();
  t.after(() => f.db.close());
  const cleanupPool = {
    async connect() {
      const client = await f.cleanupPool.connect();
      return {
        ...client,
        async query(text: string, values?: unknown[]) {
          const result = await client.query(text, values);
          // Pin setup and selection to cleanup's identical transaction clock.
          if (text === 'BEGIN') await f.db.query(`INSERT INTO nodes (node_id, role, network, last_seen, created_at) VALUES
            ('exact-boundary', 1, 'ukmesh', NOW() - INTERVAL '30 days', NOW() - INTERVAL '90 days'),
            ('just-stale', 3, 'ukmesh', NOW() - INTERVAL '30 days 1 second', NOW() - INTERVAL '90 days');`);
          return result;
        },
      };
    },
  };
  const result = await cleanupInactiveNodes({ cleanupPool, batchId: 'boundary-batch' });
  assert.equal(result.nodes, 5);
  assert.deepEqual((await f.db.query("SELECT node_id FROM nodes WHERE node_id IN ('exact-boundary', 'just-stale')")).rows,
    [{ node_id: 'exact-boundary' }]);
});

test('PostgreSQL inactive cleanup is idempotent without duplicate archives', options, async (t) => {
  const f = await fixture();
  t.after(() => f.db.close());
  await cleanupInactiveNodes({ cleanupPool: f.cleanupPool, batchId: 'first-pass' });
  const before = await f.snapshot();
  const repeat = await cleanupInactiveNodes({ cleanupPool: f.cleanupPool });
  assert.deepEqual(repeat, { batchId: null, candidates: 0, nodes: 0, observerSightings: 0, networkSightings: 0 });
  assert.deepEqual(await f.snapshot(), before, 'a second pass must not produce duplicate archives or deletes');
});
