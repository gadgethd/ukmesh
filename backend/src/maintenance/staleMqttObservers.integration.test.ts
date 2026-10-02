import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { cleanupInactiveNodes, cleanupStaleMqttObservers } from './staleMqttObservers.js';

// Optional, isolated PostgreSQL-WASM verification; never connects to a server.
// Point this at an independently installed PGlite module's file: URL.
const moduleUrl = process.env['TEST_NODE_CLEANUP_PGLITE_MODULE'];
const options = { skip: moduleUrl ? false : 'TEST_NODE_CLEANUP_PGLITE_MODULE is not configured' };

async function installPrivacyTriggers(db: { exec(sql: string): Promise<unknown> }) {
  await db.exec(`
    ALTER TABLE packets
      ADD COLUMN rx_node_id TEXT, ADD COLUMN network TEXT DEFAULT 'ukmesh',
      ADD COLUMN path_hashes TEXT[], ADD COLUMN path_hash_size_bytes INTEGER,
      ADD COLUMN is_private BOOLEAN DEFAULT FALSE, ADD COLUMN visibility_ok BOOLEAN DEFAULT TRUE;
    CREATE TABLE packet_paths (
      src_node_id TEXT, rx_node_id TEXT, network TEXT DEFAULT 'ukmesh',
      path_hashes TEXT[], path_hash_size_bytes INTEGER,
      is_private BOOLEAN DEFAULT FALSE, visibility_ok BOOLEAN DEFAULT TRUE
    );
  `);
  // Install the current function bodies and relevant trigger chain into this
  // disposable fixture. Never execute a project migration or connect to a server.
  for (const [file, names] of [
    ['042_packet_visibility_fence.sql', ['lock_packet_visibility_for_node_privacy_change', 'bump_public_visibility_generation', 'bump_visibility_for_identity_table']],
    ['049_guard_packet_privacy_rewrite.sql', ['sync_private_node_prefixes']],
    ['051_reset_readiness.sql', ['meshcore_path_matches_private', 'meshcore_path_is_valid', 'classify_packet_path_privacy', 'rematerialize_packet_path_privacy']],
  ] as const) {
    const source = await readFile(new URL(`../db/migrations/${file}`, import.meta.url), 'utf8');
    for (const name of names) {
      const definition = source.match(new RegExp(`CREATE OR REPLACE FUNCTION ${name}\\([\\s\\S]*?^\\$\\$;`, 'm'))?.[0];
      assert.ok(definition, `the production ${name} definition must be present`);
      await db.exec(definition);
    }
  }
  await db.exec(`
    CREATE TRIGGER nodes_packet_visibility_serialization BEFORE INSERT OR UPDATE OR DELETE ON nodes
      FOR EACH ROW EXECUTE FUNCTION lock_packet_visibility_for_node_privacy_change();
    CREATE TRIGGER nodes_private_prefix_materialization AFTER INSERT OR UPDATE OR DELETE ON nodes
      FOR EACH ROW EXECUTE FUNCTION sync_private_node_prefixes();
    CREATE TRIGGER nodes_public_visibility_generation AFTER INSERT OR UPDATE OR DELETE ON nodes
      FOR EACH ROW EXECUTE FUNCTION bump_public_visibility_generation();
    CREATE TRIGGER nodes_private_zz_packet_path_materialization AFTER INSERT OR UPDATE OR DELETE ON nodes
      FOR EACH ROW EXECUTE FUNCTION rematerialize_packet_path_privacy();
    CREATE TRIGGER private_node_prefixes_visibility_generation AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON private_node_prefixes
      FOR EACH STATEMENT EXECUTE FUNCTION bump_visibility_for_identity_table();
    CREATE TRIGGER packet_paths_classify_privacy BEFORE INSERT OR UPDATE OF rx_node_id, src_node_id, path_hashes, path_hash_size_bytes, network ON packet_paths
      FOR EACH ROW EXECUTE FUNCTION classify_packet_path_privacy();
  `);
}

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
    CREATE TABLE public_visibility_state (
      singleton BOOLEAN PRIMARY KEY, generation BIGINT, updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE packet_visibility_materialization_state (
      singleton BOOLEAN PRIMARY KEY, visibility_generation BIGINT, updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    INSERT INTO public_visibility_state (singleton, generation) VALUES (TRUE, 1);
    INSERT INTO packet_visibility_materialization_state (singleton, visibility_generation) VALUES (TRUE, 1);
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
      'archives', (SELECT jsonb_agg(a ORDER BY source_table, record_data::text) FROM maintenance_removed_records a),
      'visibility', (SELECT to_jsonb(v) FROM public_visibility_state v),
      'materialization', (SELECT to_jsonb(m) FROM packet_visibility_materialization_state m)
    ) AS state
  `)).rows[0].state;
  return { db, cleanupPool, snapshot, released: () => released };
}

test('PostgreSQL archives inactive roles 1/3 and never-bridged nodes while retaining every fresh clock', options, async (t) => {
  const f = await fixture();
  t.after(() => f.db.close());
  await f.db.exec(`ALTER TABLE nodes ADD COLUMN hardware_model TEXT;
    UPDATE nodes SET name = 'Dormant companion', hardware_model = 'Fixture hardware' WHERE node_id = 'companion';`);
  const before = await f.snapshot();
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
  // Replay only this disposable fixture's archive to prove every column and
  // both visibility sets survive to_jsonb(), including nullable/custom fields.
  for (const table of ['nodes', 'observer_region_observer_sightings', 'node_network_sightings']) {
    await f.db.query(`INSERT INTO ${table}
      SELECT restored.* FROM maintenance_removed_records archive
      CROSS JOIN LATERAL jsonb_populate_record(NULL::${table}, archive.record_data) restored
      WHERE archive.batch_id = 'sql-batch' AND archive.source_table = $1`, [table]);
  }
  const restored = await f.snapshot();
  for (const key of ['nodes', 'observers', 'networks']) assert.deepEqual(restored[key], before[key]);
});

for (const stage of ['archive', 'delete', 'fence']) {
  test(`PostgreSQL rolls back the whole inactive cleanup on ${stage} failure`, options, async (t) => {
    const f = await fixture();
    t.after(() => f.db.close());
    if (stage === 'fence') await installPrivacyTriggers(f.db);
    await f.db.exec(`
      CREATE FUNCTION reject_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RAISE EXCEPTION 'injected ${stage} failure'; END $$;
      CREATE TRIGGER reject_cleanup BEFORE ${stage === 'archive' ? 'INSERT ON maintenance_removed_records'
        : stage === 'delete' ? 'DELETE ON nodes' : 'UPDATE ON packet_visibility_materialization_state'}
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
    await installPrivacyTriggers(f.db);
    await f.db.exec(`
      INSERT INTO nodes (node_id, name, role, network, last_seen, last_mqtt_observer_seen_at, created_at)
        VALUES ('private-node', 'Quiet 🚫 node', 2, 'ukmesh', NOW() - INTERVAL '45 days',
                NOW() - INTERVAL '45 days', NOW() - INTERVAL '90 days');
      INSERT INTO packets (src_node_id, payload, is_private, visibility_ok)
        VALUES ('private-node', 'private history', TRUE, FALSE);
      INSERT INTO packet_paths (src_node_id) VALUES ('private-node');
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
    const generations = 'SELECT p.generation, m.visibility_generation FROM public_visibility_state p CROSS JOIN packet_visibility_materialization_state m';
    const beforeGenerations = (await f.db.query(generations)).rows;
    await cleanup({ cleanupPool: f.cleanupPool });
    const packet = await f.db.query("SELECT is_private, visibility_ok FROM packets WHERE src_node_id = 'private-node'");
    assert.deepEqual(packet.rows, [{ is_private: true, visibility_ok: false }],
      'maintenance must not turn a private deletion into consent to publish its packet history');
    assert.deepEqual((await f.db.query(privacyNodes)).rows, before);
    assert.deepEqual((await f.db.query('SELECT is_private, visibility_ok FROM packet_paths')).rows,
      [{ is_private: true, visibility_ok: false }]);
    assert.deepEqual((await f.db.query(generations)).rows.map((row: { visibility_generation: number }) => row.visibility_generation),
      beforeGenerations.map((row: { visibility_generation: number }) => row.visibility_generation),
      'an existing unfenced direct-prefix change must not be certified by public-node cleanup');
    assert.equal((await f.db.query('SELECT COUNT(*)::int AS count FROM private_node_prefixes')).rows[0].count, 4);
    assert.equal((await f.db.query("SELECT COUNT(*)::int AS count FROM maintenance_removed_records WHERE record_data->>'node_id' IN ('private-node', 'prefix-only', 'marker-only')")).rows[0].count, 0);
  });

  test(`PostgreSQL ${cleanup.name} deletes public nodes without invalidating the current privacy fence`, options, async (t) => {
    const f = await fixture();
    t.after(() => f.db.close());
    await installPrivacyTriggers(f.db);
    await f.db.exec(`UPDATE nodes SET last_mqtt_observer_seen_at = NOW() - INTERVAL '45 days' WHERE node_id = 'repeater';
      INSERT INTO nodes (node_id, name, role, network, last_seen, created_at)
        VALUES ('private-node', 'Protected 🚫 identity', 2, 'ukmesh', NOW() - INTERVAL '45 days', NOW() - INTERVAL '90 days');
      INSERT INTO packet_paths (src_node_id) VALUES ('private-node'), ('companion');`);
    const generations = 'SELECT p.generation, m.visibility_generation FROM public_visibility_state p CROSS JOIN packet_visibility_materialization_state m';
    const before = (await f.db.query(generations)).rows;
    assert.equal(before[0].generation, before[0].visibility_generation, 'fixture starts with a current materialization fence');
    const paths = (await f.db.query('SELECT * FROM packet_paths ORDER BY src_node_id')).rows;
    const result = await cleanup({ cleanupPool: f.cleanupPool });
    assert.ok(result.nodes > 0, 'the real DELETE must execute with all current node privacy triggers');
    assert.deepEqual((await f.db.query("SELECT node_id FROM nodes WHERE node_id = 'repeater'")).rows, []);
    const after = (await f.db.query(generations)).rows;
    assert.ok(after[0].generation > before[0].generation, 'the actual prefix FK cascade advances the public generation');
    assert.equal(after[0].generation, after[0].visibility_generation, 'unchanged public packet bits remain fenced');
    assert.deepEqual((await f.db.query('SELECT * FROM packet_paths ORDER BY src_node_id')).rows, paths);
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

test('PostgreSQL inactive cleanup rechecks RF sightings that arrive after the initial candidate snapshot', options, async (t) => {
  const f = await fixture();
  t.after(() => f.db.close());
  let injected = false;
  const cleanupPool = {
    async connect() {
      const client = await f.cleanupPool.connect();
      return {
        ...client,
        async query(text: string, values?: unknown[]) {
          const result = await client.query(text, values);
          if (!injected && text.includes('SELECT node_id') && text.includes('FOR UPDATE')) {
            injected = true;
            // Simulate source-node RF rollups becoming fresh between the first
            // candidate snapshot and the visibility lock that fences ingestion.
            await f.db.exec(`UPDATE node_network_sightings SET last_seen_at = NOW() WHERE node_id = 'companion';
              INSERT INTO observer_region_observer_sightings VALUES ('room-server', 'ABC', NOW());`);
          }
          return result;
        },
      };
    },
  };
  const result = await cleanupInactiveNodes({ cleanupPool, batchId: 'arrival-batch' });
  assert.equal(injected, true);
  assert.equal(result.nodes, 2, 'late-fresh companion and room-server sightings must withdraw their candidates');
  assert.deepEqual((await f.db.query("SELECT node_id FROM nodes WHERE node_id IN ('companion', 'room-server') ORDER BY node_id")).rows,
    [{ node_id: 'companion' }, { node_id: 'room-server' }]);
  assert.equal((await f.db.query("SELECT COUNT(*)::int AS count FROM maintenance_removed_records WHERE record_data->>'node_id' IN ('companion', 'room-server') OR record_data->>'rx_node_id' = 'room-server'")).rows[0].count, 0,
    'withdrawn candidates and their visibility records must not enter the removal archive');
});
