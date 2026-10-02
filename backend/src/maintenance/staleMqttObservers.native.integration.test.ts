import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdir, mkdtemp, readFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { installPrivacyTriggers } from './cleanupPrivacyFixture.js';
import { cleanupInactiveNodes, cleanupStaleMqttObservers } from './staleMqttObservers.js';

// Optional native PostgreSQL for real multi-connection lock tests. This starts
// its own loopback-only cluster in a new ignored directory; no existing DB URL.
const moduleUrl = process.env['TEST_NODE_CLEANUP_POSTGRES_MODULE'];
const options = { skip: moduleUrl ? false : 'TEST_NODE_CLEANUP_POSTGRES_MODULE is not configured', timeout: 20_000 };

function gate() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
}

async function fixture() {
  const { default: EmbeddedPostgres } = await import(moduleUrl!);
  const root = fileURLToPath(new URL('../../../.ukmesh-tools/', import.meta.url));
  await mkdir(root, { recursive: true });
  const databaseDir = await mkdtemp(`${root}native-cleanup-`);
  const socket = createServer();
  await new Promise<void>((resolve, reject) => {
    socket.once('error', reject);
    socket.listen(0, '127.0.0.1', resolve);
  });
  const address = socket.address();
  assert.ok(address && typeof address !== 'string');
  const port = address.port;
  await new Promise<void>((resolve, reject) => socket.close((error) => error ? reject(error) : resolve()));
  const postgres = new EmbeddedPostgres({
    databaseDir, port, user: 'cleanup_fixture', password: randomUUID(),
    authMethod: 'scram-sha-256', persistent: false, createPostgresUser: false,
    postgresFlags: ['-h', '127.0.0.1', '-k', databaseDir, '-c', 'deadlock_timeout=100ms', '-c', 'shared_buffers=16MB'],
    onLog: () => {}, onError: () => {},
  });
  const clients: any[] = [];
  try {
    await postgres.initialise();
    await postgres.start();
    for (let index = 0; index < 3; index++) {
      const client = postgres.getPgClient('postgres', '127.0.0.1');
      await client.connect();
      clients.push(client);
      await client.query(`SET lock_timeout = '5s'; SET statement_timeout = '10s'`);
    }
    const [cleanupClient, writer, observer] = clients;
    await observer.query(`
      CREATE TABLE nodes (
        node_id TEXT PRIMARY KEY, name TEXT, role INTEGER, network TEXT,
        last_seen TIMESTAMPTZ, last_mqtt_observer_seen_at TIMESTAMPTZ,
        last_path_evidence_at TIMESTAMPTZ, last_predicted_online_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ
      );
      CREATE TABLE observer_region_observer_sightings (rx_node_id TEXT, last_seen TIMESTAMPTZ);
      CREATE TABLE node_network_sightings (node_id TEXT, last_seen_at TIMESTAMPTZ);
      CREATE TABLE maintenance_removed_records (batch_id TEXT, source_table TEXT, record_data JSONB, reason TEXT);
      CREATE TABLE private_node_prefixes (node_id TEXT REFERENCES nodes(node_id) ON DELETE CASCADE);
      CREATE TABLE public_visibility_state (singleton BOOLEAN PRIMARY KEY, generation BIGINT);
      CREATE TABLE packet_visibility_materialization_state (
        singleton BOOLEAN PRIMARY KEY, visibility_generation BIGINT, updated_at TIMESTAMPTZ
      );
      INSERT INTO public_visibility_state VALUES (TRUE, 1);
      INSERT INTO packet_visibility_materialization_state VALUES (TRUE, 1, NOW());
      INSERT INTO nodes (node_id, role, network, last_seen, last_mqtt_observer_seen_at, created_at)
        VALUES ('dormant', 2, 'ukmesh', NOW() - INTERVAL '31 days', NOW() - INTERVAL '31 days', NOW() - INTERVAL '90 days');
    `);
    const pid = (await cleanupClient.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
    return {
      cleanupClient, writer, observer, pid,
      pool: { async connect() { return { query: cleanupClient.query.bind(cleanupClient), release() {} }; } },
      async close() {
        await Promise.all(clients.map(async (client) => {
          await client.query('ROLLBACK').catch(() => {});
          await client.end();
        }));
        await postgres.stop();
      },
    };
  } catch (error) {
    await Promise.all(clients.map((client) => client.end().catch(() => {})));
    await postgres.stop();
    throw error;
  }
}

async function waitForLock(observer: any, pid: number, participant = 'cleanup') {
  const deadline = Date.now() + 4_000;
  while (Date.now() < deadline) {
    const state = await observer.query('SELECT wait_event_type FROM pg_stat_activity WHERE pid = $1', [pid]);
    if (state.rows[0]?.wait_event_type === 'Lock') return;
    await delay(10);
  }
  assert.fail(`${participant} did not reach the competing visibility lock`);
}

const nodeRefresh = `UPDATE nodes SET last_seen = NOW(), last_mqtt_observer_seen_at = NOW() WHERE node_id = 'dormant'`;
for (const { cleanup, evidence, statement, visibilityLock } of [
  { cleanup: cleanupInactiveNodes, evidence: 'node timestamp', statement: nodeRefresh, visibilityLock: 'KEY SHARE' },
  { cleanup: cleanupStaleMqttObservers, evidence: 'node timestamp', statement: nodeRefresh, visibilityLock: 'KEY SHARE' },
  { cleanup: cleanupInactiveNodes, evidence: 'source RF sighting', statement: `INSERT INTO node_network_sightings VALUES ('dormant', NOW())`, visibilityLock: 'KEY SHARE' },
  { cleanup: cleanupInactiveNodes, evidence: 'observer RF sighting', statement: `INSERT INTO observer_region_observer_sightings VALUES ('dormant', NOW())`, visibilityLock: 'KEY SHARE' },
  ...[cleanupInactiveNodes, cleanupStaleMqttObservers].flatMap(cleanup => [
    { cleanup, evidence: 'privacy prefix', statement: `INSERT INTO private_node_prefixes VALUES ('dormant')`, visibilityLock: 'UPDATE' },
    { cleanup, evidence: 'privacy marker', statement: `UPDATE nodes SET name = 'Private 🚫' WHERE node_id = 'dormant'`, visibilityLock: 'UPDATE' },
  ]),
]) {
  test(`native ${cleanup.name} retains a concurrent ${evidence} refresh`, options, async (t) => {
    const f = await fixture();
    const selected = gate();
    const resume = gate();
    t.after(async () => { resume.resolve(); await f.close(); });
    let initialSelection = true;
    const pool = { async connect() { return {
      async query(text: string, values?: unknown[]) {
        const result = await f.cleanupClient.query(text, values);
        if (initialSelection && text.trim().startsWith('SELECT node_id')) {
          initialSelection = false;
          selected.resolve();
          await resume.promise;
        }
        return result;
      },
      release() {},
    }; } };
    const cleanupResult = cleanup({ cleanupPool: pool }).then(
      (value) => ({ ok: true as const, value }),
      (error) => ({ ok: false as const, error }),
    );
    await selected.promise;
    await f.writer.query('BEGIN');
    // Packet classification pins KEY SHARE; consent changes require UPDATE.
    // The fixture uses these lock protocols without installing privacy triggers.
    await f.writer.query(`SELECT generation FROM public_visibility_state WHERE singleton = TRUE FOR ${visibilityLock}`);
    resume.resolve();
    await waitForLock(f.observer, f.pid);
    await f.writer.query(statement);
    await f.writer.query('COMMIT');
    const result = await cleanupResult;
    assert.equal(result.ok, true, result.ok ? '' : String(result.error));
    if (!result.ok) throw result.error;
    assert.equal(result.value.nodes, 0);
    assert.equal(result.value.candidates, 0);
    assert.equal((await f.observer.query('SELECT COUNT(*)::int AS count FROM nodes')).rows[0].count, 1);
    assert.equal((await f.observer.query('SELECT COUNT(*)::int AS count FROM maintenance_removed_records')).rows[0].count, 0);
  });
}

test('native cleanup skips a busy node row and can archive it on the next pass', options, async (t) => {
  const f = await fixture();
  t.after(() => f.close());
  await f.writer.query('BEGIN');
  await f.writer.query(`SELECT node_id FROM nodes WHERE node_id = 'dormant' FOR UPDATE`);
  // A blocked row lock fails deliberately; SKIP LOCKED should avoid waiting.
  await f.cleanupClient.query(`SET lock_timeout = '1s'`);
  const skipped = await cleanupInactiveNodes({ cleanupPool: f.pool });
  assert.equal(skipped.nodes, 0);
  assert.equal((await f.observer.query('SELECT COUNT(*)::int AS count FROM maintenance_removed_records')).rows[0].count, 0);
  await f.writer.query('COMMIT');
  const retried = await cleanupInactiveNodes({ cleanupPool: f.pool });
  assert.equal(retried.nodes, 1);
  assert.equal((await f.observer.query('SELECT COUNT(*)::int AS count FROM nodes')).rows[0].count, 0);
});

for (const cleanup of [cleanupInactiveNodes, cleanupStaleMqttObservers]) {
  test(`native ${cleanup.name} yields a busy node to a waiting consent trigger`, options, async (t) => {
    const f = await fixture();
    const visibilityLocked = gate();
    const resume = gate();
    t.after(async () => { resume.resolve(); await f.close(); });
    // Execute only the production BEFORE trigger function in this new cluster,
    // not the migration or the remaining privacy materialization chain.
    const source = await readFile(new URL('../db/migrations/042_packet_visibility_fence.sql', import.meta.url), 'utf8');
    const definition = source.match(/CREATE OR REPLACE FUNCTION lock_packet_visibility_for_node_privacy_change\([\s\S]*?^\$\$;/m)?.[0];
    assert.ok(definition);
    await f.observer.query(definition);
    await f.observer.query(`CREATE TRIGGER nodes_packet_visibility_serialization
      BEFORE INSERT OR UPDATE OR DELETE ON nodes FOR EACH ROW
      EXECUTE FUNCTION lock_packet_visibility_for_node_privacy_change()`);
    const pool = { async connect() { return {
      async query(text: string, values?: unknown[]) {
        const result = await f.cleanupClient.query(text, values);
        if (text.includes('FOR UPDATE OF visibility')) {
          visibilityLocked.resolve();
          await resume.promise;
        }
        return result;
      },
      release() {},
    }; } };
    const cleanupResult = cleanup({ cleanupPool: pool }).then(
      (value) => ({ ok: true as const, value }),
      (error) => ({ ok: false as const, error }),
    );
    await visibilityLocked.promise;
    await f.writer.query('BEGIN');
    const writerPid = (await f.writer.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
    // UPDATE locks the node before its BEFORE trigger waits for visibility.
    const consentResult = f.writer.query(`UPDATE nodes SET name = 'Private 🚫' WHERE node_id = 'dormant'`).then(
      (value: { rowCount: number }) => ({ ok: true as const, value }),
      (error: unknown) => ({ ok: false as const, error }),
    );
    await waitForLock(f.observer, writerPid, 'consent writer');
    resume.resolve();
    const result = await cleanupResult;
    assert.equal(result.ok, true, result.ok ? '' : String(result.error));
    if (!result.ok) throw result.error;
    assert.equal(result.value.nodes, 0);
    const consent = await consentResult;
    assert.equal(consent.ok, true, consent.ok ? '' : String(consent.error));
    if (!consent.ok) throw consent.error;
    assert.equal(consent.value.rowCount, 1);
    await f.writer.query('COMMIT');
    assert.deepEqual((await f.observer.query('SELECT name FROM nodes')).rows, [{ name: 'Private 🚫' }]);
    assert.equal((await f.observer.query('SELECT COUNT(*)::int AS count FROM maintenance_removed_records')).rows[0].count, 0);
    assert.equal((await cleanup({ cleanupPool: f.pool })).nodes, 0, 'the next pass must retain the committed privacy marker');
  });
}

for (const fence of ['current', 'gap', 'missing'] as const) {
  test(`native public-node cleanup preserves the ${fence} privacy fence with production triggers`, options, async (t) => {
    const f = await fixture();
    t.after(() => f.close());
    await f.observer.query(`
      ALTER TABLE public_visibility_state ADD COLUMN updated_at TIMESTAMPTZ DEFAULT NOW();
      ALTER TABLE private_node_prefixes
        ADD COLUMN network TEXT, ADD COLUMN prefix_size_bytes INTEGER,
        ADD COLUMN prefix TEXT, ADD COLUMN updated_at TIMESTAMPTZ DEFAULT NOW(),
        ADD PRIMARY KEY (node_id, network, prefix_size_bytes);
      CREATE TABLE packets (src_node_id TEXT, payload TEXT);
    `);
    await installPrivacyTriggers({ exec: sql => f.observer.query(sql) });
    await f.observer.query(`
      INSERT INTO packets (src_node_id, rx_node_id, path_hashes, path_hash_size_bytes)
        VALUES ('public-source', 'public-receiver', ARRAY['AB'], 1);
      INSERT INTO packet_paths (src_node_id, rx_node_id, path_hashes, path_hash_size_bytes)
        VALUES ('public-source', 'public-receiver', ARRAY['AB'], 1);
    `);
    if (fence === 'gap') {
      await f.observer.query('UPDATE public_visibility_state SET generation = 5');
      await f.observer.query('UPDATE packet_visibility_materialization_state SET visibility_generation = 3');
    } else if (fence === 'missing') {
      await f.observer.query('DELETE FROM packet_visibility_materialization_state');
    }
    const generations = `SELECT visibility.generation::int AS generation,
      materialized.visibility_generation::int AS materialized_generation
      FROM public_visibility_state visibility
      LEFT JOIN packet_visibility_materialization_state materialized USING (singleton)`;
    const flags = `SELECT 'packet' AS kind, is_private, visibility_ok FROM packets
      UNION ALL SELECT 'path', is_private, visibility_ok FROM packet_paths ORDER BY kind`;
    const before = (await f.observer.query(generations)).rows[0];
    const beforeFlags = (await f.observer.query(flags)).rows;
    assert.deepEqual(beforeFlags, [
      { kind: 'packet', is_private: false, visibility_ok: true },
      { kind: 'path', is_private: false, visibility_ok: true },
    ]);
    assert.equal((await cleanupInactiveNodes({ cleanupPool: f.pool })).nodes, 1);
    const after = (await f.observer.query(generations)).rows[0];
    assert.ok(after.generation > before.generation, 'the empty prefix FK cascade must execute the production generation trigger');
    assert.equal(after.materialized_generation, fence === 'current' ? after.generation : before.materialized_generation,
      'only an already-current fence may advance with the public deletion');
    assert.deepEqual((await f.observer.query(flags)).rows, beforeFlags, 'public deletion must retain stored packet and path visibility');
    assert.equal((await f.observer.query("SELECT COUNT(*)::int AS count FROM maintenance_removed_records WHERE source_table = 'nodes'")).rows[0].count, 1);
    assert.equal((await f.observer.query('SELECT COUNT(*)::int AS count FROM nodes')).rows[0].count, 0);
  });
}
