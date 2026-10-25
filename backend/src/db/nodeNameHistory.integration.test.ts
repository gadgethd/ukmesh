import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { readFile } from 'node:fs/promises';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router } from 'express';
import { rateLimit } from 'express-rate-limit';
import pg from 'pg';
import { upsertNode, insertNodeStatusSample } from './index.js';
import { persistStatusTelemetrySample } from '../mqtt/statusTelemetry.js';
import { createNodeRepository } from '../repositories/nodes.js';
import { registerNodeRoutes } from '../api/routes/nodes.js';
import { networkFilters } from '../api/utils/networkFilters.js';

// Worktree-local PostgreSQL WASM only: no DATABASE_URL or existing service.
const moduleUrl = process.env['TEST_NODE_HISTORY_PGLITE_MODULE'];
const key = (c: string) => c.repeat(64);

test('name retention and search execute the production write paths, migration, identity view and HTTP route', {
  skip: moduleUrl ? false : 'TEST_NODE_HISTORY_PGLITE_MODULE is not configured',
  timeout: 120_000,
}, async t => {
  const { PGlite } = await import(moduleUrl!);
  const db = await PGlite.create();
  t.after(() => db.close());
  await db.exec(`
    CREATE TABLE nodes (
      node_id TEXT PRIMARY KEY, name TEXT, lat DOUBLE PRECISION, lon DOUBLE PRECISION,
      iata TEXT, role INTEGER, hardware_model TEXT, firmware_version TEXT, public_key TEXT,
      last_seen TIMESTAMPTZ, is_online BOOLEAN, network TEXT, advert_count INTEGER DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT NOW(), elevation_m DOUBLE PRECISION, observer_iata TEXT,
      last_mqtt_observer_seen_at TIMESTAMPTZ, last_predicted_online_at TIMESTAMPTZ,
      last_path_evidence_at TIMESTAMPTZ, last_rx_at TIMESTAMPTZ, last_status_at TIMESTAMPTZ
    );
    CREATE TABLE node_counted_adverts (canonical_advert_hash TEXT PRIMARY KEY, node_id TEXT, counted_at TIMESTAMPTZ);
    CREATE TABLE node_identity_aliases (source_node_id TEXT PRIMARY KEY, canonical_node_id TEXT);
    CREATE TABLE node_network_sightings (node_id TEXT, network TEXT, first_seen_at TIMESTAMPTZ, last_seen_at TIMESTAMPTZ);
    CREATE TABLE packets (rx_node_id TEXT, src_node_id TEXT, time TIMESTAMPTZ, network TEXT);
    CREATE TABLE node_status_samples (
      time TIMESTAMPTZ, node_id TEXT, network TEXT, battery_mv INTEGER, uptime_secs BIGINT,
      tx_air_secs DOUBLE PRECISION, rx_air_secs DOUBLE PRECISION,
      channel_utilization DOUBLE PRECISION, air_util_tx DOUBLE PRECISION, stats JSONB, status TEXT
    );
    INSERT INTO nodes (node_id, name, lat, lon, role, network, last_seen)
      VALUES ('${key('A')}', 'Original hill', 54.5, -1.5, 2, 'ukmesh', NOW());
  `);
  const identity = await readFile(new URL('./migrations/036_node_identity_merge.sql', import.meta.url), 'utf8');
  await db.exec(identity.slice(
    identity.indexOf('CREATE OR REPLACE FUNCTION meshcore_canonical_node_id'),
    identity.indexOf('CREATE VIEW node_identity_status_samples'),
  ));
  const optimizedIdentity = await readFile(new URL('./migrations/037_node_identity_view_perf.sql', import.meta.url), 'utf8');
  await db.exec(optimizedIdentity.slice(
    optimizedIdentity.indexOf('CREATE OR REPLACE VIEW node_identity_nodes AS'),
    optimizedIdentity.lastIndexOf('COMMIT;'),
  ));
  const migration = await readFile(new URL('./migrations/064_node_name_history.sql', import.meta.url), 'utf8');
  await db.exec(`BEGIN; ${migration} COMMIT;`);
  assert.equal((await db.query('SELECT * FROM node_name_history')).rows.length, 0, 'migration must not backfill nodes');

  const query = (sql: string, params?: unknown[]) => db.query(sql, params?.map(value => value ?? null));
  t.mock.method(pg.Pool.prototype, 'query', query);
  t.mock.method(pg.Pool.prototype, 'connect', async () => ({ query, release() {} }));
  const repository = createNodeRepository(query);
  const router = Router();
  registerNodeRoutes(router, {
    nodeRepository: repository, networkFilters,
    getNodes: async () => [], getNodeHistory: async () => [], getNodeAdverts: async () => [],
    getTopAdvertingRepeaters: async () => [], requireLocalOnly: () => false,
    getPublicVisibilityGeneration: async () => 1,
    inferredNodesCache: new Map(), inferredNodesInflight: new Map(), inferredNodesCacheTtlMs: 60_000,
    nodeLinksCache: new Map(), nodeLinksInflight: new Map(), nodeLinksCacheTtlMs: 60_000,
    nodesLimiter: rateLimit({ windowMs: 60_000, limit: 200 }),
  });
  const app = express();
  app.use('/api', router);
  const server = app.listen(0, '127.0.0.1');
  t.after(async () => {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  });
  await once(server, 'listening');
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}/api/nodes/map`;
  const search = async (q: string, scope = '') => {
    const response = await fetch(`${base}?q=${encodeURIComponent(q)}${scope}`);
    assert.equal(response.status, 200);
    if (q.trim()) assert.equal(response.headers.get('cache-control'), 'no-store');
    const page = await response.json() as { nodes: Array<Record<string, unknown>>; page: { complete: boolean } };
    return page.nodes;
  };
  const write = (id: string, name: string, extra: Parameters<typeof upsertNode>[1] = {}) => upsertNode(id, {
    name, lat: 54.5, lon: -1.5, role: 2, network: 'ukmesh', ...extra,
  });

  await t.test('first post-migration rename retains the old name and both upsert paths resolve the current DTO', async () => {
    assert.equal((await search('original HILL'))[0]?.name, 'Original hill', 'current names need no backfill');
    await upsertNode(key('A'), { name: 'Current hill', network: 'ukmesh', mqttObserver: true });
    const rows = await search('original HILL');
    assert.equal(rows.length, 1);
    assert.equal(rows[0]?.node_id, key('A'));
    assert.equal(rows[0]?.name, 'Current hill');
    assert.deepEqual(Object.keys(rows[0]!).sort(), [
      'node_id', 'name', 'lat', 'lon', 'role', 'iata', 'last_seen', 'is_online',
      'hardware_model', 'firmware_version', 'advert_count', 'elevation_m',
    ].sort(), 'history must not add fields to the public DTO');
    await write(key('A'), 'Third hill');
    assert.equal((await search('Current hill'))[0]?.name, 'Third hill');
    assert.equal((await search('Original hill'))[0]?.name, 'Third hill');
    await write(key('A'), 'Third hill');
    await db.exec("UPDATE node_name_history SET first_seen_at = '2020-01-01' WHERE name = 'Original hill'");
    await write(key('A'), 'Original hill');
    const history = await db.query('SELECT name, first_seen_at, last_seen_at FROM node_name_history ORDER BY name');
    assert.equal(history.rows.length, 3, 'repeat names must not create duplicate history');
    const original = history.rows.find((row: { name: string }) => row.name === 'Original hill');
    assert.equal(new Date(original.first_seen_at).getUTCFullYear(), 2020);
    assert.ok(original.last_seen_at > original.first_seen_at);
    assert.equal((await search('Third hill'))[0]?.name, 'Original hill');
  });

  await t.test('history is transactional, skips blank names and does not track ordinary observer touches', async () => {
    await db.exec('BEGIN');
    await db.query('UPDATE nodes SET name = $1 WHERE node_id = $2', ['Rolled back', key('A')]);
    await db.exec('ROLLBACK');
    assert.deepEqual(await search('Rolled back'), []);
    await write(key('B'), 'Temporary');
    await db.query('UPDATE nodes SET name = $1 WHERE node_id = $2', ['', key('B')]);
    await db.query('UPDATE nodes SET name = NULL WHERE node_id = $1', [key('B')]);
    assert.equal((await search('Temporary'))[0]?.name, null);
    assert.deepEqual((await db.query("SELECT name FROM node_name_history WHERE BTRIM(name) = ''")).rows, []);
    const before = (await db.query('SELECT * FROM node_name_history ORDER BY node_id, name')).rows;
    await upsertNode(key('A'), { mqttObserver: true });
    assert.deepEqual((await db.query('SELECT * FROM node_name_history ORDER BY node_id, name')).rows, before);
  });

  await t.test('literal search retains network, observer, freshness and privacy gates', async () => {
    await write(key('C'), 'Hill_100%');
    await write(key('C'), 'New hill');
    assert.deepEqual((await search('Hill_100%')).map(n => n.node_id), [key('C')]);
    assert.equal((await search('%')).length, 1, 'percent is not an SQL wildcard');
    await write(key('D'), 'Hill_100%', { network: 'test', allowTestOverride: true });
    await write(key('D'), 'Test hill', { network: 'test', allowTestOverride: true });
    assert.deepEqual((await search('Hill_100%')).map(n => n.node_id), [key('C')]);
    assert.deepEqual((await search('Hill_100%', '&network=test')).map(n => n.node_id), [key('D')]);
    assert.deepEqual(await search('Hill_100%', `&observer=${key('A')}`), []);
    await db.query('INSERT INTO packets VALUES ($1, $2, NOW(), $3)', [key('A'), key('C'), 'ukmesh']);
    assert.deepEqual((await search('Hill_100%', `&observer=${key('A')}`)).map(n => n.node_id), [key('C')]);
    await write(key('C'), 'Private 🚫 hill');
    assert.deepEqual(await search('Hill_100%'), []);
    await write(key('C'), 'Public again');
    assert.deepEqual(await search('Private 🚫 hill'), [], 'private historical names are not searchable');
    await db.query("UPDATE nodes SET last_seen = NOW() - INTERVAL '29 days' WHERE node_id = $1", [key('C')]);
    assert.deepEqual(await search('Hill_100%'), []);
  });

  await t.test('names resolve through current identity aliases without exposing private or test sources', async () => {
    await write(key('E'), 'Former alias name', { advertHash: 'advert-e' });
    await write(key('E'), 'Canonical name', { advertHash: 'advert-e-2' });
    await write(key('F'), 'Former alias name');
    assert.equal((await search('Former alias name')).length, 2, 'a shared name must not merge independent nodes');
    await write(key('F'), 'Alias current');
    await db.query('INSERT INTO node_identity_aliases VALUES ($1, $2)', [key('F'), key('E')]);
    assert.deepEqual((await search('Former alias name')).map(n => [n.node_id, n.name]), [[key('E'), 'Canonical name']]);
    await write(key('F'), 'Secret alias history');
    await write(key('F'), 'Alias private 🚫');
    assert.deepEqual(await search('Secret alias history'), []);
    await write(key('F'), 'Test alias', { network: 'test', allowTestOverride: true });
    assert.deepEqual(await search('Secret alias history'), []);
  });

  await t.test('search pagination and malformed input keep the existing map envelope and bounds', async () => {
    await write(key('1'), 'Pagination hill');
    const first = await fetch(`${base}?q=hill&limit=1`);
    assert.equal(first.status, 200);
    const page = await first.json() as { nodes: Array<{ node_id: string }>; page: { snapshot: string; nextCursor: string; complete: boolean } };
    assert.equal(page.nodes.length, 1);
    assert.equal(page.page.complete, false);
    const next = await fetch(`${base}?q=hill&limit=1&snapshot=${page.page.snapshot}&cursor=${page.page.nextCursor}`);
    assert.equal(next.status, 200);
    assert.notEqual((await next.json()).nodes[0]?.node_id, page.nodes[0]?.node_id);
    for (const query of ['q=one&q=two', `q=${'x'.repeat(161)}`, 'q=%00']) {
      assert.equal((await fetch(`${base}?${query}`)).status, 400);
    }
  });

  await t.test('long and Unicode names survive rename and node rediscovery without restricting existing writes', async () => {
    const longName = 'Café ridge ' + Array.from({ length: 80 }, (_, index) =>
      createHash('sha256').update(String(index)).digest('hex')).join('');
    await write(key('2'), longName);
    await write(key('2'), 'Rediscovered current');
    assert.equal((await search(longName.slice(0, 100)))[0]?.name, 'Rediscovered current');
    await db.query('DELETE FROM nodes WHERE node_id = $1', [key('2')]);
    assert.deepEqual(await search(longName.slice(0, 100)), []);
    await write(key('2'), 'Rediscovered again');
    assert.equal((await search(longName.slice(0, 100)))[0]?.name, 'Rediscovered again');
  });

  await t.test('firmware Will creates no SQL status rows while the same online telemetry persists', async () => {
    const will = JSON.parse(await readFile(new URL('../mqtt/fixtures/firmware-will.json', import.meta.url), 'utf8'));
    const options = { nodeId: key('A'), network: 'ukmesh' };
    assert.equal(await persistStatusTelemetrySample(will, options, insertNodeStatusSample), false);
    assert.equal((await db.query('SELECT * FROM node_status_samples')).rows.length, 0);
    assert.equal(await persistStatusTelemetrySample({ ...will, status: 'online' }, options, insertNodeStatusSample), true);
    const rows = (await db.query('SELECT node_id, status, battery_mv, uptime_secs, stats FROM node_status_samples')).rows;
    assert.equal(rows.length, 1);
    assert.equal(rows[0].node_id, key('A'));
    assert.equal(rows[0].status, 'online');
    assert.equal(rows[0].battery_mv, 4200);
    assert.equal(Number(rows[0].uptime_secs), 1);
    assert.deepEqual(rows[0].stats, will.stats);
  });
});
