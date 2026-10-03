import assert from 'node:assert/strict';
import test from 'node:test';
import { createOwnerRepository } from './ownerRepository.js';
import { createOwnerService } from './ownerService.js';

// Optional PostgreSQL-WASM fixture; no server connection or live data.
const moduleUrl = process.env['TEST_OWNER_LAST_HOP_PGLITE_MODULE'];
const options = {
  skip: moduleUrl ? false : 'TEST_OWNER_LAST_HOP_PGLITE_MODULE is not configured',
  timeout: 20_000,
};

test('actual last-hop SQL replaces renamed and newly resolved groups during a warm refresh', options, async (t) => {
  const { PGlite } = await import(moduleUrl!);
  const db = await PGlite.create();
  t.after(() => db.close());
  await db.exec(`
    SET TIME ZONE 'UTC';
    CREATE TABLE node_identity_nodes (node_id TEXT PRIMARY KEY, name TEXT, role INTEGER, lat DOUBLE PRECISION, lon DOUBLE PRECISION);
    CREATE TABLE node_identity_aliases (source_node_id TEXT, canonical_node_id TEXT);
    CREATE TABLE node_identity_links (node_a_id TEXT, node_b_id TEXT, force_viable BOOLEAN, itm_viable BOOLEAN, itm_path_loss_db DOUBLE PRECISION);
    CREATE TABLE node_identity_packets (
      time TIMESTAMPTZ, packet_hash TEXT, rx_node_id_raw TEXT, rx_node_id TEXT, src_node_id TEXT,
      hop_count INTEGER, rssi DOUBLE PRECISION, snr DOUBLE PRECISION, path_hashes TEXT[], packet_type INTEGER
    );
    CREATE FUNCTION meshcore_canonical_node_id(value TEXT) RETURNS TEXT LANGUAGE SQL AS $$ SELECT value $$;
    -- This fixture supplies only the hourly bucketing needed by the real query.
    -- It does not install TimescaleDB or exercise the production identity views.
    CREATE FUNCTION time_bucket(width INTERVAL, value TIMESTAMPTZ) RETURNS TIMESTAMPTZ LANGUAGE SQL
      AS $$ SELECT date_trunc('hour', value) $$;
    INSERT INTO node_identity_nodes (node_id, name, role) VALUES ('BB01', 'Owner', 2), ('AA01', 'Peer', 2);
    INSERT INTO node_identity_packets VALUES
      (date_trunc('hour', NOW()) - INTERVAL '2 hours' + INTERVAL '10 minutes', 'historical', 'BB01', 'BB01', 'AA01', 0, -90, 4, '{}', 5),
      (date_trunc('hour', NOW()) - INTERVAL '1 hour' + INTERVAL '10 minutes', 'direct-1', 'BB01', 'BB01', 'AA01', 0, -90, 4, '{}', 5),
      (date_trunc('hour', NOW()) - INTERVAL '1 hour' + INTERVAL '11 minutes', 'direct-2', 'BB01', 'BB01', 'AA01', 0, -92, 2, '{}', 5),
      (date_trunc('hour', NOW()) - INTERVAL '1 hour' + INTERVAL '12 minutes', 'unresolved', 'BB01', 'BB01', 'CC02', 1, -94, 1, '{CC}', 5);
  `);
  const repository = createOwnerRepository({ query: (sql, params) => db.query(sql, params) });
  const service = createOwnerService({
    repository,
    ownerLiveCache: new Map(), ownerLiveCacheTtlMs: 15_000,
    ownerDashboardCacheTtlMs: 20_000, ownerLastHopCacheTtlMs: 60_000,
    verifyMqttCredentials: async () => true,
    resolveOwnerNodeIds: async () => [], autoLinkOwnerNodeIds: async () => [],
    buildOwnerDashboard: async () => ({ nodes: [] }), invalidateOwnerNodeIdCache: () => {},
  });
  const first = await service.getOwnerLastHopStrength(['BB01'], 'BB01');
  assert.equal(first.points.length, 3);
  const latest = first.points.at(-1)!.bucket;
  const history = first.points.filter(point => point.bucket < latest);
  assert.deepEqual(first.points.filter(point => point.bucket === latest).map(point => [point.lastHopName, point.resolution, point.sampleCount]), [
    ['Peer', 'direct', 2], ['Unresolved', 'unresolved', 1],
  ]);

  await db.exec(`
    UPDATE node_identity_nodes SET name = 'Renamed peer' WHERE node_id = 'AA01';
    INSERT INTO node_identity_nodes (node_id, name, role) VALUES ('CC01', 'New peer', 2);
  `);
  const refreshed = await service.getOwnerLastHopStrength(['BB01'], 'BB01', true);
  assert.deepEqual(refreshed.points.filter(point => point.bucket < latest), history);
  assert.deepEqual(refreshed.points.filter(point => point.bucket === latest).map(point => [
    point.lastHopNodeId, point.lastHopName, point.resolution, point.sampleCount, point.avgSnr, point.avgRssi,
  ]), [
    ['AA01', 'Renamed peer', 'direct', 2, 3, -91], ['CC01', 'New peer', 'resolved', 1, 1, -94],
  ]);
  assert.equal(refreshed.points.reduce((total, point) => total + point.sampleCount, 0), 4, 'regrouping must not double-count packets');

  await db.query('DELETE FROM node_identity_packets WHERE time >= $1::timestamptz', [latest]);
  assert.deepEqual((await service.getOwnerLastHopStrength(['BB01'], 'BB01', true)).points, history,
    'an empty refreshed query removes the vanished current groups');
});
