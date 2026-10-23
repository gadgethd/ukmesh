import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test, { type TestContext } from 'node:test';
import { promisify } from 'node:util';
import pg, { type QueryResultRow } from 'pg';
import express, { Router, type RequestHandler } from 'express';
import { getNodeHistory } from '../../db/index.js';
import { rfValidationRows } from '../../repositories/networkAnalysis.js';
import { networkFilters } from '../utils/networkFilters.js';
import { registerExportRoutes } from './exports.js';

// Real PostgreSQL evaluation of application SQL, with every application
// relation shadowed by a CTE. SELECT only; no fixture tables or DB writes.
const sqlTest = process.env['AUDITWAVE_SQL_TESTS'] === '1' ? test : test.skip;
const execFileAsync = promisify(execFile);
const key = (c: string) => c.repeat(64);
const A = key('A'), B = key('B'), C = key('C'), D = key('D'), F = key('F');
function literal(value: unknown): string {
  if (Array.isArray(value)) return `ARRAY[${value.map(literal).join(',')}]::text[]`;
  if (typeof value === 'number') { assert.ok(Number.isFinite(value)); return String(value); }
  return `'${String(value).replace(/'/g, "''")}'`;
}
function node(node_id: string, overrides: Record<string, unknown> = {}) {
  return { node_id, name: `Repeater ${node_id[0]}`, role: 2, network: 'ukmesh',
    lat: 51, lon: -1, iata: 'LHR', last_seen: '2026-09-07T12:00:00Z', hardware_model: null, ...overrides };
}
function packet(overrides: Record<string, unknown> = {}) {
  return { age: 3600, packet_hash: 'repeated', rx_node_id: A, src_node_id: B,
    network: 'ukmesh', topic: 'ukmesh/LHR/observer/packets', topic_prefix: 'ukmesh',
    visibility_ok: true, is_private: false, packet_type: 4, hop_count: 2,
    rssi: null, snr: null, payload: { direction: 'rx' }, ...overrides };
}
function link(node_a_id: string, node_b_id: string, overrides: Record<string, unknown> = {}) {
  return { node_a_id, node_b_id, age: 3600, observed_count: 1,
    multibyte_observed_count: 0, itm_path_loss_db: 120, itm_viable: true, force_viable: false, ...overrides };
}
type Fixture = {
  packets?: ReturnType<typeof packet>[];
  links?: ReturnType<typeof link>[];
  nodes?: ReturnType<typeof node>[];
  stalePrivacy?: boolean;
  sighted?: boolean;
};
function fixtureQuery(fixture: Fixture) {
  return async <T extends QueryResultRow = QueryResultRow>(sql: string, params: unknown[] = []): Promise<{ rows: T[] }> => {
    const bound = sql.replace(/\$(\d+)\b/g, (_p, index: string) => literal(params[Number(index) - 1]));
    const fixtureSql = `WITH
      packets AS (
        SELECT p.*, NOW() - age * INTERVAL '1 second' AS time
        FROM jsonb_to_recordset(${literal(JSON.stringify(fixture.packets ?? []))}::jsonb) AS p(
          age double precision, packet_hash text, rx_node_id text, src_node_id text,
          network text, topic text, topic_prefix text, visibility_ok boolean, is_private boolean,
          packet_type integer, hop_count integer, rssi double precision, snr double precision, payload jsonb)
      ), node_identity_aliases AS (
        SELECT * FROM (VALUES (${literal(F)},${literal(A)})) a(source_node_id,canonical_node_id)
      ), node_identity_nodes AS (
        SELECT * FROM jsonb_to_recordset(${literal(JSON.stringify(fixture.nodes ?? [node(A), node(B), node(C), node(D)]))}::jsonb)
        AS n(node_id text,name text,role integer,network text,lat double precision,lon double precision,
          iata text,last_seen timestamptz,hardware_model text)
      ), node_identity_sightings AS (
        SELECT ${literal(B)} AS node_id,'ukmesh'::text AS network,NOW() AS last_seen_at
        WHERE ${fixture.sighted ? 'TRUE' : 'FALSE'}
      ), node_links AS (
        SELECT l.*, NOW() - age * INTERVAL '1 second' AS last_observed
        FROM jsonb_to_recordset(${literal(JSON.stringify(fixture.links ?? []))}::jsonb) AS l(
          node_a_id text,node_b_id text,age double precision,observed_count integer,
          multibyte_observed_count integer,itm_path_loss_db double precision,itm_viable boolean,force_viable boolean)
      ), packet_visibility_materialization_state AS (
        SELECT TRUE AS singleton,1 AS visibility_generation
      ), public_visibility_state AS (
        SELECT TRUE AS singleton,${fixture.stalePrivacy ? 2 : 1} AS generation
      ) SELECT COALESCE(jsonb_agg(to_jsonb(result)),'[]'::jsonb) FROM (${bound}) result;`;
    const child = execFileAsync('docker', ['exec', '-i', '-e',
      'PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=4500',
      'meshcore-infra-timescaledb-1', 'psql', '-X', '-U', 'meshcore', '-d', 'meshcore',
      '-v', 'ON_ERROR_STOP=1', '-At'], { timeout: 6_000, maxBuffer: 4 * 1024 * 1024 });
    child.child.stdin!.end(fixtureSql);
    return { rows: JSON.parse((await child).stdout) as T[] };
  };
}
async function history(t: TestContext, fixture: Fixture, id = A, network = 'ukmesh') {
  const mocked = t.mock.method(pg.Pool.prototype, 'query', fixtureQuery(fixture));
  try { return await getNodeHistory(id, 24, network); }
  finally { mocked.mock.restore(); }
}

sqlTest('history selects RX and origin aliases once per observation, retains repeated hashes and canonicalizes source ids', async (t) => {
  const rows = await history(t, { packets: [
    packet({ packet_hash: 'rx' }), packet({ packet_hash: 'src', rx_node_id: B, src_node_id: A }),
    packet({ packet_hash: 'alias-rx', rx_node_id: F }),
    packet({ packet_hash: 'alias-src', rx_node_id: B, src_node_id: F }),
    packet({ packet_hash: 'self', rx_node_id: F, src_node_id: A }),
    packet({ packet_hash: 'same' }), packet({ packet_hash: 'same', age: 3700 }),
    packet({ packet_hash: 'unrelated', rx_node_id: B, src_node_id: C }),
  ] }, ` ${F.toLowerCase()} `);
  assert.equal(rows.length, 7);
  assert.equal(rows.filter(r => r.packet_hash === 'self').length, 1);
  assert.equal(rows.filter(r => r.packet_hash === 'same').length, 2);
  assert.equal(rows.find(r => r.packet_hash === 'alias-src')!.src_node_id, A);
  assert.deepEqual(Object.keys(rows[0]!).sort(), ['time','packet_hash','src_node_id','topic','packet_type','hop_count','rssi','snr','payload'].sort());
});

sqlTest('history enforces the exact time window, public network compatibility and privacy for either direction', async (t) => {
  for (const direction of ['rx', 'src']) {
    const row = (change: Record<string, unknown>) => packet({ ...(direction === 'src' ? { src_node_id: F, rx_node_id: B } : {}), ...change });
    const packets = [row({}), row({ network: 'northeast' }), row({ network: 'teesside' }), row({ age: 0 }),
      row({ age: 86400 }), row({ age: -1 }), row({ network: 'test' }), row({ network: 'other' }),
      row({ topic_prefix: 'meshcore-test' }), row({ topic_prefix: null, topic: 'meshcore-test/LHR/x/packets' }),
      row({ visibility_ok: false }), row({ visibility_ok: null }), row({ is_private: true })];
    assert.equal((await history(t, { packets })).length, 4);
    assert.deepEqual(await history(t, { packets, stalePrivacy: true }), []);
  }
  assert.equal((await history(t, { packets: [packet({ network: 'test', topic_prefix: 'meshcore-test' })] }, A, 'test')).length, 1);
});

sqlTest('history keeps the newest 500 observations and returns an honest empty set', async (t) => {
  const rows = await history(t, { packets: Array.from({ length: 510 }, (_, i) => packet({ age: i, packet_hash: String(i) })) });
  assert.equal(rows.length, 500);
  assert.equal(rows[0]!.packet_hash, '0');
  assert.equal(rows[499]!.packet_hash, '499');
  assert.deepEqual(await history(t, { packets: [packet()] }, key('0')), []);
  assert.deepEqual(await history(t, { packets: [] }), []);
});

sqlTest('RF query combines aliases and preserves mismatch, override and stale weak-evidence classifications', async () => {
  const { rows } = await rfValidationRows(fixtureQuery({ links: [
    link(A, B, { observed_count: 5, multibyte_observed_count: 1, itm_viable: false, itm_path_loss_db: 150 }),
    link(B, F, { observed_count: 7, multibyte_observed_count: 2, itm_viable: false, itm_path_loss_db: 140 }),
    link(A, C, { force_viable: true }),
    link(B, C, { age: 8 * 86400, observed_count: 2 }),
    link(A, D),
    link(C, D, { age: 30 * 86400 }),
    link(A, F, { observed_count: 1000, itm_viable: false }),
  ] }), networkFilters('ukmesh'), 100);
  assert.equal(rows.length, 4);
  assert.deepEqual(rows.map(r => r.classification), ['observed_unexpected','operator_override','weak_model_evidence','match']);
  assert.equal(rows[0]!.observed_count, 12);
  assert.equal(rows[0]!.multibyte_observed_count, 3);
  assert.equal(rows[0]!.itm_path_loss_db, 140);
  assert.equal(rows[0]!.node_a_id, A);
  assert.equal(rows[0]!.node_b_id, B);
});

sqlTest('RF scopes both endpoints before its limit, allowing recent sightings but excluding private, test and non-repeater nodes', async () => {
  const fixture: Fixture = { nodes: [node(A), node(B, { network: 'other' }), node(C, { name: '🚫 Private' }), node(D, { role: 1 })],
    links: [link(A, C, { observed_count: 99999, itm_viable: false }), link(A, D, { observed_count: 99999 }), link(A, B)], sighted: true };
  const { rows } = await rfValidationRows(fixtureQuery(fixture), networkFilters('ukmesh'), 1);
  assert.equal(rows.length, 1);
  assert.equal(rows[0]!.node_b_id, B);
  assert.deepEqual((await rfValidationRows(fixtureQuery({ ...fixture, sighted: false }), networkFilters('ukmesh'), 1)).rows, []);
  assert.deepEqual((await rfValidationRows(fixtureQuery({ ...fixture, nodes: [node(A), node(B, { network: 'test' })] }), networkFilters('ukmesh'), 1)).rows, []);
});

sqlTest('exports execute real SQL with the correct alias and omit private, unpositioned and out-of-scope nodes', async () => {
  const app = express(), router = Router();
  const limiter: RequestHandler = (_req, _res, next) => next();
  registerExportRoutes(router, { networkFilters,
    exportLimiter: limiter as Parameters<typeof registerExportRoutes>[1]['exportLimiter'],
    getPublicVisibilityGeneration: async () => 1,
    query: fixtureQuery({ nodes: [node(A), node(B, { network: 'other' }), node(C, { name: '🚫 Private' }),
      node(D, { network: 'test' }), node(F, { lat: null })], sighted: true }),
  });
  app.use(router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    for (const format of ['csv', 'geojson']) {
      const response = await fetch(`${base}/v1/exports/nodes.${format}?network=ukmesh&limit=3`);
      assert.equal(response.status, 200);
      const body = await response.text();
      assert.ok(body.includes(A) && body.includes(B));
      for (const id of [C, D, F]) assert.ok(!body.includes(id));
    }
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
