import assert from 'node:assert/strict';
import test from 'node:test';
import {
  reactivateHistoricPathNodes,
  type HistoricPathNode,
  type QueryFn,
} from './pathEvidence.js';

const NODE_ID = `ABCDEF${'1'.repeat(58)}`;
const SEEN_AT = new Date('2026-08-03T12:34:56.000Z');

test('historic path activation resolves only unique coordinate-bearing relays', async () => {
  let capturedSql = '';
  let capturedParams: unknown[] | undefined;
  const expected: HistoricPathNode = {
    node_id: NODE_ID,
    name: 'Historic relay',
    lat: 54.5,
    lon: -1.2,
    iata: 'MME',
    role: 2,
    last_seen: SEEN_AT.toISOString(),
    is_online: true,
    hardware_model: null,
    public_key: NODE_ID,
    advert_count: 4,
    elevation_m: 120,
    network: 'ukmesh',
  };
  const query: QueryFn = async (sql, params) => {
    capturedSql = sql;
    capturedParams = params;
    return { rows: [expected] };
  };

  const rows = await reactivateHistoricPathNodes(query, {
    pathHashes: ['abcdef', 'ABCDEF', 'not-hex', '1234'],
    sizeBytes: 3,
    seenAt: SEEN_AT,
    routeType: 1,
    network: 'ukmesh',
  });

  assert.deepEqual(rows, [expected]);
  assert.deepEqual(capturedParams, [['ABCDEF'], SEEN_AT.toISOString()]);
  assert.match(capturedSql, /UPPER\(LEFT\(n\.node_id, 6\)\) = i\.hash/);
  assert.match(capturedSql, /n\.network IS DISTINCT FROM 'test'/);
  assert.match(capturedSql, /n\.lat BETWEEN -90 AND 90/);
  assert.match(capturedSql, /n\.lon BETWEEN -180 AND 180/);
  assert.match(capturedSql, /NOT \(ABS\(n\.lat\) < 1e-9 AND ABS\(n\.lon\) < 1e-9\)/);
  assert.match(capturedSql, /HAVING COUNT\(\*\) = 1/);
  assert.match(capturedSql, /SET last_path_evidence_at = \$2::timestamptz/);
  assert.match(capturedSql, /n\.lat,[\s\S]*n\.lon/);
  assert.match(capturedSql, /TRUE AS is_online/);
});

test('historic path activation rejects future routes, short hashes, and missing evidence', async () => {
  let calls = 0;
  const query: QueryFn = async () => {
    calls += 1;
    return { rows: [] };
  };

  assert.deepEqual(await reactivateHistoricPathNodes(query, {
    pathHashes: ['ABCDEF'], sizeBytes: 3, seenAt: SEEN_AT, routeType: 2,
  }), []);
  assert.deepEqual(await reactivateHistoricPathNodes(query, {
    pathHashes: ['AB'], sizeBytes: 1, seenAt: SEEN_AT, routeType: 1,
  }), []);
  assert.deepEqual(await reactivateHistoricPathNodes(query, {
    pathHashes: ['ZZZZ'], sizeBytes: 2, seenAt: SEEN_AT, routeType: 0,
  }), []);
  assert.equal(calls, 0);
});

test('historic test-network activation cannot match public inventory rows', async () => {
  let capturedSql = '';
  const query: QueryFn = async (sql) => {
    capturedSql = sql;
    return { rows: [] };
  };

  await reactivateHistoricPathNodes(query, {
    pathHashes: ['ABCD'],
    sizeBytes: 2,
    seenAt: SEEN_AT,
    routeType: 0,
    network: 'test',
  });

  assert.match(capturedSql, /UPPER\(LEFT\(n\.node_id, 4\)\) = i\.hash/);
  assert.match(capturedSql, /n\.network = 'test'/);
  assert.doesNotMatch(capturedSql, /IS DISTINCT FROM 'test'/);
});
