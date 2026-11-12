import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { closeDb } from '../db/runtime.js';
import type { DatabaseQueryFn } from '../db/contracts.js';
import { spamDetectorRefreshHealthy } from '../metrics.js';
import { evaluateAdvert, refreshSpamDetectorCaches } from './spamDetector.js';
import { readSpamSnapshot } from './spamSnapshot.js';

const databaseUrl = process.env['TEST_INGEST_DATABASE_URL'];
test('spam snapshots execute complete bounded SQL and retain verdicts on a rejected refresh', {
  skip: databaseUrl ? false : 'TEST_INGEST_DATABASE_URL is not configured', timeout: 60_000,
}, async t => {
  const admin = new pg.Pool({ connectionString: databaseUrl, max: 1 });
  const database = `meshcore_spam_${process.pid}_${Date.now()}`;
  const url = new URL(databaseUrl!); url.pathname = `/${database}`;
  await admin.query(`CREATE DATABASE "${database}"`);
  const fixture = new pg.Pool({ connectionString: url.toString(), max: 2 });
  t.after(async () => {
    await fixture.end(); await closeDb();
    await admin.query(`DROP DATABASE "${database}" WITH (FORCE)`); await admin.end();
  });
  const query: DatabaseQueryFn = (sql, params) => fixture.query(sql, params);
  await fixture.query(`CREATE SCHEMA core; CREATE SCHEMA mesh;
    CREATE TABLE core.nodes (node_id text, advert_count integer, last_seen timestamptz);
    CREATE TABLE mesh.packets (src_node_id text, rx_node_id text, packet_hash text,
      packet_type integer, time timestamptz, hop_count integer, payload jsonb);
    INSERT INTO core.nodes SELECT repeat(key,64), 30, NOW() FROM unnest(ARRAY['A','B','C','D','E','F']) key;
    INSERT INTO mesh.packets
      SELECT repeat(key,64), 'observer-'||(i%5), key||'-'||i, 4, NOW()-i*INTERVAL '1 second', 2,
        jsonb_build_object('_summary','Invented hill','publicKey',repeat(key,64),
          'appData',jsonb_build_object('location',jsonb_build_object('latitude',54,'longitude',-1)))
      FROM unnest(ARRAY['A','B','C','D','E','F']) key
      CROSS JOIN generate_series(1,30) i WHERE i <= 30-ascii(key)+ascii('A');
    INSERT INTO mesh.packets
      SELECT repeat('F',64), 'observer-'||(i%5), 'rotation-'||i, 4, NOW()-i*INTERVAL '1 second', 2,
        jsonb_build_object('_summary','Invented rotation '||(i%5),'publicKey',repeat('F',64))
      FROM generate_series(1,25) i;
  `);
  t.mock.method(console, 'log', () => undefined);
  t.mock.method(console, 'error', () => undefined);
  const inputs = ['A','B','C','D','E','F','0'].map(key => ({
    name: 'Invented hill', publicKey: key.repeat(64), srcNodeId: key.repeat(64),
    network: 'ukmesh', lat: 50, lon: -1,
  }));
  await refreshSpamDetectorCaches(query);
  const actual = await Promise.all(inputs.map(evaluateAdvert));
  assert.ok(actual.slice(0,6).every(result => result.verdict === 'spam'));
  assert.equal(actual[6].verdict, 'clean');
  assert.equal(actual[0].canonicalKey, 'A'.repeat(64));
  assert.ok(actual[5].signals.some(signal => signal.name === 'PUBLIC_KEY_NAME_ROTATION'));
  // Optional local comparison runs the untouched previous implementation against
  // this same invented SQL fixture; CI asserts the concrete verdicts above.
  if (process.env['TEST_SPAM_BASELINE_MODULE']) {
    const baseline = await import(process.env['TEST_SPAM_BASELINE_MODULE']);
    baseline.setBaselineQuery(query);
    await baseline.refreshSpamDetectorCaches();
    const expected = await Promise.all(inputs.map(baseline.evaluateAdvert));
    assert.deepEqual(actual, expected);
  }
  const healthy = async () => (await spamDetectorRefreshHealthy.get()).values.find(v => v.labels.dataset === 'identity')!.value;
  assert.equal(await healthy(), 1);
  // An oversized legacy name rejects the whole identity refresh. Existing
  // complete evidence and its verdicts remain available, never a truncated set.
  await fixture.query(`INSERT INTO mesh.packets VALUES ('oversized','observer','oversized',4,NOW(),2,
    jsonb_build_object('_summary',repeat('x',16777217),'publicKey',repeat('9',64)))`);
  await refreshSpamDetectorCaches(query);
  assert.equal(await healthy(), 0);
  assert.deepEqual(await Promise.all(inputs.map(evaluateAdvert)), actual);

  assert.deepEqual(await readSpamSnapshot(query, 'SELECT i FROM generate_series(1,3) i', [], { rows: 3, bytes: 100 }), [{i:1},{i:2},{i:3}]);
  await assert.rejects(readSpamSnapshot(query, 'SELECT i FROM generate_series(1,4) i', [], { rows: 3, bytes: 100 }), /capacity/);
  await assert.rejects(readSpamSnapshot(query, 'SELECT i FROM generate_series(1,100001) i'), /capacity/);
  await assert.rejects(readSpamSnapshot(query, "SELECT repeat('é',100) AS name", [], { rows: 3, bytes: 150 }), /capacity/);
  const oversized = await query(`WITH candidate AS (SELECT repeat('x',1000000) AS name)
    SELECT octet_length(row_to_json(candidate)::text) AS bytes FROM candidate`);
  assert.ok(Number(oversized.rows[0].bytes) > 1_000_000);
  await assert.rejects(readSpamSnapshot(query, "SELECT repeat('x',1000000) AS name", [], { rows: 3, bytes: 100 }), /capacity/);
});
