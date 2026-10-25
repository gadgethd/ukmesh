import assert from 'node:assert/strict';
import test from 'node:test';
import { linkHistoryRows, visibleLinkNodeIds, type QueryFn } from './productFeatures.js';

test('public link history authorizes both endpoints through network and privacy scope', async () => {
  let capturedSql = '';
  let capturedParams: unknown[] | undefined;
  const query: QueryFn = async (sql, params) => {
    capturedSql = sql;
    capturedParams = params;
    return { rows: [{ node_id: 'A'.repeat(64) }] };
  };
  const nodeIds = ['A'.repeat(64), 'B'.repeat(64)];
  const networks = ['ukmesh', 'northeast', 'teesside'];

  const result = await visibleLinkNodeIds(query, nodeIds, networks);

  assert.equal(result.rows.length, 1);
  assert.deepEqual(capturedParams, [nodeIds, networks]);
  assert.match(capturedSql, /n\.node_id = ANY\(\$1::text\[\]\)/);
  assert.match(capturedSql, /n\.name IS NULL OR n\.name NOT LIKE '%🚫%'/);
  assert.match(capturedSql, /n\.network = ANY\(\$2::text\[\]\)/);
  assert.match(capturedSql, /sighting\.network = ANY\(\$2::text\[\]\)/);
  assert.match(capturedSql, /sighting\.last_seen_at > NOW\(\) - INTERVAL '30 days'/);
  assert.doesNotMatch(capturedSql, /SELECT .* FROM node_link_radio_reports/is);
});

test('link history binds both identities, the time window and packet network scope', async () => {
  const calls: Array<{ sql: string; params: unknown[] | undefined }> = [];
  const query: QueryFn = async (sql, params) => {
    calls.push({ sql, params });
    return { rows: [] };
  };
  const nodeA = 'A'.repeat(64);
  const nodeB = 'B'.repeat(64);

  assert.deepEqual(await linkHistoryRows(query, nodeA, nodeB, 72, ['test']), { rows: [] });
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].params, [nodeA, nodeB, '72', ['test']]);
  // Regressions here would either revive the orphaned source, hide aliases,
  // or turn indexed packet lookups into per-row identity function calls.
  assert.match(calls[0].sql, /FROM packets p/);
  assert.match(calls[0].sql, /FROM node_identity_aliases alias/);
  assert.doesNotMatch(calls[0].sql, /node_identity_link_radio_reports/);
  assert.doesNotMatch(calls[0].sql, /meshcore_canonical_node_id\(p\./);
});
