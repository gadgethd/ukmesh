import assert from 'node:assert/strict';
import test from 'node:test';
import { visibleLinkNodeIds, type QueryFn } from './productFeatures.js';

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
