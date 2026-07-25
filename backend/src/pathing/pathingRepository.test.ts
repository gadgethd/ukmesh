import assert from 'node:assert/strict';
import test from 'node:test';
import { createPathingRepository } from './pathingRepository.js';

test('path-learning reads filter stale private and out-of-scope node identities', async () => {
  const queries: string[] = [];
  const repository = createPathingRepository({
    getPathHistoryCache: async () => null,
    query: async (text) => {
      queries.push(text);
      return { rows: [] };
    },
  });

  await repository.fetchPathLearning('ukmesh', 6_000);

  assert.equal(queries.length, 5);
  for (const sql of queries.slice(0, 4)) {
    assert.match(sql, /eligible_nodes AS MATERIALIZED/);
    assert.match(sql, /n\.name IS NULL OR n\.name NOT LIKE '%🚫%'/);
    assert.match(sql, /n\.network IS DISTINCT FROM 'test'/);
    assert.match(sql, /s\.last_seen_at > NOW\(\) - INTERVAL '30 days'/);
  }
  assert.match(queries[0]!, /JOIN eligible_nodes n ON n\.node_id = p\.node_id/);
  assert.match(queries[1]!, /JOIN eligible_nodes source ON source\.node_id = p\.from_node_id/);
  assert.match(queries[1]!, /JOIN eligible_nodes target ON target\.node_id = p\.to_node_id/);
  assert.match(queries[3]!, /unnest\(string_to_array\(p\.node_ids, '>'\)\)/);
});
