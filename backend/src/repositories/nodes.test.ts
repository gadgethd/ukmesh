import assert from 'node:assert/strict';
import test from 'node:test';
import { networkFilters } from '../api/utils/networkFilters.js';
import type { QueryResultRow } from 'pg';
import { createNodeRepository, type NodeLinkRow } from './nodes.js';

test('public map projects path evidence as the effective seen and online state', async () => {
  let capturedSql = '';
  let capturedParams: unknown[] | undefined;
  const repository = createNodeRepository(async (sql, params) => {
    capturedSql = sql;
    capturedParams = params;
    return { rows: [] };
  });
  const snapshot = '2026-08-03T12:00:00.000Z';

  await repository.listPublicMapRows(
    ['node_id', 'last_seen', 'is_online'],
    networkFilters('ukmesh'),
    snapshot,
    null,
    100,
  );

  assert.match(capturedSql, /GREATEST\([\s\S]*n\.last_path_evidence_at[\s\S]*\)::text AS last_seen/);
  assert.match(
    capturedSql,
    /n\.last_path_evidence_at > \$2::timestamptz - INTERVAL '60 minutes'[\s\S]*THEN TRUE[\s\S]*AS is_online/,
  );
  assert.doesNotMatch(capturedSql, /n\.last_seen::text AS last_seen/);
  assert.deepEqual(capturedParams, [
    ['ukmesh', 'northeast', 'teesside'],
    snapshot,
    null,
    101,
  ]);
});

test('node links preserve metrics and fetch one scoped aggregate for the entire peer set', async () => {
  const nodeId = 'A'.repeat(64);
  const { recent_count_72h, ...link } = {
    peer_id: 'B'.repeat(64), peer_name: 'Peer', observed_count: 999,
    itm_path_loss_db: 100, count_this_to_peer: 500, count_peer_to_this: 499,
    last_observed: '2026-09-07T12:00:00.123Z', recent_count_72h: 0,
  } satisfies NodeLinkRow;
  const calls: Array<{ sql: string; params?: unknown[] }> = [];
  const rows = [{ ...link, recent_count_72h }];
  const repository = createNodeRepository(async <T extends QueryResultRow>(sql: string, params?: unknown[]) => {
    calls.push({ sql, params });
    return { rows: (calls.length === 1 ? [link] : rows) as unknown as T[] };
  });
  assert.deepEqual(await repository.listNodeLinks(nodeId, networkFilters('ukmesh')), rows);
  assert.equal(calls.length, 2);
  assert.match(calls[0]!.sql, /to_char\(rl.last_observed AT TIME ZONE 'UTC'/);
  const aggregate = calls[1]!;
  assert.deepEqual(aggregate.params, [['ukmesh', 'northeast', 'teesside'], nodeId, JSON.stringify([link])]);
  assert.equal(aggregate.sql.match(/FROM packets p/g)?.length, 1);
  assert.match(aggregate.sql, /COUNT\(\*\)::integer AS recent_count_72h/);
  assert.match(aggregate.sql, /GROUP BY pa.peer_id/);
  assert.match(aggregate.sql, /LEFT JOIN recent_counts/);
  assert.match(aggregate.sql, /COALESCE\(rc.recent_count_72h, 0\)::integer/);
  assert.match(aggregate.sql, /p.src_node_id IS NOT NULL/);
  assert.match(aggregate.sql, /p.time > NOW\(\) - INTERVAL '72 hours'/);
  assert.match(aggregate.sql, /p.time <= NOW\(\)/);
  assert.match(aggregate.sql, /p.network = ANY\(\$1\)/);
  assert.match(aggregate.sql, /p.visibility_ok IS TRUE/);
  assert.match(aggregate.sql, /p.is_private IS NOT TRUE/);
  assert.match(aggregate.sql, /cached_visibility.visibility_generation = current_visibility.generation/);
});

test('empty or invisible link sets never scan packets', async () => {
  let calls = 0;
  const repository = createNodeRepository(async () => { calls += 1; return { rows: [] }; });
  assert.deepEqual(await repository.listNodeLinks('A'.repeat(64), networkFilters('ukmesh')), []);
  assert.equal(calls, 1);
});
