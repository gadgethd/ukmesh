import assert from 'node:assert/strict';
import test from 'node:test';
import { networkFilters } from '../api/utils/networkFilters.js';
import { createNodeRepository } from './nodes.js';

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
