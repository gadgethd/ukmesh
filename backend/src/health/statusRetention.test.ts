import assert from 'node:assert/strict';
import test from 'node:test';
import {
  deleteExpiredRows,
  RETENTION_TARGETS,
  retentionDeleteSql,
} from './retention.js';
import { DATA_LIFECYCLE_POLICIES } from '../db/dataLifecycle.js';

test('exact-row cleanup derives identity and duration while retaining its explicit eligible set', () => {
  for (const target of RETENTION_TARGETS) {
    const policy = DATA_LIFECYCLE_POLICIES.find(candidate =>
      candidate.table === target.table || candidate.qualifiedTable === target.table);
    assert.ok(policy);
    assert.equal(target.qualifiedTable ?? target.table, policy.qualifiedTable ?? policy.table);
    assert.equal(target.timestampColumn, policy.timestampColumn);
    assert.equal(target.retention, policy.retention);
  }
  assert.equal(RETENTION_TARGETS.some(target => target.table === ('node_neighbor_samples' as string)), false);
  assert.equal(RETENTION_TARGETS.some(target => target.table === ('packet_paths' as string)), false);
});

test('retention targets include hard packet boundary and reviewed long-lived tables', () => {
  const byTable = new Map(RETENTION_TARGETS.map((target) => [target.table, target]));
  assert.deepEqual(byTable.get('packets'), {
    table: 'packets',
    qualifiedTable: 'mesh.packets',
    timestampColumn: 'time',
    retention: '30 days',
    batchSize: 25_000,
  });
  assert.deepEqual(byTable.get('observer_registration_requests'), {
    table: 'observer_registration_requests',
    qualifiedTable: 'core.observer_registration_requests',
    timestampColumn: 'updated_at',
    retention: '365 days',
    batchSize: 1_000,
    extraPredicate: `status IN ('rejected', 'expired', 'provisioned')`,
  });
  assert.equal(byTable.get('operator_audit_events')?.retention, '730 days');
  assert.deepEqual(byTable.get('node_status_samples'), {
    table: 'node_status_samples',
    qualifiedTable: 'core.node_status_samples',
    timestampColumn: 'time',
    retention: '30 days',
    batchSize: 25_000,
  });
  assert.deepEqual(byTable.get('owner_packet_share_deliveries'), {
    table: 'owner_packet_share_deliveries',
    qualifiedTable: 'core.owner_packet_share_deliveries',
    timestampColumn: 'created_at',
    retention: '30 days',
    batchSize: 2_000,
    extraPredicate: `status IN ('succeeded', 'dead_lettered')`,
  });
  assert.equal(byTable.has('packet_paths'), false);
});

test('bounded retention deletes are deterministic and compressed-table safe', async () => {
  const target = RETENTION_TARGETS.find(({ table }) => table === 'packet_decryptions');
  assert.ok(target);
  const sql = retentionDeleteSql(target);
  assert.match(sql, /ORDER BY created_at ASC/);
  assert.doesNotMatch(sql, /ctid/);
  assert.match(sql, /LIMIT \$2/);
  assert.match(sql, /MAX\(created_at\) AS boundary/);

  const calls: Array<{ text: string; params?: unknown[] }> = [];
  await deleteExpiredRows(target, async (text, params) => {
    calls.push({ text, params });
    return {};
  });
  assert.deepEqual(calls, [{ text: sql, params: ['30 days', 2_000] }]);
});
