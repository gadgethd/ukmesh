import assert from 'node:assert/strict';
import test from 'node:test';
import {
  deleteExpiredRows,
  RETENTION_TARGETS,
  retentionDeleteSql,
} from './status.js';

test('retention targets include hard packet boundary and reviewed long-lived tables', () => {
  const byTable = new Map(RETENTION_TARGETS.map((target) => [target.table, target]));
  assert.deepEqual(byTable.get('packets'), {
    table: 'packets',
    timestampColumn: 'time',
    retention: '30 days',
    batchSize: 25_000,
  });
  assert.deepEqual(byTable.get('observer_registration_requests'), {
    table: 'observer_registration_requests',
    timestampColumn: 'updated_at',
    retention: '365 days',
    batchSize: 1_000,
    extraPredicate: `status IN ('rejected', 'expired', 'provisioned')`,
  });
  assert.equal(byTable.get('operator_audit_events')?.retention, '730 days');
  assert.equal(byTable.has('packet_paths'), false);
});

test('bounded retention deletes are deterministic and physical-row exact', async () => {
  const target = RETENTION_TARGETS.find(({ table }) => table === 'packet_decryptions');
  assert.ok(target);
  const sql = retentionDeleteSql(target);
  assert.match(sql, /ORDER BY created_at ASC, tableoid, ctid/);
  assert.match(sql, /LIMIT \$2/);
  assert.match(sql, /target\.tableoid = expired\.tableoid/);

  const calls: Array<{ text: string; params?: unknown[] }> = [];
  await deleteExpiredRows(target, async (text, params) => {
    calls.push({ text, params });
    return {};
  });
  assert.deepEqual(calls, [{ text: sql, params: ['30 days', 2_000] }]);
});
