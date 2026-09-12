import assert from 'node:assert/strict';
import test from 'node:test';
import { deleteOwnerAlertRule } from './ownerAlerts.js';

test('deleteOwnerAlertRule returns true and scopes the delete by owner and node', async () => {
  let capturedSql = '';
  let capturedParams: unknown[] | undefined;
  const deleted = await deleteOwnerAlertRule(
    async (sql, params) => {
      capturedSql = sql;
      capturedParams = params;
      return { rows: [], rowCount: 1 };
    },
    '42',
    'owner',
    ['node-a', 'node-b'],
  );

  assert.equal(deleted, true);
  assert.match(capturedSql, /DELETE FROM owner_alert_rules/);
  assert.match(
    capturedSql,
    /WHERE id = \$1 AND owner_username = \$2 AND node_id = ANY\(\$3::text\[\]\)/,
  );
  assert.deepEqual(capturedParams, ['42', 'owner', ['node-a', 'node-b']]);
});

test('deleteOwnerAlertRule returns false when no row was deleted', async () => {
  const deleted = await deleteOwnerAlertRule(
    async () => ({ rows: [], rowCount: 0 }),
    '42',
    'owner',
    ['node-a'],
  );

  assert.equal(deleted, false);
});

test('deleteOwnerAlertRule returns false when the row count is unknown', async () => {
  const deleted = await deleteOwnerAlertRule(
    async () => ({ rows: [], rowCount: null }),
    '42',
    'owner',
    ['node-a'],
  );

  assert.equal(deleted, false);
});
