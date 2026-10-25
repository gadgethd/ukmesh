import assert from 'node:assert/strict';
import test from 'node:test';
import {
  OWNER_GRANT_REVOCATION_GRACE_MS,
  planOperatorConfiguredOwnerGrantSync,
  type CurrentOwnerGrant,
} from './ownerGrantReconciliation.js';

const NODE_A = 'A1'.repeat(32);
const GENERATION = 'config-generation';
const NOW = Date.parse('2026-08-30T12:00:00.000Z');

function currentGrant(overrides: Partial<CurrentOwnerGrant> = {}): CurrentOwnerGrant {
  return {
    mqttUsername: 'owner',
    nodeId: NODE_A,
    revokedAt: null,
    verificationMethod: 'operator-config',
    grantGeneration: null,
    updatedAt: new Date(NOW - 1_000).toISOString(),
    ...overrides,
  };
}

test('a grant inserted before rollout is never revoked when the new map contains it', () => {
  const desired = [{ mqttUsername: 'owner', nodeId: NODE_A }];
  const actions = planOperatorConfiguredOwnerGrantSync(
    desired,
    [currentGrant()],
    GENERATION,
    { nowMs: NOW },
  );

  assert.deepEqual(actions, [{ type: 'upsert', grant: desired[0], reauthorize: false }]);
  assert.equal(actions.some((action) => action.type === 'revoke'), false);
});

test('a stale-map reconcile skips a newly inserted grant during the rollout grace', () => {
  const actions = planOperatorConfiguredOwnerGrantSync(
    [],
    [currentGrant()],
    GENERATION,
    { nowMs: NOW },
  );

  assert.deepEqual(actions, []);
});

test('a newly revoked mapped grant skips the tombstone guard and is reauthorized', () => {
  const desired = [{ mqttUsername: 'owner', nodeId: NODE_A }];
  const actions = planOperatorConfiguredOwnerGrantSync(
    desired,
    [currentGrant({
      revokedAt: new Date(NOW - OWNER_GRANT_REVOCATION_GRACE_MS / 2).toISOString(),
    })],
    GENERATION,
    { nowMs: NOW },
  );

  assert.deepEqual(actions, [{ type: 'upsert', grant: desired[0], reauthorize: true }]);
});

test('an unmapped config grant is revoked after the rollout grace expires', () => {
  const actions = planOperatorConfiguredOwnerGrantSync(
    [],
    [currentGrant({
      updatedAt: new Date(NOW - OWNER_GRANT_REVOCATION_GRACE_MS).toISOString(),
    })],
    GENERATION,
    { nowMs: NOW },
  );

  assert.deepEqual(actions, [{
    type: 'revoke',
    grant: { mqttUsername: 'owner', nodeId: NODE_A },
  }]);
});
