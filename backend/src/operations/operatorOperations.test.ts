import assert from 'node:assert/strict';
import test from 'node:test';
import {
  operatorActor,
  validateDecisionReason,
  validateIdempotencyKey,
} from './operatorOperations.js';

test('operator mutation inputs are bounded and canonical', () => {
  assert.equal(validateIdempotencyKey('request-key-1234567890'), 'request-key-1234567890');
  assert.throws(() => validateIdempotencyKey('short'), /INVALID_IDEMPOTENCY_KEY/);
  assert.throws(
    () => validateIdempotencyKey('request key with spaces'),
    /INVALID_IDEMPOTENCY_KEY/,
  );
  assert.equal(validateDecisionReason(' Reviewed and approved '), 'Reviewed and approved');
  assert.throws(() => validateDecisionReason('no'), /INVALID_DECISION_REASON/);
});

test('browser audit actors are stable, pseudonymous, and distinct from automation', () => {
  const first = operatorActor('session', 'secret-session-token');
  const again = operatorActor('session', 'secret-session-token');
  assert.deepEqual(first, again);
  assert.doesNotMatch(first.id, /secret-session-token/);
  assert.notEqual(first.id, operatorActor('session', 'other-session-token').id);
  assert.deepEqual(operatorActor('automation'), {
    id: 'automation-token',
    mode: 'automation',
  });
});
