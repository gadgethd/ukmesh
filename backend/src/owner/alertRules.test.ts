import assert from 'node:assert/strict';
import test from 'node:test';
import {
  deliveryFailureTransition,
  ownerAlertTriggered,
} from './alertRules.js';

test('owner alert conditions are deterministic at the supplied clock boundary', () => {
  const now = Date.parse('2026-07-29T12:00:00Z');
  assert.equal(ownerAlertTriggered({
    rule_type: 'offline_minutes',
    threshold: 30,
    last_seen: '2026-07-29T11:30:00Z',
    battery_mv: null,
    path_loss_db: null,
  }, now), true);
  assert.equal(ownerAlertTriggered({
    rule_type: 'battery_below_mv',
    threshold: 3_300,
    last_seen: null,
    battery_mv: 3_301,
    path_loss_db: null,
  }, now), false);
  assert.equal(ownerAlertTriggered({
    rule_type: 'link_loss_above_db',
    threshold: 120,
    last_seen: null,
    battery_mv: null,
    path_loss_db: 121,
  }, now), true);
});

test('owner alert retry is bounded and the fifth failure is terminal', () => {
  assert.deepEqual(deliveryFailureTransition(1), {
    status: 'failed',
    retryAfterSeconds: 30,
  });
  assert.deepEqual(deliveryFailureTransition(4), {
    status: 'failed',
    retryAfterSeconds: 240,
  });
  assert.deepEqual(deliveryFailureTransition(5), {
    status: 'dead_lettered',
    retryAfterSeconds: 0,
  });
});
