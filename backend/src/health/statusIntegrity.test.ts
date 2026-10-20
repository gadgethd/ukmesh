import assert from 'node:assert/strict';
import test from 'node:test';
import { privacyRematerializationBacklogProblem, summarizeAuthoritativeHealth } from './status.js';

test('anonymous frontend diagnostics cannot forge health severity', () => {
  assert.deepEqual(summarizeAuthoritativeHealth([], 1_000_000), {
    status: 'healthy',
    frontendErrors: 1_000_000,
  });
  assert.deepEqual(summarizeAuthoritativeHealth([
    { code: 'queue', severity: 'warning', message: 'bounded fixture' },
  ], 0), {
    status: 'degraded',
    frontendErrors: 0,
  });
  assert.deepEqual(summarizeAuthoritativeHealth([
    { code: 'dependency', severity: 'critical', message: 'bounded fixture' },
  ], Number.NaN), {
    status: 'critical',
    frontendErrors: 0,
  });
});

test('privacy rematerialization backlog ignores active work and classifies stale or failed work', () => {
  const state = {
    pending: 0, processing: 1, failed: 0, failedRetryable: 0,
    failedExhausted: 0, oldestPendingAgeSeconds: null,
  };
  assert.equal(privacyRematerializationBacklogProblem(state), null);
  assert.equal(privacyRematerializationBacklogProblem({ ...state, pending: 1, oldestPendingAgeSeconds: 1_801 })?.severity, 'warning');
  assert.equal(privacyRematerializationBacklogProblem({ ...state, failed: 1, failedRetryable: 1 })?.severity, 'warning');
  const exhausted = privacyRematerializationBacklogProblem({ ...state, failed: 1, failedExhausted: 1 });
  assert.equal(exhausted?.severity, 'critical');
  assert.match(exhausted?.message ?? '', /pending=0 processing=1 failed=1 oldest_pending_age_minutes=0/);
  assert.equal(privacyRematerializationBacklogProblem({ ...state, pending: 1, oldestPendingAgeSeconds: 21_601 })?.severity, 'critical');
});
