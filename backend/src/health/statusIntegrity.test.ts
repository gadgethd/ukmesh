import assert from 'node:assert/strict';
import test from 'node:test';
import { summarizeAuthoritativeHealth } from './status.js';

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
