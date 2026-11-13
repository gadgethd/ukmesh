import assert from 'node:assert/strict';
import test from 'node:test';
import { CACHE_POLICY_REGISTRY } from '../cache/policyRegistry.js';
import { boundedCacheMetricLabel } from './labels.js';

test('every bounded cache has its own finite metric label', () => {
  const policies = CACHE_POLICY_REGISTRY.filter(policy => policy.disposition === 'bounded-cache');
  const labels = policies.map(policy => boundedCacheMetricLabel(policy.name));
  assert.ok(!labels.includes('other'));
  assert.equal(new Set(labels).size, policies.length);
  assert.equal(boundedCacheMetricLabel('arbitrary-unregistered-cache'), 'other');
});
