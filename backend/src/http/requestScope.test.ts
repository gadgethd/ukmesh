import assert from 'node:assert/strict';
import test from 'node:test';
import {
  PublicAllScopeForbiddenError,
  resolvePublicNetworkScope,
} from './requestScope.js';

test('public network scope defaults to production and preserves isolated test', () => {
  assert.equal(resolvePublicNetworkScope(undefined, {}), 'ukmesh');
  assert.equal(resolvePublicNetworkScope('northeast', {}), 'ukmesh');
  assert.equal(resolvePublicNetworkScope('test', {}), 'test');
  assert.equal(resolvePublicNetworkScope('unknown', {}), 'ukmesh');
});

test('public network scope rejects cross-network aggregation', () => {
  assert.throws(
    () => resolvePublicNetworkScope('all', {}),
    PublicAllScopeForbiddenError,
  );
});
