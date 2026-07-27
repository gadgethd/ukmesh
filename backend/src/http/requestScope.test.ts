import assert from 'node:assert/strict';
import test from 'node:test';
import {
  PublicAllScopeForbiddenError,
  resolvePublicNetworkScope,
  resolvePublicVisibilityScope,
} from './requestScope.js';

test('public network scope defaults to ukmesh and retains legacy aliases', () => {
  assert.equal(resolvePublicNetworkScope(undefined, {}), 'ukmesh');
  assert.equal(resolvePublicNetworkScope('teesside', {}), 'ukmesh');
  assert.equal(resolvePublicNetworkScope('test', {}), 'test');
});

test('public network scope rejects cross-network aggregation', () => {
  assert.throws(
    () => resolvePublicNetworkScope('all', {}),
    PublicAllScopeForbiddenError,
  );
});

test('visibility scope owns public network and observer selection', () => {
  const scope = resolvePublicVisibilityScope('ukmesh', {}, 'observer-1');

  assert.deepEqual(scope, {
    access: 'public',
    network: 'ukmesh',
    observer: 'observer-1',
  });
  assert.equal(Object.isFrozen(scope), true);
});
