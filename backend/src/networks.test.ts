import assert from 'node:assert/strict';
import test from 'node:test';
import { expandResolverScope, networkMatchesScope, UKMESH_NETWORKS } from './networks.js';
import { normalizeNetworkValue, resolveRequestNetwork } from './http/requestScope.js';

test('UKMesh scope retains legacy production data without admitting test traffic', () => {
  assert.deepEqual(UKMESH_NETWORKS, ['ukmesh', 'northeast', 'teesside']);
  assert.equal(networkMatchesScope('ukmesh', 'ukmesh'), true);
  assert.equal(networkMatchesScope('northeast', 'ukmesh'), true);
  assert.equal(networkMatchesScope('teesside', 'ukmesh'), true);
  assert.equal(networkMatchesScope('test', 'ukmesh'), false);
  assert.equal(networkMatchesScope('test', 'test'), true);
  assert.deepEqual(expandResolverScope('ukmesh'), ['ukmesh', 'northeast', 'teesside']);
  assert.deepEqual(expandResolverScope('test'), ['test']);
});

test('request scope aliases legacy production labels but preserves explicit test isolation', () => {
  assert.equal(normalizeNetworkValue('teesside'), 'ukmesh');
  assert.equal(normalizeNetworkValue('northeast'), 'ukmesh');
  assert.equal(normalizeNetworkValue('test'), 'test');
  assert.equal(normalizeNetworkValue('all'), 'all');
  assert.equal(normalizeNetworkValue('unknown'), undefined);
  assert.equal(resolveRequestNetwork('test', {}, 'ukmesh'), 'test');
});
