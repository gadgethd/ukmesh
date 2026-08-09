import assert from 'node:assert/strict';
import test from 'node:test';
import { PrivatePrefixCache } from './privatePrefixCache.js';

const privateId = `ABCD${'1'.repeat(60)}`;

test('private prefix cache fails closed until a fenced generation is loaded', () => {
  const cache = new PrivatePrefixCache();
  assert.deepEqual(cache.classify({
    network: 'ukmesh', rxNodeId: '2'.repeat(64), srcNodeId: null,
    pathHashes: ['1234'], pathHashSizeBytes: 2,
  }), {
    generation: 0, isPrivate: true, pathIsValid: true, visibilityOk: false,
  });
});

test('private prefix cache preserves public-family and exact-network matching', () => {
  const cache = new PrivatePrefixCache();
  cache.replace(7, [
    { node_id: privateId, network: 'northeast', prefix_size_bytes: 2, prefix: 'ABCD' },
    { node_id: 'EF01'.padEnd(64, '2'), network: 'test', prefix_size_bytes: 2, prefix: 'EF01' },
  ]);

  assert.equal(cache.classify({
    network: 'teesside', rxNodeId: null, srcNodeId: null,
    pathHashes: ['abcd'], pathHashSizeBytes: 2,
  }).isPrivate, true);
  assert.equal(cache.classify({
    network: 'ukmesh', rxNodeId: privateId.toLowerCase(), srcNodeId: null,
    pathHashes: [], pathHashSizeBytes: null,
  }).isPrivate, true);
  assert.equal(cache.classify({
    network: 'test', rxNodeId: null, srcNodeId: null,
    pathHashes: ['ABCD'], pathHashSizeBytes: 2,
  }).isPrivate, false);
});

test('private prefix cache applies the exact persisted path validity rules', () => {
  const cache = new PrivatePrefixCache();
  cache.replace(3, []);
  assert.equal(cache.classify({
    network: 'ukmesh', rxNodeId: null, srcNodeId: null,
    pathHashes: ['AB'], pathHashSizeBytes: 2,
  }).visibilityOk, false);
  assert.equal(cache.classify({
    network: 'ukmesh', rxNodeId: null, srcNodeId: null,
    pathHashes: [], pathHashSizeBytes: 9,
  }).visibilityOk, true);
});
