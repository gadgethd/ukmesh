import assert from 'node:assert/strict';
import test from 'node:test';
import {
  getResolveCache,
  getStickyNodeMap,
  invalidateResolveCache,
  mergeStickyNodes,
  resolveCacheMetrics,
  setResolveCache,
} from './resolveCache.js';

test('resolve cache invalidates a packet without a full-cache scan and remains bounded', () => {
  const packetHash = 'ABCD1234';
  const key = `r|${packetHash}|ukmesh||v1`;
  setResolveCache(key, { resolved: true });
  assert.deepEqual(getResolveCache(key), { resolved: true });
  invalidateResolveCache(packetHash);
  assert.equal(getResolveCache(key), undefined);

  for (let index = 0; index < 4_200; index += 1) {
    setResolveCache(`r|hash-${index}|ukmesh||v1`, { index });
  }
  assert.ok(resolveCacheMetrics().results.size <= 4_096);
});

test('sticky anchors cap nested entries per packet', () => {
  const updates = Object.fromEntries(
    Array.from({ length: 100 }, (_, index) => [`${index}`, `node-${index}`]),
  );
  mergeStickyNodes('sticky-packet', 'ukmesh', updates);
  assert.equal(getStickyNodeMap('sticky-packet', 'ukmesh')?.hashToNodeId.size, 64);
});
