import assert from 'node:assert/strict';
import test from 'node:test';
import { createApiCaches } from './caches.js';

test('API runtimes retain independent snapshots and close their own storage', t => {
  const first = createApiCaches();
  const second = createApiCaches();
  t.after(first.close); t.after(second.close);
  first.statsCache.set('ukmesh:v1', { ts: 1, data: 'first' });
  second.statsCache.set('ukmesh:v1', { ts: 2, data: 'second' });
  first.close();
  assert.equal(first.statsCache.size, 0);
  assert.equal(second.statsCache.get('ukmesh:v1')?.data, 'second');
});
