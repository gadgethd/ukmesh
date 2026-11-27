import assert from 'node:assert/strict';
import test from 'node:test';
import { setImmediate } from 'node:timers/promises';
import { createPathResolutionRuntime } from './liveResolution.js';
import { getResolveCache, resolveCacheKey, setResolveCache } from './resolveCache.js';

test('committed observations invalidate paths and coalesce optional warmups', async (t) => {
  t.mock.method(Date, 'now', () => 100_000);
  const enabled = process.env['PATH_BETA_PRERESOLVE_ENABLED'];
  process.env['PATH_BETA_PRERESOLVE_ENABLED'] = '1';
  t.after(() => {
    if (enabled === undefined) delete process.env['PATH_BETA_PRERESOLVE_ENABLED'];
    else process.env['PATH_BETA_PRERESOLVE_ENABLED'] = enabled;
  });
  const observation = { packetHash: 'a'.repeat(64), network: 'ukmesh', slow: false, warm: true };
  const key = resolveCacheKey('multi', observation.packetHash, observation.network);
  const result = { canonicalPath: [] };
  let finish!: () => void;
  const gate = new Promise<void>(resolve => { finish = resolve; });
  let calls = 0;
  const service = createPathResolutionRuntime({
    run: async () => null,
    runBackground: async <T>() => { calls++; await gate; return result as unknown as T; },
    close: async () => undefined,
  });
  t.after(async () => { finish(); await service.close(); });
  setResolveCache(key, result);
  service.acceptObservation(observation);
  service.acceptObservation(observation);
  assert.equal(getResolveCache(key), undefined);
  assert.equal(calls, 1);
  let closed = false;
  const closing = service.close().then(() => { closed = true; });
  await setImmediate();
  assert.equal(closed, false);
  finish();
  await closing;
  assert.equal(getResolveCache(key), undefined, 'closing runtime does not publish a stale warmup');
  service.acceptObservation({ ...observation, packetHash: 'b'.repeat(64) });
  assert.equal(calls, 1);
});

test('losing the observation subscription clears the path cache', async (t) => {
  const service = createPathResolutionRuntime({
    run: async () => null,
    runBackground: async () => null,
    close: async () => undefined,
  });
  t.after(() => service.close());
  const key = resolveCacheKey('multi', 'c'.repeat(64), 'ukmesh');
  setResolveCache(key, { canonicalPath: [] });
  // A subscriber cannot recover invalidations sent while it was offline.
  service.clearResolvedPaths();
  assert.equal(getResolveCache(key), undefined);
});
