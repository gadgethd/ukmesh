import assert from 'node:assert/strict';
import test from 'node:test';
import { setImmediate } from 'node:timers/promises';
import { clearResolvedPaths, invalidateResolveCache } from './resolveCache.js';
import { createResolveCoordinator, StalePathResolutionError } from './resolveCoordinator.js';

test('shares identical path work but separates later observations and other observer scopes', async () => {
  clearResolvedPaths();
  const computations: Array<(value: unknown) => void> = [];
  const resolver = createResolveCoordinator({
    run: async <T>() => await new Promise<unknown>(resolve => computations.push(resolve)) as T,
    runBackground: async () => null,
  }, async () => 1);
  const request = { kind: 'single' as const, packetHash: 'AA00', network: 'ukmesh', observer: 'aa' };
  const original = resolver.resolve(request);
  const joined = resolver.resolve(request);
  await setImmediate();
  assert.equal(computations.length, 1);
  const stale = Promise.all([assert.rejects(original, StalePathResolutionError), assert.rejects(joined, StalePathResolutionError)]);

  invalidateResolveCache(request.packetHash.toLowerCase());
  const updated = resolver.resolve(request);
  const otherObserver = resolver.resolve({ ...request, observer: 'bb' });
  await setImmediate();
  assert.equal(computations.length, 3);
  computations[1]!({ packetHash: 'AA00', observers: ['aa'] });
  computations[2]!({ packetHash: 'AA00', observers: ['bb'] });
  assert.deepEqual((await updated)?.payload.observers, ['aa']);
  assert.deepEqual((await otherObserver)?.payload.observers, ['bb']);
  computations[0]!({ packetHash: 'AA00', observers: [] });
  await stale;
  assert.deepEqual((await resolver.resolve(request))?.payload.observers, ['aa']);
  assert.equal(computations.length, 3);
});

test('a stream gap and a privacy change both withhold in-flight work', async () => {
  clearResolvedPaths();
  const computations: Array<(value: unknown) => void> = [];
  let generation = 1;
  const resolver = createResolveCoordinator({
    run: async () => null as never,
    runBackground: async <T>() => await new Promise<unknown>(resolve => computations.push(resolve)) as T,
  }, async () => generation);
  const request = { kind: 'multi' as const, packetHash: 'AB01', network: 'ukmesh' };
  for (const [index, invalidate] of [clearResolvedPaths, () => { generation++; }].entries()) {
    const result = resolver.resolve(request, 'background');
    const stale = assert.rejects(result, StalePathResolutionError);
    await setImmediate();
    invalidate();
    computations[index]!({ packetHash: request.packetHash, canonicalPath: [] });
    await stale;
  }
});

test('rematerialization withholds cached and in-flight paths before the generation changes', async () => {
  clearResolvedPaths();
  const request = { kind: 'single' as const, packetHash: 'CE01', network: 'ukmesh' };
  const { promise, resolve: finish } = Promise.withResolvers<unknown>();
  let ready = true;
  let runs = 0;
  const resolver = createResolveCoordinator({
    run: async <T>() => (++runs === 1 ? { packetHash: request.packetHash } : await promise) as T,
    runBackground: async () => null,
  }, async () => {
    if (!ready) throw new Error('PUBLIC_VISIBILITY_GENERATION_UNAVAILABLE');
    return 1;
  });
  await resolver.resolve(request);
  const loading = resolver.resolve({ ...request, packetHash: 'CE02' });
  const withheld = assert.rejects(loading, StalePathResolutionError);
  await setImmediate();
  ready = false;
  await assert.rejects(resolver.resolve(request), StalePathResolutionError);
  finish({ packetHash: 'CE02' });
  await withheld;
  assert.equal(runs, 2);
});
