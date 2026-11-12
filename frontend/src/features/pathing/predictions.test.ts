import assert from 'node:assert/strict';
import test from 'node:test';
import { createPredictionResource } from './predictions.js';
import type { AggregatedPredictionState } from './model.js';

const answer = (hash = 'packet') => Response.json({ packetHash: hash, canonicalPath: [], observers: [] });

test('prediction results share sorted observer identity, honor freshness and bypass eager cache for slow mode', async () => {
  let stamp = 10;
  const urls: string[] = [];
  const resource = createPredictionResource('scope', async input => {
    urls.push(String(input)); return answer();
  }, () => stamp);
  await resource.resolve('scope', 'packet', ['b', 'a'], 'ukmesh');
  await resource.resolve('scope', 'packet', ['a', 'b'], 'ukmesh');
  assert.equal(urls.length, 1);
  stamp = 11;
  await resource.resolve('scope', 'packet', ['a', 'b'], 'ukmesh', 11);
  assert.equal(urls.length, 2);
  await resource.resolve('scope', 'packet', ['a', 'b'], 'ukmesh', 0, 'slow');
  assert.equal(urls.length, 3);
  assert.equal(new URL(urls[2]!, 'http://local.test').searchParams.get('mode'), 'slow');
  resource.dispose();
});

test('eager and slow requests singleflight independently and superseded physical work keeps its capacity', async () => {
  const requests: Array<{ signal: AbortSignal; resolve: (value: Response) => void }> = [];
  const resource = createPredictionResource('old', async (_input, init) => new Promise<Response>(resolve => {
    requests.push({ signal: init!.signal!, resolve });
  }));
  const eager = resource.resolve('old', 'packet', [], 'ukmesh');
  const eagerAgain = resource.resolve('old', 'packet', [], 'ukmesh');
  const slow = resource.resolve('old', 'packet', [], 'ukmesh', 0, 'slow');
  assert.equal(requests.length, 2);
  resource.setScope('new');
  assert.equal(requests.every(request => request.signal.aborted), true);
  assert.equal(resource.snapshot().inflight, 2);
  assert.equal(await resource.resolve('new', 'another', [], 'ukmesh'), null);
  requests.forEach(request => request.resolve(answer()));
  assert.deepEqual(await Promise.all([eager, eagerAgain, slow]), [null, null, null]);
  assert.equal(resource.snapshot().entries, 0);
  assert.equal(resource.snapshot().inflight, 0);
  assert.equal(await resource.resolve('old', 'packet', [], 'ukmesh'), null);
});

test('disposing a pending slow-mode wait cancels its retry without retaining a timer', async () => {
  let calls = 0;
  const resource = createPredictionResource('scope', async () => {
    calls++; return Response.json({ status: 'pending', remainingMs: 30_000 }, { status: 202 });
  });
  const result = resource.resolve('scope', 'packet', [], 'ukmesh', 0, 'slow');
  await new Promise<void>(done => setImmediate(done));
  resource.dispose();
  assert.equal(await result, null);
  assert.equal(calls, 1);
  assert.equal(resource.snapshot().inflight, 0);
});

test('recent prediction fallback has an exact 48-item bound and expires independently', () => {
  let stamp = 100;
  const resource = createPredictionResource('scope', async () => answer(), () => stamp);
  for (let index = 0; index < 60; index++) resource.remember(String(index), { ts: index } as AggregatedPredictionState);
  assert.equal(resource.snapshot().recent, 48);
  assert.equal(resource.recent('0'), undefined);
  assert.ok(resource.recent('59'));
  stamp = 46_000;
  assert.equal(resource.snapshot().recent, 0);
  resource.dispose();
});
