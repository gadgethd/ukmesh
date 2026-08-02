import assert from 'node:assert/strict';
import test from 'node:test';
import { RequestLimiter } from './requestLimiter.js';

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

test('RequestLimiter caps concurrency and drains in order', async () => {
  const limiter = new RequestLimiter(2, 2);
  const first = deferred<number>();
  const second = deferred<number>();
  const third = deferred<number>();
  const signal = new AbortController().signal;
  const results = [
    limiter.run(signal, () => first.promise),
    limiter.run(signal, () => second.promise),
    limiter.run(signal, () => third.promise),
  ];

  await Promise.resolve();
  assert.deepEqual(limiter.snapshot(), {
    active: 2,
    queued: 1,
    maxConcurrent: 2,
    maxQueued: 2,
    rejected: 0,
  });
  first.resolve(1);
  await results[0];
  await Promise.resolve();
  assert.equal(limiter.snapshot().active, 2);
  assert.equal(limiter.snapshot().queued, 0);
  second.resolve(2);
  third.resolve(3);
  assert.deepEqual(await Promise.all(results), [1, 2, 3]);
});

test('RequestLimiter removes aborted queued jobs and enforces queue bounds', async () => {
  const limiter = new RequestLimiter(1, 1);
  const active = deferred<void>();
  const first = limiter.run(new AbortController().signal, () => active.promise);
  await Promise.resolve();

  const queuedController = new AbortController();
  const queued = limiter.run(queuedController.signal, async () => 2);
  await assert.rejects(
    limiter.run(new AbortController().signal, async () => 3),
    /queue limit exceeded/,
  );
  queuedController.abort(new DOMException('cancelled', 'AbortError'));
  await assert.rejects(queued, /cancelled/);
  assert.equal(limiter.snapshot().queued, 0);
  active.resolve();
  await first;
});
