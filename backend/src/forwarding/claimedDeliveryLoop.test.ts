import assert from 'node:assert/strict';
import test from 'node:test';
import { createClaimedDeliveryLoop } from './claimedDeliveryLoop.js';

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(ready => { resolve = ready; });
  return { promise, resolve };
}

test('concurrent polls share one claim and stop joins delivery before refusing further admission', async () => {
  const claimed = deferred<void>(), release = deferred<void>();
  let claims = 0, deliveries = 0, settled = 0;
  const loop = createClaimedDeliveryLoop({
    pollIntervalMs: 100, deliveryTimeoutMs: 1_000,
    async claim(token) { assert.match(token, /^[\da-f-]{36}$/); claims++; return ['owned']; },
    async deliver() { deliveries++; claimed.resolve(); await release.promise; },
    deliveryLabel: () => 'owned delivery', aborted: async () => { assert.fail('unexpected abort'); },
    failed: error => { throw error; }, settled: () => { settled++; },
  });
  const first = loop.processDue(), concurrent = loop.processDue();
  await claimed.promise;
  let stopped = false;
  const stop = loop.stop().then(() => { stopped = true; });
  await Promise.resolve();
  assert.equal(stopped, false);
  assert.equal(claims, 1);
  assert.equal(deliveries, 1);
  release.resolve();
  await Promise.all([first, concurrent, stop]);
  await loop.processDue();
  assert.equal(loop.start(), false);
  assert.equal(claims, 1);
  assert.equal(settled, 1);
});

test('delivery deadline aborts publication and joins claim release before another poll', async () => {
  const aborted = deferred<void>(), release = deferred<void>();
  let signal!: AbortSignal, claims = 0;
  const tokens: string[] = [];
  const loop = createClaimedDeliveryLoop({
    pollIntervalMs: 100, deliveryTimeoutMs: 5,
    async claim(token) { tokens.push(token); return ++claims === 1 ? ['hung'] : []; },
    async deliver(_row, _token, currentSignal) { signal = currentSignal; await new Promise<void>(() => {}); },
    deliveryLabel: () => 'owned delivery',
    async aborted(_row, token, error) {
      assert.equal(token, tokens[0]);
      assert.match(String(error), /owned delivery timed out/);
      assert.equal(signal.aborted, true);
      aborted.resolve(); await release.promise;
    },
    failed: error => { throw error; }, settled() {},
  });
  const first = loop.processDue();
  await aborted.promise;
  const concurrent = loop.processDue();
  assert.equal(claims, 1);
  release.resolve();
  await Promise.all([first, concurrent]);
  await loop.processDue();
  assert.equal(claims, 2);
  assert.notEqual(tokens[0], tokens[1]);
  await loop.stop();
});
