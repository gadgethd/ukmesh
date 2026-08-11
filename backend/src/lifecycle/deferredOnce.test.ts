import assert from 'node:assert/strict';
import test from 'node:test';
import { createDeferredOnce } from './deferredOnce.js';

test('deferred lifecycle work starts exactly once after registration', () => {
  const deferred = createDeferredOnce('chart warmup');
  let starts = 0;

  assert.throws(() => deferred.start(), /not registered/);
  deferred.register(() => { starts += 1; });
  assert.equal(starts, 0);
  assert.equal(deferred.start(), true);
  assert.equal(deferred.start(), false);
  assert.equal(starts, 1);
  assert.throws(() => deferred.register(() => undefined), /already registered/);
});
