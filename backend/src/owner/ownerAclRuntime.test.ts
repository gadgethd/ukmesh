import assert from 'node:assert/strict';
import { setImmediate } from 'node:timers/promises';
import test from 'node:test';
import { createOwnerAclRuntime } from './ownerAclRuntime.js';

test('owner authorization retries independently and stays unavailable until its first successful reconciliation', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let attempts = 0;
  let forwardingStarts = 0;
  const errors: unknown[] = [];
  const failure = new Error('ACL mount unavailable');
  const runtime = createOwnerAclRuntime({
    initialize: async () => {},
    reconcile: async () => {
      attempts++;
      if (attempts !== 2) throw failure;
    },
    refreshIntervalMs: () => 60_000,
    reportError: error => errors.push(error),
  });
  t.after(runtime.stop);
  runtime.start(() => { forwardingStarts++; });
  runtime.start();
  await setImmediate();
  assert.equal(runtime.isAvailable(), false);
  assert.equal(forwardingStarts, 0);
  assert.equal(attempts, 1);
  assert.deepEqual(errors, [failure]);

  t.mock.timers.tick(10_000);
  await setImmediate();
  assert.equal(runtime.isAvailable(), true);
  assert.equal(forwardingStarts, 1);
  assert.equal(attempts, 2);

  t.mock.timers.tick(60_000);
  await setImmediate();
  assert.equal(attempts, 3);
  assert.equal(forwardingStarts, 1);
  assert.equal(runtime.isAvailable(), true);

  await runtime.stop();
  assert.equal(runtime.isAvailable(), false);
  t.mock.timers.tick(60_000);
  await setImmediate();
  assert.equal(attempts, 3);
});
