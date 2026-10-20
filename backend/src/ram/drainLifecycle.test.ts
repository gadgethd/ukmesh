import assert from 'node:assert/strict';
import test from 'node:test';
import { joinAndDrainRamWriter } from './drainLifecycle.js';

test('F15 shutdown joins the active persist before attempting to drain', async () => {
  let releasePersist!: () => void;
  const activePersist = new Promise<void>((resolve) => { releasePersist = resolve; });
  let drainCalled = false;
  let stopped = false;
  const close = joinAndDrainRamWriter({
    requestStop: () => { stopped = true; },
    drain: async () => { drainCalled = true; },
  }, activePersist, Date.now() + 5_000);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(stopped, true);
  assert.equal(drainCalled, false, 'the active database write still owns the writer');
  releasePersist();
  await close;
  assert.equal(drainCalled, true);
});

test('F15 an active write past the deadline reports an incomplete shutdown', async () => {
  let releasePersist!: () => void;
  const activePersist = new Promise<void>((resolve) => { releasePersist = resolve; });
  let drainCalled = false;
  await assert.rejects(joinAndDrainRamWriter({
    requestStop: () => undefined,
    drain: async () => { drainCalled = true; },
  }, activePersist, Date.now() + 15), /RAM_DRAIN_TIMEOUT:writer-loop/);
  assert.equal(drainCalled, false);
  releasePersist();
});
