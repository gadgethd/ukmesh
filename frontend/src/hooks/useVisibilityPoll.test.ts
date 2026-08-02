import assert from 'node:assert/strict';
import test from 'node:test';
import { createVisibilityPoller } from './useVisibilityPoll.js';

test('visibility poller prevents overlap, aborts when hidden, and refreshes once on return', async () => {
  let visible = true;
  let visibilityListener = () => {};
  let active = 0;
  let maxActive = 0;
  let calls = 0;
  const resolvers: Array<() => void> = [];
  const timers = new Map<number, () => void>();
  let nextTimer = 1;
  const poller = createVisibilityPoller({
    intervalMs: 1_000,
    timeoutMs: 5_000,
    jitterRatio: 0,
    poll: (signal) => new Promise<void>((resolve, reject) => {
      calls += 1;
      active += 1;
      maxActive = Math.max(maxActive, active);
      const finish = () => {
        active -= 1;
        resolve();
      };
      resolvers.push(finish);
      signal.addEventListener('abort', () => {
        active -= 1;
        reject(signal.reason);
      }, { once: true });
    }),
    isVisible: () => visible,
    subscribeVisibility: (listener) => {
      visibilityListener = listener;
      return () => {};
    },
    setTimer: (callback) => {
      const id = nextTimer;
      nextTimer += 1;
      timers.set(id, callback);
      return id as unknown as ReturnType<typeof setTimeout>;
    },
    clearTimer: (handle) => {
      timers.delete(handle as unknown as number);
    },
  });

  await Promise.resolve();
  assert.equal(calls, 1);
  void poller.trigger();
  assert.equal(calls, 1, 'a trigger during an active request must not overlap');
  resolvers.shift()?.();
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(calls, 2, 'one queued immediate refresh must follow the first request');

  visible = false;
  visibilityListener();
  await Promise.resolve();
  assert.equal(poller.snapshot().scheduled, false);
  const callsWhileHidden = calls;
  for (const callback of timers.values()) callback();
  await Promise.resolve();
  assert.equal(calls, callsWhileHidden);

  visible = true;
  visibilityListener();
  await Promise.resolve();
  assert.equal(calls, callsWhileHidden + 1);
  assert.equal(maxActive, 1);
  poller.stop();
});
