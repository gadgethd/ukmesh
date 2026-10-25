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

for (const cancellation of ['timeout', 'hidden', 'stop'] as const) {
  test(`visibility poller reports ${cancellation} correctly and retry does not overlap`, async () => {
    let visible = true;
    let visibilityListener = () => {};
    let calls = 0;
    const errors: unknown[] = [];
    const timers = new Map<number, { callback: () => void; delay: number }>();
    let nextTimer = 1;
    const poller = createVisibilityPoller({
      intervalMs: 1_000, timeoutMs: 5_000, jitterRatio: 0,
      poll: signal => {
        calls += 1;
        if (calls > 1) return Promise.resolve();
        return new Promise<void>((_resolve, reject) => {
          signal.addEventListener('abort', () => reject(signal.reason), { once: true });
        });
      },
      isVisible: () => visible,
      subscribeVisibility: listener => { visibilityListener = listener; return () => {}; },
      onError: error => errors.push(error),
      setTimer: (callback, delay) => {
        const id = nextTimer++;
        timers.set(id, { callback, delay });
        return id as unknown as ReturnType<typeof setTimeout>;
      },
      clearTimer: handle => { timers.delete(handle as unknown as number); },
    });
    if (cancellation === 'timeout') [...timers.values()].find(timer => timer.delay === 5_000)!.callback();
    if (cancellation === 'hidden') { visible = false; visibilityListener(); }
    if (cancellation === 'stop') poller.stop();
    await Promise.resolve();
    await Promise.resolve();
    assert.equal(errors.length, cancellation === 'timeout' ? 1 : 0);
    if (cancellation === 'timeout') {
      assert.equal((errors[0] as Error).name, 'TimeoutError');
      assert.equal(poller.snapshot().failures, 1);
      assert.equal([...timers.values()][0]!.delay, 2_000);
      await poller.trigger();
      assert.equal(calls, 2, 'retry must bypass the scheduled backoff');
      assert.equal(poller.snapshot().failures, 0);
      assert.equal(errors.length, 1, 'abort rejection must not report the deadline twice');
    }
    poller.stop();
  });
}

test('deadline becomes visible even when a request ignores abort until later', async () => {
  const errors: unknown[] = [];
  let deadline = () => {};
  let complete = () => {};
  const poller = createVisibilityPoller({
    intervalMs: 1_000, timeoutMs: 5_000,
    poll: () => new Promise<void>(resolve => { complete = resolve; }),
    isVisible: () => true, subscribeVisibility: () => () => {},
    setTimer: (callback) => { deadline = callback; return 1 as unknown as ReturnType<typeof setTimeout>; },
    clearTimer: () => {}, onError: error => errors.push(error),
  });
  deadline();
  assert.equal(errors.length, 1);
  assert.equal(poller.snapshot().running, true, 'an unresolved request must continue to prevent overlap');
  complete();
  await Promise.resolve();
  poller.stop();
});
