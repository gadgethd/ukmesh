import assert from 'node:assert/strict';
import test from 'node:test';
import { createFrameSnapshotScheduler } from './frameSnapshotScheduler.js';

test('frame snapshot scheduler coalesces ticks and respects its visible-frame budget', () => {
  const callbacks = new Map<number, FrameRequestCallback>();
  let nextId = 0;
  let emitted = 0;
  let visible = true;
  const scheduler = createFrameSnapshotScheduler({
    emit: () => { emitted += 1; },
    minIntervalMs: 66,
    isVisible: () => visible,
    requestFrame: (callback) => {
      const id = ++nextId;
      callbacks.set(id, callback);
      return id;
    },
    cancelFrame: (id) => { callbacks.delete(id); },
  });

  scheduler.noteMutation();
  scheduler.noteMutation();
  assert.equal(callbacks.size, 1);
  callbacks.get(1)?.(0);
  callbacks.delete(1);
  assert.equal(emitted, 1);

  scheduler.noteMutation();
  callbacks.get(2)?.(20);
  callbacks.delete(2);
  assert.equal(emitted, 1);
  assert.equal(callbacks.size, 1);
  callbacks.get(3)?.(70);
  callbacks.delete(3);
  assert.equal(emitted, 2);

  visible = false;
  scheduler.noteMutation();
  assert.equal(callbacks.size, 0);
  scheduler.stop();
});
