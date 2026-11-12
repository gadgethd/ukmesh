import assert from 'node:assert/strict';
import test from 'node:test';
import { createMessageTimestampPublisher } from './messageTimestamp.js';

test('connection timestamps publish immediately, coalesce bursts and retain the exact trailing reception', () => {
  let time = 0;
  const values: number[] = [];
  const timers = new Map<number, () => void>();
  let next = 0;
  const publisher = createMessageTimestampPublisher(value => values.push(value), {
    intervalMs: 1000, now: () => time,
    setTimer: callback => { const id = ++next; timers.set(id, callback); return id as unknown as ReturnType<typeof setTimeout>; },
    clearTimer: id => { timers.delete(id as unknown as number); },
  });
  publisher.record(1_000_000);
  assert.deepEqual(values, [1_000_000]);
  for (let i = 1; i <= 500; i++) { time = i; publisher.record(1_000_000 + i); }
  assert.equal(timers.size, 1, 'traffic cannot accumulate or postpone publication timers');
  assert.equal(values.length, 1);
  time = 1000;
  [...timers.values()][0]!();
  assert.deepEqual(values, [1_000_000, 1_000_500]);
  assert.equal(timers.size, 0);
  time = 1100; publisher.record(1_000_600);
  publisher.flush();
  assert.equal(values[values.length - 1], 1_000_600, 'socket closure can publish the final exact timestamp');
  publisher.stop();
  publisher.record(1_000_700);
  assert.equal(values.length, 3);
  assert.equal(timers.size, 0);
});

test('scope cleanup cancels the trailing update and ignores a late callback', () => {
  const values: number[] = [];
  let pending = () => {};
  let cleared = 0;
  const publisher = createMessageTimestampPublisher(value => values.push(value), {
    intervalMs: 1000, now: () => 0,
    setTimer: callback => { pending = callback; return 1 as unknown as ReturnType<typeof setTimeout>; },
    clearTimer: () => { cleared++; },
  });
  publisher.record(10); publisher.record(11);
  publisher.stop(); pending();
  assert.deepEqual(values, [10]);
  assert.ok(cleared > 0);
});
