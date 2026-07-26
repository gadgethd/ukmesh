import assert from 'node:assert/strict';
import test from 'node:test';
import { BoundedSegmentCounter } from './boundedSegmentCounter.js';

test('bounded segment counter never exceeds its configured capacity', () => {
  const counter = new BoundedSegmentCounter(32);
  for (let index = 0; index < 10_000; index += 1) {
    counter.observe(`unique-${index}`);
  }
  assert.equal(counter.size(), 32);
  assert.ok(counter.replacementCount() > 0);
});

test('bounded segment counter retains heavy hitters with conservative counts', () => {
  const counter = new BoundedSegmentCounter(64);
  for (let round = 0; round < 100; round += 1) {
    counter.observe('common-a');
    counter.observe('common-b');
    if (round % 2 === 0) counter.observe('common-a');
    for (let noise = 0; noise < 20; noise += 1) {
      counter.observe(`noise-${round}-${noise}`);
    }
  }

  const candidates = new Map(counter.candidates(30).map((entry) => [entry.key, entry.count]));
  assert.ok((candidates.get('common-a') ?? 0) >= 30);
  assert.ok((candidates.get('common-b') ?? 0) >= 30);
  assert.ok((candidates.get('common-a') ?? 0) <= 150);
  assert.ok((candidates.get('common-b') ?? 0) <= 100);
});
