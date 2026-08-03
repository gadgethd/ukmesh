import assert from 'node:assert/strict';
import test from 'node:test';
import { PATH_LINE_TTL_MS, pathArcColors, pathConfidenceBand } from './pathArcStyle.js';

test('path confidence uses low/mid/high traffic-light bands', () => {
  assert.equal(PATH_LINE_TTL_MS, 15_000);
  assert.equal(pathConfidenceBand(null), 'low');
  assert.equal(pathConfidenceBand(0.39), 'low');
  assert.equal(pathConfidenceBand(0.4), 'mid');
  assert.equal(pathConfidenceBand(0.749), 'mid');
  assert.equal(pathConfidenceBand(0.75), 'high');
});

test('path arcs render high green, mid yellow, and low red with fade opacity', () => {
  assert.deepEqual(pathArcColors(1), {
    bloomSource: [34, 197, 94, 35],
    bloomTarget: [34, 197, 94, 70],
    coreSource: [34, 197, 94, 200],
    coreTarget: [34, 197, 94, 255],
  });
  assert.deepEqual(pathArcColors(0.5, 0.5).coreTarget, [250, 204, 21, 128]);
  assert.deepEqual(pathArcColors(0.2).coreTarget, [239, 68, 68, 255]);
});
