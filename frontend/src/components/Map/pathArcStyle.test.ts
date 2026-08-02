import assert from 'node:assert/strict';
import test from 'node:test';
import { PATH_LINE_TTL_MS, pathArcColors, pathConfidenceBand } from './pathArcStyle.js';

test('path confidence uses the established low/high/confirmed bands', () => {
  assert.equal(PATH_LINE_TTL_MS, 60_000);
  assert.equal(pathConfidenceBand(null), 'low');
  assert.equal(pathConfidenceBand(0.39), 'low');
  assert.equal(pathConfidenceBand(0.4), 'high');
  assert.equal(pathConfidenceBand(0.749), 'high');
  assert.equal(pathConfidenceBand(0.75), 'confirmed');
});

test('confirmed arcs retain the main map ArcLayer colours and respect fade opacity', () => {
  assert.deepEqual(pathArcColors(1), {
    bloomSource: [0, 196, 255, 35],
    bloomTarget: [0, 196, 255, 70],
    coreSource: [120, 220, 255, 200],
    coreTarget: [200, 245, 255, 255],
  });
  assert.deepEqual(pathArcColors(0.5, 0.5).coreTarget, [251, 191, 36, 128]);
  assert.deepEqual(pathArcColors(0.2).coreTarget, [239, 68, 68, 255]);
});
