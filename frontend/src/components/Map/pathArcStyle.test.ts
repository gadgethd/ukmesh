import assert from 'node:assert/strict';
import test from 'node:test';
import {
  PATH_HOP_ANIMATION_MS,
  PATH_ARC_HEIGHT_M,
  PATH_ARC_SEGMENTS,
  PATH_LINE_TTL_MS,
  PATH_TERRAIN_CLEARANCE_M,
  packetIdentityColor,
  packetPathColors,
} from './pathArcStyle.js';

test('live path styling uses a 30-second TTL', () => {
  assert.equal(PATH_LINE_TTL_MS, 30_000);
  assert.equal(PATH_HOP_ANIMATION_MS, 400);
  assert.equal(PATH_ARC_HEIGHT_M, 120);
  assert.equal(PATH_ARC_SEGMENTS, 32);
  assert.equal(PATH_TERRAIN_CLEARANCE_M, 32);
});

test('packet identity colors are deterministic and distinct for visible packets', () => {
  const hashes = ['ABC123', 'DEF456', '012345', 'AABBCC'];
  const colors = hashes.map(packetIdentityColor);

  assert.deepEqual(packetIdentityColor('ABC123'), packetIdentityColor('ABC123'));
  assert.deepEqual(packetIdentityColor('abc123'), packetIdentityColor('ABC123'));
  assert.equal(new Set(colors.map((color) => color.join(','))).size, hashes.length);

  for (let first = 0; first < colors.length; first += 1) {
    for (let second = first + 1; second < colors.length; second += 1) {
      const distance = Math.hypot(...colors[first]!.map((value, channel) => value - colors[second]![channel]!));
      assert(distance > 70, `${hashes[first]} and ${hashes[second]} are too visually close`);
    }
  }
});

test('packet path colors preserve identity while fading alpha', () => {
  const full = packetPathColors('ABC123');
  const half = packetPathColors('ABC123', 0.5);
  assert.deepEqual(full.coreTarget.slice(0, 3), half.coreTarget.slice(0, 3));
  assert.equal(full.coreTarget[3], 255);
  assert.equal(half.coreTarget[3], 128);
});
