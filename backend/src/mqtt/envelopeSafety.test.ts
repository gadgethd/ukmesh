import assert from 'node:assert/strict';
import test from 'node:test';
import { validPacketIntegers, isPermanentDataError } from './envelopeSafety.js';

test('packet integer poison values are refused; fractional signal measurements remain valid', () => {
  assert.ok(validPacketIntegers({ packetType: 5, hopCount: 0, pathHashSizeBytes: 3 }));
  for (const value of [5.5, -1, NaN, Infinity, 2 ** 40]) {
    assert.equal(validPacketIntegers({ packetType: value }), false);
  }
  assert.ok(isPermanentDataError({ code: '22P02' }));
  for (const code of ['42P01', '08006', '40001', '57014', 'XX000']) assert.equal(isPermanentDataError({ code }), false);
});
