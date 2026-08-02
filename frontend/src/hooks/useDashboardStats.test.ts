import assert from 'node:assert/strict';
import test from 'node:test';
import { packetObservedCount } from './useDashboardStats.js';

test('packet observed events carry the exact batch size with safe fallback', () => {
  assert.equal(packetObservedCount(new Event('meshcore:packet-observed')), 1);
  const batch = new Event('meshcore:packet-observed');
  Object.defineProperty(batch, 'detail', { value: { count: 7 } });
  assert.equal(packetObservedCount(batch), 7);
  const invalid = new Event('meshcore:packet-observed');
  Object.defineProperty(invalid, 'detail', { value: { count: -1 } });
  assert.equal(packetObservedCount(invalid), 1);
});
