import assert from 'node:assert/strict';
import test from 'node:test';
import {
  feedConnectionStatus,
  initialPathTreeStatus,
  validatedRegionSelection,
} from './feedState.js';

test('stored region is preserved until asynchronous options are authoritative', () => {
  assert.equal(validatedRegionSelection('NCL', [], 'loading'), 'NCL');
  assert.equal(validatedRegionSelection('NCL', [], 'failed'), 'NCL');
  assert.equal(validatedRegionSelection('NCL', ['NCL', 'LHR'], 'ready'), 'NCL');
  assert.equal(validatedRegionSelection('NCL', ['LHR'], 'ready'), 'all');
});

test('packets without hashes have a terminal unavailable path state', () => {
  assert.equal(initialPathTreeStatus(false, false), 'unavailable');
  assert.equal(initialPathTreeStatus(true, false), 'idle');
  assert.equal(initialPathTreeStatus(true, true), 'ready');
});

test('connection status uses socket state and cache independently of traffic age', () => {
  const now = 100_000;
  assert.equal(feedConnectionStatus('connected', now - 5_000, false, now), 'live');
  assert.equal(feedConnectionStatus('connected', now - 90_000, true, now), 'connected-quiet');
  assert.equal(feedConnectionStatus('connecting', null, true, now), 'reconnecting');
  assert.equal(feedConnectionStatus('disconnected', null, true, now), 'offline-cached');
  assert.equal(feedConnectionStatus('disconnected', null, false, now), 'offline');
});
