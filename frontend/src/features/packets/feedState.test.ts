import assert from 'node:assert/strict';
import test from 'node:test';
import {
  feedConnectionStatus,
  initialPathTreeStatus,
  parseFeedRegions,
  validatedRegionSelection,
} from './feedState.js';

test('stored region is preserved until asynchronous options are authoritative', () => {
  assert.equal(validatedRegionSelection('NCL', [], 'loading'), 'NCL');
  assert.equal(validatedRegionSelection('NCL', [], 'failed'), 'NCL');
  assert.equal(validatedRegionSelection('NCL', ['NCL', 'LHR'], 'ready'), 'NCL');
  assert.equal(validatedRegionSelection('NCL', ['LHR'], 'ready'), 'all');
});

test('feed region options require the current network, canonical observer and visibility generation', () => {
  const scope = { network: 'test', observer: 'ab'.repeat(32), privacyGeneration: 4 };
  const payload = { network: scope.network, observer: scope.observer.toUpperCase(),
    visibilityGeneration: 4, regions: ['LHR', 'NCL'] };
  assert.deepEqual(parseFeedRegions(payload, scope), ['LHR', 'NCL']);
  assert.equal(parseFeedRegions(payload, { ...scope, privacyGeneration: 5 }), null);
  assert.equal(parseFeedRegions({ ...payload, network: 'ukmesh' }, scope), null);
  assert.equal(parseFeedRegions({ ...payload, observer: null }, scope), null);
  assert.equal(parseFeedRegions({ ...payload, regions: ['not a code'] }, scope), null);
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
