import assert from 'node:assert/strict';
import test from 'node:test';
import {
  shouldDiscardUnverifiedTxAdvert,
  statusEnvelopeTargetsObserver,
} from './identityBinding.js';

const OBSERVER = 'A'.repeat(64);

test('status identity accepts a missing or matching origin and rejects malformed or mismatched origins', () => {
  assert.equal(statusEnvelopeTargetsObserver(OBSERVER, {}), true);
  assert.equal(statusEnvelopeTargetsObserver(OBSERVER, { origin_id: '' }), true);
  assert.equal(statusEnvelopeTargetsObserver(OBSERVER, { origin_id: OBSERVER.toLowerCase() }), true);
  assert.equal(statusEnvelopeTargetsObserver(OBSERVER, { origin_id: 'not-a-node-id' }), false);
  assert.equal(statusEnvelopeTargetsObserver(OBSERVER, { origin_id: 'B'.repeat(64) }), false);
});

test('only decoded TX adverts may establish advert source identity', () => {
  assert.equal(shouldDiscardUnverifiedTxAdvert({
    direction: 'tx', packetType: 4, decodedAdvertPayload: false,
  }), true);
  assert.equal(shouldDiscardUnverifiedTxAdvert({
    direction: 'tx', packetType: 4, decodedAdvertPayload: true,
  }), false);
  assert.equal(shouldDiscardUnverifiedTxAdvert({
    direction: 'rx', packetType: 4, decodedAdvertPayload: false,
  }), false);
});
