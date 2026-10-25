import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildPersistedPacketPayload,
  stripDerivedPacketFields,
} from './envelopeSafety.js';
import { buildPacketBatchInput } from '../db/packetInput.js';

const OBSERVER = 'A'.repeat(64);
const HASH = 'AB'.repeat(8);

/** A publisher-controlled envelope that claims decoded content of its own. */
function forgedEnvelope(): Record<string, unknown> {
  return {
    packet_type: 5,
    hash: HASH,
    raw: '',
    direction: 'rx',
    origin: 'forged-origin',
    _summary: 'DM: "meet me at the usual place"',
    decrypted: { sender: 'FORGED-SENDER-NODE' },
  };
}

test('an unverified envelope cannot persist _summary or decrypted', () => {
  const envelope = forgedEnvelope();
  const payload = buildPersistedPacketPayload(undefined, envelope, 'rx');

  assert.equal(Object.hasOwn(payload, '_summary'), false);
  assert.equal(Object.hasOwn(payload, 'decrypted'), false);
  // Everything the publisher legitimately supplied is still stored.
  assert.equal(payload['packet_type'], 5);
  assert.equal(payload['hash'], HASH);
  assert.equal(payload['origin'], 'forged-origin');
  assert.equal(payload['direction'], 'rx');
  // The input envelope is not mutated for other consumers of `json`.
  assert.equal(envelope['_summary'], 'DM: "meet me at the usual place"');
  assert.deepEqual(envelope['decrypted'], { sender: 'FORGED-SENDER-NODE' });
});

test('the persisted batch row keeps summary and companion_sender null for a forged envelope', () => {
  const payload = buildPersistedPacketPayload(undefined, forgedEnvelope(), 'rx');
  const row = buildPacketBatchInput({
    packetHash: HASH,
    topic: `meshcore/MME/${OBSERVER}/packets`,
    packetType: 5,
    payload,
    rawHex: '',
  });

  assert.equal(row.companionSender, null, 'decrypted.sender must not be promoted to companion_sender');
  const stored = JSON.parse(row.payloadJson ?? '{}') as Record<string, unknown>;
  assert.equal(Object.hasOwn(stored, '_summary'), false);
  assert.equal(Object.hasOwn(stored, 'decrypted'), false);
});

test('a verified decode owns the derived fields, and its summary wins over a forged one', () => {
  const verified = { appData: { name: 'Real Node' }, timestamp: 1_700_000_000 };
  const payload = buildPersistedPacketPayload(verified, forgedEnvelope(), 'tx');
  assert.deepEqual(payload, { ...verified, direction: 'tx' });

  const row = buildPacketBatchInput({
    packetHash: HASH,
    topic: `meshcore/MME/${OBSERVER}/packets`,
    packetType: 4,
    payload,
    summary: 'advert: Real Node',
    rawHex: '',
  });
  const stored = JSON.parse(row.payloadJson ?? '{}') as Record<string, unknown>;
  assert.equal(stored['_summary'], 'advert: Real Node');
  assert.equal(Object.hasOwn(stored, 'decrypted'), false);
});

test('the live-consumer envelope view is stripped of forged derived fields', () => {
  const sanitized = stripDerivedPacketFields(forgedEnvelope());
  assert.equal(Object.hasOwn(sanitized, '_summary'), false);
  assert.equal(Object.hasOwn(sanitized, 'decrypted'), false);
  assert.equal(sanitized['origin'], 'forged-origin');
  assert.equal(sanitized['direction'], 'rx');
});
