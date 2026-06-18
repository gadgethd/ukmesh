import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  sanitizeUsername,
  sanitizeUsernames,
  sanitizeSample,
  coarsenCoord,
  sanitizeIncident,
} from './sanitize.js';
import { DEFAULT_SPAM_MESSAGE_CONFIG as CFG } from './config.js';
import { estimateOrigin } from './origin.js';
import { normalizeMessage } from './normalize.js';
import type { Incident } from './types.js';

test('usernames are reduced to a non-identifying hint', () => {
  assert.equal(sanitizeUsername('JohnSmith'), 'Jo…');
  assert.equal(sanitizeUsername(''), 'unknown');
  // The hint still conveys that variants are related, without the full handle.
  const hints = sanitizeUsernames(['Spammer', 'Spammer2', 'Spammer_UK', 'Spammer']);
  assert.deepEqual(hints, ['Sp…']);
});

test('sample message strips links, marker, mentions and ids', () => {
  const s = sanitizeSample('Hey @[NUK] buy now https://shop.com/x ref 0011aabbccdd', false);
  assert.ok(!s.includes('shop.com'));
  assert.ok(s.includes('[link]'));
  assert.ok(s.includes('[mention]'));
  assert.ok(s.includes('[id]'));
});

test('sample message flags the spam marker link', () => {
  const s = sanitizeSample('report me at ukmesh.com/spam', true);
  assert.ok(s.includes('[spam-link]'));
  assert.ok(!s.toLowerCase().includes('ukmesh.com/spam'));
});

test('sample message is length-bounded', () => {
  const s = sanitizeSample('x '.repeat(300), false);
  assert.ok(s.length <= 161);
});

test('coordinates are coarsened to the privacy grid', () => {
  // Coarsening lands on the 0.1° grid (compared after rounding away FP noise).
  assert.equal(Number(coarsenCoord(53.8123, 0.1).toFixed(3)), 53.8);
  assert.equal(Number(coarsenCoord(-1.5567, 0.1).toFixed(3)), -1.6);
});

test('sanitizeIncident never leaks raw sender, raw text or exact coords', () => {
  const incident: Incident = {
    key: 'abcdef0123456789',
    network: 'ukmesh',
    members: [],
    firstSeen: Date.parse('2026-06-17T10:00:00Z'),
    lastSeen: Date.parse('2026-06-17T10:20:00Z'),
    messageCount: 5,
    observerCount: 4,
    channels: ['Public'],
    senderNames: ['ScamLord', 'ScamLord2'],
    canonicalText: 'send me your seed phrase at evil.com/x',
    representativeText: 'SEND me your seed phrase at https://evil.com/x !!!',
    hasSpamMarker: false,
    hasUrl: true,
    score: 0.73,
    reasons: ['5 near-duplicate messages'],
  };
  const origin = estimateOrigin(
    [
      { observerId: 'n1', lat: 53.8011, lon: -1.5499, hopCount: 1, snr: 8, rssi: -70 },
      { observerId: 'n2', lat: 53.8203, lon: -1.5301, hopCount: 2, snr: 4, rssi: -85 },
      { observerId: 'n3', lat: 53.7899, lon: -1.5701, hopCount: 1, snr: 6, rssi: -78 },
    ],
    CFG,
  );
  const pub = sanitizeIncident(incident, origin, 'active', CFG);
  const blob = JSON.stringify(pub);

  assert.ok(!blob.includes('ScamLord'), 'raw sender leaked');
  assert.ok(!blob.includes('evil.com'), 'raw url leaked');
  assert.ok(!blob.includes('seed phrase') || pub.sampleMessage.includes('seed phrase'), 'only sanitized sample exposes text');
  assert.ok(!blob.includes('53.8011') && !blob.includes('-1.5499'), 'exact observer coord leaked');
  assert.ok(!blob.includes('n1') && !blob.includes('n2'), 'observer id leaked');

  // It should still carry the useful, safe fields.
  assert.equal(pub.id, 'abcdef0123456789');
  assert.equal(pub.status, 'active');
  assert.equal(pub.messageCount, 5);
  assert.deepEqual(pub.similarUsernames, ['Sc…']);
  assert.ok(pub.origin.zone, 'a coarse zone should be present');
  // Coarsened to 3dp on a 0.1 grid.
  assert.equal(pub.origin.zone!.lat, Number(pub.origin.zone!.lat.toFixed(3)));
});

test('insufficient origin produces no map zone', () => {
  const incident: Incident = {
    key: '0000000000000000',
    network: 'ukmesh',
    members: [],
    firstSeen: Date.now(),
    lastSeen: Date.now(),
    messageCount: 3,
    observerCount: 1,
    channels: [],
    senderNames: ['a'],
    canonicalText: 'hi',
    representativeText: 'hi',
    hasSpamMarker: false,
    hasUrl: false,
    score: 0.5,
    reasons: [],
  };
  const origin = estimateOrigin([{ observerId: 'x', lat: 53.8, lon: -1.5, hopCount: 1 }], CFG);
  const pub = sanitizeIncident(incident, origin, 'closed', CFG);
  assert.equal(pub.origin.zone, null);
  assert.equal(pub.origin.level, 'insufficient');
});
