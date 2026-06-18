import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildIncidents } from './analyzer.js';
import { normalizeMessage } from './normalize.js';
import { DEFAULT_SPAM_MESSAGE_CONFIG as CFG } from './config.js';
import type { MessageRecord, ObserverObservation } from './types.js';

const MIN = 60 * 1000;

function obs(id: string, lat: number, lon: number, hopCount: number): ObserverObservation {
  return { observerId: id, lat, lon, hopCount, rssi: -80, snr: 6 };
}

let seq = 0;
function rec(text: string, sender: string, observedAt: number, observers: ObserverObservation[]): MessageRecord {
  return {
    id: `hash-${seq++}`,
    network: 'ukmesh',
    sender,
    text,
    norm: normalizeMessage(text),
    channelHash: '80',
    channelLabel: 'Public',
    observedAt,
    observers,
  };
}

// The live analyzer runs every few minutes over a rolling window; an event that
// is still arriving (last message within the ongoing window) must surface as a
// live "active" incident — this is the whole point for new events.
test('a fresh, still-arriving flood is reported as an active incident', () => {
  const now = Date.now();
  const text = 'flash sale ends tonight visit deal.example.com hurry';
  const observers = [obs('near', 53.48, -2.24, 1), obs('mid', 52.9, -1.2, 7), obs('far', 54.9, -1.6, 12)];
  const records: MessageRecord[] = [];
  for (let i = 0; i < 12; i++) {
    // 2 minutes apart, the last one landing right now.
    records.push(rec(text, 'PromoBot', now - (11 - i) * 2 * MIN, observers));
  }

  const items = buildIncidents(records, now, CFG);
  assert.equal(items.length, 1, 'the fresh flood should form one incident');
  assert.equal(items[0]!.status, 'active', 'a still-arriving flood must be active');
  assert.equal(items[0]!.publicJson.status, 'active');
  assert.ok(
    items[0]!.publicJson.confidence >= CFG.publicMinScore,
    `should clear the public floor, got ${items[0]!.publicJson.confidence}`,
  );
  // It was heard close by, so we should get a usable (non-insufficient) origin.
  assert.notEqual(items[0]!.origin.level, 'insufficient');
});

test('the same flood, gone quiet for over the cooldown, is reported closed', () => {
  const now = Date.now();
  const text = 'flash sale ends tonight visit deal.example.com hurry';
  const observers = [obs('near', 53.48, -2.24, 1), obs('mid', 52.9, -1.2, 7)];
  const records: MessageRecord[] = [];
  // Whole burst sits well before `now` (longer ago than the ongoing window).
  const base = now - 5 * 60 * MIN;
  for (let i = 0; i < 12; i++) records.push(rec(text, 'PromoBot', base + i * 2 * MIN, observers));

  const items = buildIncidents(records, now, CFG);
  assert.equal(items.length, 1);
  assert.equal(items[0]!.status, 'closed');
});
