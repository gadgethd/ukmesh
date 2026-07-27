import assert from 'node:assert/strict';
import test from 'node:test';
import { normalizeMessage } from './normalize.js';
import { selectIncidentEvidenceMembers } from './repository.js';
import type { MessageRecord } from './types.js';

function member(index: number, text = `message-${index}`): MessageRecord {
  return {
    id: `hash-${index}`,
    network: 'ukmesh',
    sender: 'sender',
    text,
    norm: normalizeMessage(text),
    channelHash: '80',
    channelLabel: 'Public',
    observedAt: index,
    observers: [],
  };
}

test('bounded incident evidence retains first, representative, and newest observations', () => {
  const members = Array.from({ length: 20 }, (_, index) =>
    member(index, index === 7 ? 'representative' : `message-${index}`));
  const selected = selectIncidentEvidenceMembers(members, 8, 'representative');
  assert.equal(selected.length, 8);
  assert.ok(selected.some((item) => item.id === 'hash-0'));
  assert.ok(selected.some((item) => item.id === 'hash-7'));
  assert.ok(selected.some((item) => item.id === 'hash-19'));
  assert.deepEqual(selected.map((item) => item.observedAt), [...selected].map((item) => item.observedAt).sort((a, b) => a - b));
});
