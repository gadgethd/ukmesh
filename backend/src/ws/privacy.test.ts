import assert from 'node:assert/strict';
import test from 'node:test';
import type { WSMessage } from '../types/index.js';
import { PublicWsPrivacyIndex } from './privacy.js';

const privateId = 'ab'.repeat(32);

test('live packets fail closed until the privacy index is ready', () => {
  const privacy = new PublicWsPrivacyIndex();
  const message = { type: 'packet', data: { packetHash: 'hash' }, ts: 1 } as WSMessage;
  assert.equal(privacy.filterMessage(message), null);
});

test('private identities and relay prefixes are suppressed', () => {
  const privacy = new PublicWsPrivacyIndex();
  privacy.replace([{ node_id: privateId, name: 'Home 🚫' }]);

  assert.equal(privacy.packetHasPrivateParticipant({ srcNodeId: privateId }), true);
  assert.equal(privacy.packetHasPrivateParticipant({
    path: [privateId.slice(0, 4)],
    pathHashSizeBytes: 2,
  }), true);
  assert.equal(privacy.packetHasPrivateParticipant({
    path: ['cdef'],
    pathHashSizeBytes: 2,
  }), false);
});

test('private node and link events are suppressed and live opt-outs update the index', () => {
  const privacy = new PublicWsPrivacyIndex();
  privacy.replace([]);
  const upsert = {
    type: 'node_upsert',
    data: { node_id: privateId, name: 'New private 🚫' },
    ts: 1,
  } as WSMessage;
  assert.equal(privacy.filterMessage(upsert), null);

  const link = {
    type: 'link_update',
    data: { node_a_id: privateId, node_b_id: 'cd'.repeat(32) },
    ts: 1,
  } as WSMessage;
  assert.equal(privacy.filterMessage(link), null);
});

test('a first-seen private advert is suppressed before its node upsert arrives', () => {
  const privacy = new PublicWsPrivacyIndex();
  privacy.replace([]);

  assert.equal(privacy.packetHasPrivateParticipant({
    packetType: 4,
    srcNodeId: privateId,
    payload: {
      appData: {
        name: 'Fresh private node 🚫',
        latitude: 54.1,
        longitude: -1.2,
      },
    },
  }), true);

  // The advert also seeds subsequent suppression synchronously.
  assert.equal(privacy.hasNode(privateId), true);
});
