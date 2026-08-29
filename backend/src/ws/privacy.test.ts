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
  let changes = 0;
  const privacy = new PublicWsPrivacyIndex(() => { changes += 1; });
  assert.equal(privacy.replace([{ node_id: privateId, name: 'Home 🚫' }]), true);
  assert.equal(privacy.replace([{ node_id: privateId, name: 'Home 🚫' }]), false);
  assert.equal(privacy.currentRevision, 1);
  assert.equal(changes, 1);

  assert.equal(privacy.packetHasPrivateParticipant({
    srcNodeId: privateId,
    visibilityOk: false,
  }), true);
  assert.equal(privacy.packetHasPrivateParticipant({
    path: [privateId.slice(0, 4)],
    pathHashSizeBytes: 2,
    visibilityOk: false,
  }), true);
  assert.equal(privacy.packetHasPrivateParticipant({
    path: ['cdef'],
    pathHashSizeBytes: 2,
    visibilityOk: true,
  }), false);
});

test('live opt-outs advance the privacy revision exactly once', () => {
  let changes = 0;
  const privacy = new PublicWsPrivacyIndex(() => { changes += 1; });
  privacy.replace([]);
  const readyRevision = privacy.currentRevision;

  assert.equal(privacy.remember(privateId), true);
  assert.equal(privacy.remember(privateId.toUpperCase()), false);
  assert.equal(privacy.currentRevision, readyRevision + 1);
  assert.equal(changes, 2);
});

test('authoritative refreshes invalidate when a private identity becomes public', () => {
  let changes = 0;
  const privacy = new PublicWsPrivacyIndex(() => { changes += 1; });
  privacy.replace([{ node_id: privateId, name: 'Private 🚫' }]);
  const privateRevision = privacy.currentRevision;

  assert.equal(privacy.replace([{ node_id: privateId, name: 'Public again' }]), true);
  assert.equal(privacy.hasNode(privateId), false);
  assert.equal(privacy.currentRevision, privateRevision + 1);
  assert.equal(changes, 2);
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
