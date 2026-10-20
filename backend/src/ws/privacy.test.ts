import assert from 'node:assert/strict';
import test from 'node:test';
import type { WSMessage } from '../types/index.js';
import { authorizePublicWsMessage, PublicWsPrivacyIndex } from './privacy.js';
import { PrivatePrefixCache } from '../privacy/privatePrefixCache.js';

const privateId = 'ab'.repeat(32);

test('F01 broadcast authority checks observers, sources, all path widths and network families', () => {
  const policy = new PrivatePrefixCache();
  policy.replace(7, [1, 2, 3].map((size) => ({ node_id: privateId, network: 'teesside',
    prefix_size_bytes: size, prefix: privateId.slice(0, size * 2) })));
  const message = (data: unknown) => ({ type: 'packet', data, ts: 1 } as WSMessage);
  for (const network of ['ukmesh', 'northeast', 'teesside']) {
    assert.equal(authorizePublicWsMessage(message({ network, rxNodeId: privateId, visibilityOk: true }), policy), null);
    assert.equal(authorizePublicWsMessage(message({ network, srcNodeId: privateId, visibilityOk: true }), policy), null);
    for (const size of [1, 2, 3]) assert.equal(authorizePublicWsMessage(message({ network,
      path: [privateId.slice(0, size * 2)], pathHashSizeBytes: size, visibilityOk: true }), policy), null);
  }
  assert.ok(authorizePublicWsMessage(message({ network: 'othernet', rxNodeId: privateId, visibilityOk: false }), policy));
  assert.equal(authorizePublicWsMessage(message({ rxNodeId: privateId, visibilityOk: true }), policy), null);
  assert.equal(authorizePublicWsMessage(message({ network: 'ukmesh', path: ['GG'], pathHashSizeBytes: 1 }), policy), null);
});

test('live packets fail closed until the privacy index is ready', () => {
  const privacy = new PublicWsPrivacyIndex();
  const message = { type: 'packet', data: { packetHash: 'hash' }, ts: 1 } as WSMessage;
  assert.equal(privacy.filterMessage(message), null);
});

test('private identities and relay prefixes are suppressed', () => {
  const privacy = new PublicWsPrivacyIndex();
  privacy.replace([{ node_id: privateId, name: 'Home 🚫' }]);

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

test('F01 known private observer or source suppresses a stale visibility flag', () => {
  const privacy = new PublicWsPrivacyIndex();
  privacy.replace([{ node_id: privateId, name: 'Private 🚫' }]);
  assert.equal(privacy.packetHasPrivateParticipant({ rxNodeId: privateId, visibilityOk: true }), true);
  assert.equal(privacy.packetHasPrivateParticipant({ srcNodeId: privateId, visibilityOk: true }), true);
  assert.equal(privacy.packetHasPrivateParticipant({
    path: [privateId.slice(0, 4)], pathHashSizeBytes: 2, visibilityOk: true,
  }), true);
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
