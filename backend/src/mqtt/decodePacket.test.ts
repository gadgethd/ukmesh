import assert from 'node:assert/strict';
import test from 'node:test';
import { MeshCoreDecoder } from '@michaelhart/meshcore-decoder';
import { decodePacketCompat } from './decodePacket.js';

const keyStore = MeshCoreDecoder.createKeyStore({ channelSecrets: [] });

test('preserves native two-byte path decoding', () => {
  // Header: ACK/Flood; path length 0x42 = two 2-byte hashes.
  const result = decodePacketCompat('0D42AABBCCDDDEADBEEF', keyStore);

  assert.equal(result.metadataValid, true);
  assert.equal(result.pathHashSize, 2);
  assert.equal(result.pathHashCount, 2);
  assert.deepEqual(result.pathHashes, ['AABB', 'CCDD']);
  assert.deepEqual(result.decoded?.path, ['AABB', 'CCDD']);
  assert.equal(result.decoded?.pathLength, 2);
});

test('preserves native three-byte path decoding', () => {
  // Header: ACK/Flood; path length 0x82 = two 3-byte hashes.
  const result = decodePacketCompat('0D82010203A1A2A3DEADBEEF', keyStore);

  assert.equal(result.metadataValid, true);
  assert.equal(result.pathHashSize, 3);
  assert.equal(result.pathHashCount, 2);
  assert.deepEqual(result.pathHashes, ['010203', 'A1A2A3']);
  assert.deepEqual(result.decoded?.path, ['010203', 'A1A2A3']);
  assert.equal(result.decoded?.pathLength, 2);
});

test('canonical identity ignores route framing and relay path', () => {
  // Both carry ACK version 0 and DEADBEEF payload. Their route type, transport
  // codes, and relay paths differ, so they should still represent one packet.
  const flood = decodePacketCompat('0D02AABBDEADBEEF', keyStore);
  const transportFlood = decodePacketCompat('0C1122334403102030DEADBEEF', keyStore);
  const differentPayload = decodePacketCompat('0D02AABBDEADBEEE', keyStore);

  assert.equal(flood.metadataValid, true);
  assert.equal(transportFlood.metadataValid, true);
  assert.match(flood.canonicalPacketId ?? '', /^[A-F0-9]{64}$/);
  assert.equal(flood.canonicalPacketId, transportFlood.canonicalPacketId);
  assert.notEqual(flood.canonicalPacketId, differentPayload.canonicalPacketId);
});

test('does not expose metadata or an identity for malformed framing', () => {
  // 0x82 declares two 3-byte hashes, but only two path bytes are present.
  const truncated = decodePacketCompat('0D82AABB', keyStore);
  const oddLength = decodePacketCompat('0', keyStore);

  assert.equal(truncated.metadataValid, false);
  assert.equal(truncated.decoded, undefined);
  assert.equal(truncated.pathHashes, undefined);
  assert.equal(truncated.canonicalPacketId, undefined);
  assert.equal(oddLength.metadataValid, false);
  assert.equal(oddLength.canonicalPacketId, undefined);
});
