import assert from 'node:assert/strict';
import test from 'node:test';
import { MeshCoreDecoder } from '@michaelhart/meshcore-decoder';
import { beaconGroupTextIdentity, decodePacketCompat, verifyAdvertSignature } from './decodePacket.js';

const keyStore = MeshCoreDecoder.createKeyStore({ channelSecrets: [] });
const VALID_ADVERT = '11007E7662676F7F0850A8A355BAAFBFC1EB7B4174C340442D7D7161C9474A2C94006CE7CF682E58408DD8FCC51906ECA98EBF94A037886BDADE7ECD09FD92B839491DF3809C9454F5286D1D3370AC31A34593D569E9A042A3B41FD331DFFB7E18599CE1E60992A076D50238C5B8F85757375354522F50756765744D65736820436F75676172';

test('Beacon wire evidence keeps packet identity and individual relay framing separate', () => {
  const direct = beaconGroupTextIdentity('1500010203');
  assert.deepEqual(direct, {
    ukmeshPacketUid: 'B0BAC56D00069E3AF42FEF4D85C7416E2D9945EC32BF269C481A94AA0BED0118',
    beaconPacketHash: '9cde9054b5b1b34d',
    observationWireSha256: 'd768bace47bab63d7dd9bed058004537eaa07b2528f5de1060717c0b4e407638',
  });
  const relayed = beaconGroupTextIdentity('144455667741aabb010203');
  assert.equal(relayed?.ukmeshPacketUid, direct?.ukmeshPacketUid);
  assert.equal(relayed?.beaconPacketHash, direct?.beaconPacketHash);
  assert.notEqual(relayed?.observationWireSha256, direct?.observationWireSha256);

  // Beacon's short digest deliberately does not carry the UKMesh wire version.
  const otherVersion = beaconGroupTextIdentity('5500010203');
  assert.equal(otherVersion?.beaconPacketHash, direct?.beaconPacketHash);
  assert.notEqual(otherVersion?.ukmeshPacketUid, direct?.ukmeshPacketUid);
});

test('Beacon wire evidence requires framed GroupText', () => {
  for (const frame of ['0d00deadbeef', '1500', '15c001', '1542AABB', 'Z5', '1']) {
    assert.equal(beaconGroupTextIdentity(frame), undefined);
  }
});

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

test('only a valid Ed25519 advert may establish node identity', async () => {
  const valid = decodePacketCompat(VALID_ADVERT, keyStore);
  assert.equal(await verifyAdvertSignature(valid.decoded), true);

  const tampered = decodePacketCompat(`${VALID_ADVERT.slice(0, -2)}00`, keyStore);
  assert.equal(await verifyAdvertSignature(tampered.decoded), false);
});
