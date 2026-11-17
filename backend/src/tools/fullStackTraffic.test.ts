import assert from 'node:assert/strict';
import test from 'node:test';
import { MeshCoreDecoder } from '@michaelhart/meshcore-decoder';
import { buildFullStackTraffic, encryptedTestMessage, signedAdvert } from './fullStackTraffic.js';
import { PUBLIC_CHANNELS } from '../mqtt/channelRegistry.js';
import { decodePacketCompat, verifyAdvertSignature } from '../mqtt/decodePacket.js';

const keyStore = MeshCoreDecoder.createKeyStore({ channelSecrets: PUBLIC_CHANNELS.map(channel => channel.secret) });

test('invented adverts establish identity only after native signature verification', async () => {
  const advert = signedAdvert('native-roundtrip', 0, 1794614400);
  const packet = decodePacketCompat(advert.raw, keyStore);
  assert.equal(packet.metadataValid, true);
  assert.equal(await verifyAdvertSignature(packet.decoded), true);
  assert.equal((packet.decoded?.payload.decoded as any).publicKey, advert.nodeId);
  const corrupted = decodePacketCompat(advert.raw.slice(0, -2) + '00', keyStore);
  assert.equal(await verifyAdvertSignature(corrupted.decoded), false);
});

test('public test traffic decrypts through the production decoder with its actual sender and text', () => {
  const packet = decodePacketCompat(encryptedTestMessage('DEV qualification proof', 1794614400), keyStore);
  assert.equal(packet.metadataValid, true);
  const decoded = packet.decoded?.payload.decoded as any;
  assert.equal(decoded.decrypted.sender, 'Qualification');
  assert.equal(decoded.decrypted.message, 'DEV qualification proof');
  assert.equal(decoded.decrypted.timestamp, 1794614400);
});

test('native count expectations distinguish packets, observations and same-observer redelivery', () => {
  const traffic = buildFullStackTraffic('count-proof', '2026-11-14T00:00:00Z', 2, 1000);
  assert.equal(traffic.events.length, 12);
  assert.equal(new Set(traffic.ukmeshHashes).size, 6);
  assert.equal(new Set(traffic.radioHashes).size, 6);
  const observations = new Set(traffic.events.filter(event => event.envelope.raw).map(event =>
    event.topic + ':' + event.envelope.raw));
  assert.equal(observations.size, 8);
  assert.equal(traffic.expected.ukmesh.packets, observations.size);
  assert.equal(traffic.expected.ukmesh.paths, 0);
  assert.equal(traffic.expected.corescope.transmissions, 6);
  assert.equal(traffic.expected.beacon.observations, 8);
  assert.equal(traffic.expected.beacon.messages, 2);
  assert.equal(traffic.expected.ukmesh.statuses, 2);
  assert.equal(traffic.expected.corescope.statuses, 2);
  assert.equal(traffic.expected.beacon.statuses, 2);
  assert.equal(new Set(traffic.statusObserverIds).size,2,'each cycle owns one native telemetry bucket');
  assert.deepEqual(traffic.events.filter(event=>event.topic.endsWith('/status')).map(event=>event.envelope.origin_id),traffic.statusObserverIds);
  const other = buildFullStackTraffic('another-run', '2026-11-14T00:00:00Z', 2, 1000);
  assert.notEqual(traffic.statusObserverId, other.statusObserverId, 'status samples must not overlap between independent runs');
});
