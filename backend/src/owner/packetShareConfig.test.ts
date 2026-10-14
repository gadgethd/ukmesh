import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildPacketSharePayload,
  destinationConfigured,
  destinationAcceptsPacket,
  destinationAllowedForOwner,
  isValidPacketShareWebsiteUrl,
  isForwardablePacket,
  renderPacketShareTopic,
  renderDestinationTopic,
} from './packetShareConfig.js';
import { decryptPacketShareSecret, encryptPacketShareSecret } from './packetShareCrypto.js';
import type { LivePacket } from '../types/index.js';

const packet: LivePacket = {
  id: 'event-id',
  packetHash: 'B'.repeat(16),
  rxNodeId: 'A'.repeat(64),
  topic: `meshcore/LHR/${'A'.repeat(64)}/packets`,
  iata: 'LHR',
  network: 'ukmesh',
  packetType: 5,
  visibilityOk: true,
  ts: Date.parse('2026-09-16T12:00:00Z'),
};

test('packet share topic rendering is bounded and supports only known values', () => {
  assert.equal(
    renderPacketShareTopic('replicated/{network}/{iata}/{observerId}/packets', packet),
    `replicated/ukmesh/LHR/${'A'.repeat(64)}/packets`,
  );
  assert.throws(() => renderPacketShareTopic('replicated/+/packets', packet));
  assert.throws(() => renderPacketShareTopic('replicated//packets', packet));
});

test('forward payload matches the flat mctomqtt packet shape and excludes decoded content', () => {
  const payload = buildPacketSharePayload(packet, {
    hash: 'upstream-hash',
    origin: 'Test observer',
    type: 'PACKET',
    direction: 'rx',
    len: '2',
    packet_type: '5',
    route: 'F',
    payload_len: '1',
    raw: '0102',
    origin_id: packet.rxNodeId,
    SNR: '12.2',
    RSSI: '-30',
    score: '1000',
    timestamp: '2026-09-16T12:00:00.000Z',
    time: '12:00:00',
    date: '16/9/2026',
    body: 'should not be copied',
    secret: 'should not be copied',
  });
  assert.deepEqual(payload, {
    hash: 'upstream-hash',
    origin: 'Test observer',
    type: 'PACKET',
    direction: 'rx',
    len: '2',
    packet_type: '5',
    route: 'F',
    payload_len: '1',
    raw: '0102',
    origin_id: packet.rxNodeId,
    SNR: '12.2',
    RSSI: '-30',
    score: '1000',
    timestamp: '2026-09-16T12:00:00.000Z',
    time: '12:00:00',
    date: '16/09/2026',
  });
  assert.equal('body' in payload, false);
  assert.equal('secret' in payload, false);
});

test('only public validated packets are forwardable', () => {
  assert.equal(isForwardablePacket(packet), true);
  assert.equal(isForwardablePacket({ ...packet, visibilityOk: false }), false);
  assert.equal(isForwardablePacket({ ...packet, network: 'test' }), false);
  assert.equal(isForwardablePacket({ ...packet, rxNodeId: 'short' }), false);
});

test('destination configuration requires complete broker details', () => {
  const base = {
    enabled: true,
    broker_url: 'mqtts://remote.example.test:8883',
    topic_template: 'replicated/{network}/{observerId}/packets',
    broker_username: null,
    broker_password_ciphertext: null,
  };
  assert.equal(destinationConfigured(base), true);
  assert.equal(destinationConfigured({ ...base, broker_url: null }), false);
  assert.equal(destinationConfigured({ ...base, broker_username: 'user' }), false);
  assert.equal(destinationConfigured({ ...base, broker_password_ciphertext: 'v1.invalid' }, false), false);
  assert.equal(destinationConfigured({ ...base, broker_username: 'user', broker_password_ciphertext: 'v1.invalid' }), false);
  assert.equal(destinationConfigured({ ...base, broker_url: 'https://remote.example.test' }), false);
  assert.equal(destinationConfigured({ ...base, broker_url: 'mqtts://user:pass@remote.example.test:8883' }), false);
  assert.equal(destinationConfigured({ ...base, topic_template: 'replicated/{unknown}/packets' }), false);
});

test('destination metadata accepts only safe HTTPS website links', () => {
  assert.equal(isValidPacketShareWebsiteUrl('https://remote.example.test'), true);
  assert.equal(isValidPacketShareWebsiteUrl('http://remote.example.test'), false);
  assert.equal(isValidPacketShareWebsiteUrl('https://user:pass@remote.example.test'), false);
  assert.equal(isValidPacketShareWebsiteUrl(null), false);
});

test('packet share secrets round-trip with authenticated encryption', () => {
  const encrypted = encryptPacketShareSecret('broker-password', 'test-encryption-key');
  assert.equal(decryptPacketShareSecret(encrypted, 'test-encryption-key'), 'broker-password');
  assert.throws(() => decryptPacketShareSecret(encrypted, 'wrong-encryption-key'));
});

test('account and observer restrictions require exact matches', () => {
  const scope = { allowed_owner_username: 'node1', allowed_node_id: packet.rxNodeId };
  assert.equal(destinationAllowedForOwner(scope, 'node1', packet.rxNodeId!), true);
  assert.equal(destinationAllowedForOwner(scope, 'node2', packet.rxNodeId!), false);
  assert.equal(destinationAllowedForOwner(scope, 'NODE1', packet.rxNodeId!), false);
  assert.equal(destinationAllowedForOwner(scope, 'node1', 'C'.repeat(64)), false);
});

test('advert TX mode preserves RX but rejects other transmissions and relayed adverts', () => {
  const destination = { tx_mode: 'advert' as const };
  assert.equal(destinationAcceptsPacket(destination, { ...packet, direction: 'rx' }, {}), true);
  assert.equal(destinationAcceptsPacket(destination, { ...packet, direction: 'tx' }, {}), false);
  assert.equal(destinationAcceptsPacket(destination, { ...packet, packetType: 4, direction: 'tx', srcNodeId: packet.rxNodeId }, {}), true);
  assert.equal(destinationAcceptsPacket(destination, { ...packet, packetType: 4, direction: 'tx', srcNodeId: 'C'.repeat(64) }, {}), false);
  assert.equal(destinationAcceptsPacket(destination, packet, { direction: 'tx' }), false);
  assert.equal(destinationAcceptsPacket(destination, { ...packet, direction: 'unknown' }, {}), false);
});

test('ScotMesh uses MME and the relay identity in the MQTT topic and origin_id', () => {
  const relayPublicKey = 'D'.repeat(64);
  const destination = {
    destination_id: 'scotmesh', display_name: 'ScotMesh', enabled: true,
    broker_url: 'wss://scotmesh-mqtt.mm7roq.compute.oarc.uk:443/',
    topic_template: 'meshcore/MME/{observerId}/packets',
    broker_username: `v1_${relayPublicKey}`, broker_password_ciphertext: null,
    auth_mode: 'meshcore_jwt' as const,
  };
  assert.equal(renderDestinationTopic(destination, packet), `meshcore/MME/${relayPublicKey}/packets`);
  const payload = buildPacketSharePayload(packet, { raw: '0102', origin: 'Gadget-RPT-TBeam', origin_id: packet.rxNodeId }, destination);
  assert.equal(payload.origin_id, relayPublicKey);
  assert.equal(payload.origin, 'Gadget-RPT-TBeam');
  assert.equal(payload.raw, '0102');
});
