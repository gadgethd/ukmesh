import assert from 'node:assert/strict';
import test from 'node:test';
import type { LivePacket } from '../types/index.js';
import {
  buildPublicFeedPacket,
  getPublicFeedConfig,
  isValidPublicFeedBrokerUrl,
  publicFeedPacketIsEligible,
} from './publicFeedConfig.js';

const packet: LivePacket = {
  id: 'public-feed-event',
  packetHash: 'B'.repeat(16),
  rxNodeId: 'A'.repeat(64),
  topic: `meshcore/LHR/${'A'.repeat(64)}/packets`,
  iata: 'LHR',
  network: 'ukmesh',
  packetType: 5,
  visibilityOk: true,
  ts: Date.parse('2026-09-20T12:00:00Z'),
};

test('public feed configuration requires the explicit operator switch and broker endpoint', () => {
  assert.equal(getPublicFeedConfig({}).configured, false);
  assert.equal(getPublicFeedConfig({
    PUBLIC_FEED_PUBLISH_ENABLED: 'true',
    PUBLIC_FEED_BROKER_URL: 'mqtts://broker.example.test:8883',
  }).configured, true);
  assert.equal(getPublicFeedConfig({
    PUBLIC_FEED_PUBLISH_ENABLED: 'true',
    PUBLIC_FEED_BROKER_URL: 'mqtts://user:pass@broker.example.test:8883',
  }).configured, false);
});

test('public feed serializer emits CoreScope topic and numeric JSON fields without a wrapper', () => {
  const outbound = buildPublicFeedPacket(packet, {
    raw: '04719600004AA03688E8F83CEEB7B40DE40D9E2DB29FED705273C5C1FF3DBC375E5916AB5F24649A54AA7BCF3664',
    origin: '49Segre-Room-Obs',
    timestamp: '2026-09-20T12:00:00.000Z',
    SNR: '12.2',
    RSSI: '-30',
    score: '1000',
    direction: 'rx',
    body: 'decoded content must not cross the public-feed boundary',
  });
  assert.equal(outbound.topic, `public/LHR/${'A'.repeat(64)}/packets`);
  assert.deepEqual(outbound.payload, {
    raw: '04719600004AA03688E8F83CEEB7B40DE40D9E2DB29FED705273C5C1FF3DBC375E5916AB5F24649A54AA7BCF3664',
    origin: '49Segre-Room-Obs',
    timestamp: '2026-09-20T12:00:00.000Z',
    SNR: 12.2,
    RSSI: -30,
    score: 1000,
    direction: 'rx',
  });
});

test('public feed serializer rejects bare or malformed raw packets and invalid regions', () => {
  assert.throws(() => buildPublicFeedPacket(packet, { raw: 'not-hex' }));
  assert.throws(() => buildPublicFeedPacket(packet, { raw: 'ABC' }));
  assert.throws(() => buildPublicFeedPacket({ ...packet, iata: 'unknown region' }, { raw: '0102' }));
  assert.equal(publicFeedPacketIsEligible({ ...packet, visibilityOk: false }), false);
  assert.equal(publicFeedPacketIsEligible({ ...packet, iata: undefined }), false);
});

test('public feed broker validation accepts MQTT transports without embedded credentials', () => {
  assert.equal(isValidPublicFeedBrokerUrl('mqtts://broker.example.test:8883'), true);
  assert.equal(isValidPublicFeedBrokerUrl('wss://broker.example.test/mqtt'), true);
  assert.equal(isValidPublicFeedBrokerUrl('mqtts://user:pass@broker.example.test:8883'), false);
  assert.equal(isValidPublicFeedBrokerUrl('https://broker.example.test'), false);
});
