import assert from 'node:assert/strict';
import test from 'node:test';
import type { LivePacket } from '../types/index.js';
import { createPublicFeedForwarder, type PublicFeedPublisher } from './publicFeedForwarder.js';
import type { PublicFeedRepository } from './publicFeedRepository.js';

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

test('hung publish and hung cleanup release the worker for subsequent polls', async () => {
  const previous = { ...process.env };
  Object.assign(process.env, configuredEnv());
  let claims = 0;
  let failed = 0;
  const repository = {
    discardInactiveDeliveries: async () => 0,
    reclaimExpiredClaims: async () => 0,
    hasActiveNode: async () => true,
    claimDue: async () => ++claims === 1 ? [{ id: 'hung', node_id: packet.rxNodeId, attempts: 1,
      destination_topic: `public/LHR/${packet.rxNodeId}/packets`, payload: { raw: '0102' } }] : [],
    markFailed: async () => { failed++; },
    queueStats: async () => ({ count: 1, oldestAgeSeconds: 600 }),
  } as unknown as PublicFeedRepository;
  const worker = createPublicFeedForwarder({ repository, getOwnerAuthorizationSnapshot: async () => [],
    publishTimeoutMs: 10, closeTimeoutMs: 10, deliveryTimeoutMs: 200,
    publisherFactory: () => ({ publish: () => new Promise(() => {}), close: () => new Promise(() => {}) }),
  });
  try {
    await worker.processDue();
    assert.equal(failed, 1);
    await worker.processDue();
    assert.equal(claims, 2);
    await worker.stop();
  } finally {
    for (const key of Object.keys(process.env)) if (!(key in previous)) delete process.env[key];
    Object.assign(process.env, previous);
  }
});

function configuredEnv(): Record<string, string> {
  return {
    PUBLIC_FEED_PUBLISH_ENABLED: 'true',
    PUBLIC_FEED_BROKER_URL: 'mqtts://broker.example.test:8883',
    PUBLIC_FEED_BROKER_USERNAME: 'publisher',
    PUBLIC_FEED_BROKER_PASSWORD: 'secret',
  };
}

test('public feed queues one aggregate CoreScope packet and publishes it through the MQTT seam', async () => {
  const previous = { ...process.env };
  Object.assign(process.env, configuredEnv());
  const enqueued: Array<Record<string, unknown>> = [];
  const published: Array<{ topic: string; payload: string }> = [];
  let claimed = false;
  const repository = {
    hasActiveNode: async () => true,
    enqueueDelivery: async (input: Record<string, unknown>) => {
      enqueued.push(input);
      return true;
    },
    syncOwnership: async () => undefined,
    accountStatus: async () => ({ enabled: true, nodeCount: 1, lastForwardedAt: null }),
    setEnabled: async () => undefined,
    discardInactiveDeliveries: async () => 0,
    reclaimExpiredClaims: async () => 0,
    claimDue: async () => {
      if (claimed || enqueued.length < 1) return [];
      claimed = true;
      const row = enqueued[0]!;
      return [{
        id: '1',
        node_id: packet.rxNodeId!,
        event_key: String(row['eventKey']),
        packet_hash: packet.packetHash,
        source_topic: packet.topic,
        destination_topic: String(row['destinationTopic']),
        payload: row['payload'] as Record<string, unknown>,
        attempts: 1,
      }];
    },
    markSucceeded: async () => undefined,
    markFailed: async () => undefined,
    markDiscarded: async () => undefined,
  } as unknown as PublicFeedRepository;
  const publisher: PublicFeedPublisher = {
    publish: async (topic, payload) => { published.push({ topic, payload }); },
    close: async () => undefined,
  };
  const forwarder = createPublicFeedForwarder({
    repository,
    getOwnerAuthorizationSnapshot: async () => [],
    publisherFactory: () => publisher,
  });

  try {
    await forwarder.enqueuePacket(packet, {
      raw: '0102',
      origin: 'Test observer',
      SNR: '12.2',
      RSSI: '-30',
      score: '1000',
      direction: 'rx',
      body: 'private decoded text',
    });
    await forwarder.processDue();
    await forwarder.stop();
  } finally {
    for (const key of Object.keys(process.env)) {
      if (!(key in previous)) delete process.env[key];
    }
    Object.assign(process.env, previous);
  }

  assert.equal(enqueued.length, 1);
  assert.equal(published.length, 1);
  assert.equal(published[0]?.topic, `public/LHR/${'A'.repeat(64)}/packets`);
  const outbound = JSON.parse(published[0]!.payload) as Record<string, unknown>;
  assert.equal(outbound['raw'], '0102');
  assert.equal(outbound['SNR'], 12.2);
  assert.equal(outbound['RSSI'], -30);
  assert.equal(outbound['score'], 1000);
  assert.equal('body' in outbound, false);
  assert.equal('relay' in outbound, false);
  assert.equal('packet' in outbound, false);
});

test('public feed does not enqueue a packet while the broker is not operator-enabled', async () => {
  const previous = { ...process.env };
  delete process.env.PUBLIC_FEED_PUBLISH_ENABLED;
  delete process.env.PUBLIC_FEED_BROKER_URL;
  delete process.env.PUBLIC_FEED_BROKER_USERNAME;
  delete process.env.PUBLIC_FEED_BROKER_PASSWORD;
  let enqueues = 0;
  const repository = {
    hasActiveNode: async () => { throw new Error('must not check ownership when unconfigured'); },
    enqueueDelivery: async () => { enqueues++; return true; },
  } as unknown as PublicFeedRepository;
  const forwarder = createPublicFeedForwarder({
    repository,
    getOwnerAuthorizationSnapshot: async () => [],
  });
  try {
    await forwarder.enqueuePacket(packet, { raw: '0102' });
    await forwarder.stop();
  } finally {
    for (const key of Object.keys(process.env)) {
      if (!(key in previous)) delete process.env[key];
    }
    Object.assign(process.env, previous);
  }
  assert.equal(enqueues, 0);
});
