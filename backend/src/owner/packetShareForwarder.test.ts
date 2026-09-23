import assert from 'node:assert/strict';
import test from 'node:test';
import type { LivePacket } from '../types/index.js';
import type { PacketSharePublisher } from './packetShareForwarder.js';
import { createPacketShareForwarder } from './packetShareForwarder.js';
import type { PacketShareRepository } from './packetShareRepository.js';

const packet: LivePacket = {
  id: 'packet-event',
  packetHash: 'B'.repeat(16),
  rxNodeId: 'A'.repeat(64),
  topic: `meshcore/LHR/${'A'.repeat(64)}/packets`,
  iata: 'LHR',
  network: 'ukmesh',
  packetType: 5,
  visibilityOk: true,
  ts: Date.parse('2026-09-16T12:00:00Z'),
};

test('forwarder queues one selected owner packet and publishes it through the MQTT seam', async () => {
  const destination = {
    destination_id: 'remote-site',
    display_name: 'Remote site',
    broker_url: 'mqtts://remote.example.test:8883',
    topic_template: 'replicated/{network}/{observerId}/packets',
    broker_username: null,
    broker_password_ciphertext: null,
    enabled: true,
  };
  const enqueued: Array<Record<string, unknown>> = [];
  const published: Array<{ topic: string; payload: string }> = [];
  let claimed = false;
  const repository = {
    activeTargets: async () => [{ ...destination, owner_username: 'node1', node_id: packet.rxNodeId! }],
    enqueueDelivery: async (input: Record<string, unknown>) => {
      enqueued.push(input);
      return true;
    },
    reclaimExpiredClaims: async () => 0,
    claimDue: async (_limit: number, _claimToken: string) => {
      if (claimed || enqueued.length < 1) return [];
      claimed = true;
      const row = enqueued[0]!;
      return [{
        id: '1',
        owner_username: 'node1',
        node_id: packet.rxNodeId!,
        destination_id: 'remote-site',
        event_key: String(row['eventKey']),
        packet_hash: packet.packetHash,
        source_topic: packet.topic,
        destination_topic: String(row['destinationTopic']),
        payload: row['payload'] as Record<string, unknown>,
        attempts: 1,
      }];
    },
    getDestination: async () => destination,
    markSucceeded: async () => undefined,
    markFailed: async () => undefined,
  } as unknown as PacketShareRepository;
  const publisher: PacketSharePublisher = {
    publish: async (topic, payload) => { published.push({ topic, payload }); },
    close: async () => undefined,
  };
  const forwarder = createPacketShareForwarder({
    repository,
    publisherFactory: () => publisher,
  });

  await forwarder.enqueuePacket(packet, { raw: '0102', packet_type: 5, body: 'private decoded text' });
  await forwarder.processDue();
  await forwarder.stop();

  assert.equal(enqueued.length, 1);
  assert.equal(published.length, 1);
  assert.equal(published[0]?.topic, `replicated/ukmesh/${'A'.repeat(64)}/packets`);
  const outbound = JSON.parse(published[0]!.payload) as Record<string, unknown>;
  assert.equal(enqueued[0]?.['eventKey'], `node1:remote-site:${packet.rxNodeId}:${packet.packetHash}`);
  assert.equal(outbound['raw'], '0102');
  assert.equal(outbound['packet_type'], '5');
  assert.equal(outbound['timestamp'], '2026-09-16T12:00:00.000Z');
  assert.equal(outbound['time'], '12:00:00');
  assert.equal(outbound['date'], '16/09/2026');
  assert.equal('relay' in outbound, false);
  assert.equal('packet' in outbound, false);
  assert.equal('version' in outbound, false);
  assert.equal('body' in outbound, false);
});

test('forwarder does not queue a privacy-rejected packet', async () => {
  let targets = 0;
  const repository = {
    activeTargets: async () => { targets += 1; return []; },
  } as unknown as PacketShareRepository;
  const forwarder = createPacketShareForwarder({ repository });
  await forwarder.enqueuePacket({ ...packet, visibilityOk: false }, { raw: '0102' });
  await forwarder.stop();
  assert.equal(targets, 0);
});
