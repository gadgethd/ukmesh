import assert from 'node:assert/strict';
import test from 'node:test';
import type { LivePacket } from '../types/index.js';
import type { PacketSharePublisher } from './packetShareForwarder.js';
import { createPacketShareForwarder } from './packetShareForwarder.js';
import { generatePacketShareIdentity } from './packetShareAuth.js';
import type { PacketShareRepository } from './packetShareRepository.js';

async function withGuard<T>(promise: Promise<T>, label: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} hung`)), 5_000);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

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
    isOwnerNodeGrantActive: async () => true,
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
  const forwarder = createPacketShareForwarder({
    isOwnerNodeGrantActive: async () => true, repository });
  await forwarder.enqueuePacket({ ...packet, visibilityOk: false }, { raw: '0102' });
  await forwarder.stop();
  assert.equal(targets, 0);
});

test('forwarder rejects a stale queued ScotMesh delivery for another account or observer', async () => {
  let published = 0;
  let failed = 0;
  let claimed = false;
  const repository = {
    reclaimExpiredClaims: async () => 0,
    claimDue: async () => {
      if (claimed) return [];
      claimed = true;
      return [
        { id: '1', owner_username: 'node2', node_id: packet.rxNodeId, destination_id: 'scotmesh', attempts: 1 },
        { id: '2', owner_username: 'node1', node_id: 'C'.repeat(64), destination_id: 'scotmesh', attempts: 1 },
      ];
    },
    getDestination: async () => ({ allowed_owner_username: 'node1', allowed_node_id: packet.rxNodeId }),
    markFailed: async () => { failed += 1; },
  } as unknown as PacketShareRepository;
  const forwarder = createPacketShareForwarder({
    isOwnerNodeGrantActive: async () => true, repository, publisherFactory: () => {
    published += 1;
    throw new Error('unauthorized destination reached publisher');
  } });
  await forwarder.processDue();
  await forwarder.stop();
  assert.equal(failed, 2);
  assert.equal(published, 0);
});

test('ScotMesh queues only the scoped repeater and applies advert TX mode with its relay identity', async () => {
  const encryptionKey = 'test-packet-share-encryption-key';
  const previousKey = process.env['OWNER_PACKET_SHARE_ENCRYPTION_KEY'];
  process.env['OWNER_PACKET_SHARE_ENCRYPTION_KEY'] = encryptionKey;
  try {
    const identity = await generatePacketShareIdentity(encryptionKey);
    const target = {
      destination_id: 'scotmesh', display_name: 'ScotMesh', enabled: true,
      broker_url: 'wss://scotmesh-mqtt.mm7roq.compute.oarc.uk:443/',
      topic_template: 'meshcore/MME/{observerId}/packets',
      broker_username: identity.username, broker_password_ciphertext: identity.privateKeyCiphertext,
      allowed_owner_username: 'node1', allowed_node_id: packet.rxNodeId,
      owner_username: 'node1', node_id: packet.rxNodeId,
      auth_mode: 'meshcore_jwt' as const, auth_audience: 'scotmesh-mqtt.mm7roq.compute.oarc.uk', tx_mode: 'advert' as const,
    };
    const queued: Array<{ destinationTopic: string; payload: Record<string, unknown> }> = [];
    const repository = {
      activeTargets: async () => [target, { ...target, owner_username: 'node2' }],
      enqueueDelivery: async (input: typeof queued[number]) => { queued.push(input); return true; },
      reclaimExpiredClaims: async () => 0,
      claimDue: async () => [],
    } as unknown as PacketShareRepository;
    const forwarder = createPacketShareForwarder({
    isOwnerNodeGrantActive: async () => true, repository });
    await forwarder.enqueuePacket({ ...packet, direction: 'rx' }, { raw: '0102' });
    await forwarder.enqueuePacket({ ...packet, direction: 'tx' }, { raw: '0102' });
    await forwarder.enqueuePacket({ ...packet, direction: 'tx', packetType: 4, srcNodeId: packet.rxNodeId }, { raw: '0102' });
    await forwarder.enqueuePacket({ ...packet, direction: 'tx', packetType: 4, srcNodeId: 'C'.repeat(64) }, { raw: '0102' });
    await forwarder.enqueuePacket({ ...packet, rxNodeId: 'C'.repeat(64) }, { raw: '0102' });
    await forwarder.stop();
    assert.equal(queued.length, 2);
    for (const row of queued) {
      assert.equal(row.destinationTopic, `meshcore/MME/${identity.username.slice(3)}/packets`);
      assert.equal(row.payload['origin_id'], identity.username.slice(3));
    }
    assert.equal(queued[0]?.payload['direction'], 'rx');
    assert.equal(queued[1]?.payload['direction'], 'tx');
  } finally {
    if (previousKey === undefined) delete process.env['OWNER_PACKET_SHARE_ENCRYPTION_KEY'];
    else process.env['OWNER_PACKET_SHARE_ENCRYPTION_KEY'] = previousKey;
  }
});

const wedgeDestination = {
  destination_id: 'remote-site',
  display_name: 'Remote site',
  broker_url: 'mqtts://remote.example.test:8883',
  topic_template: 'replicated/{network}/{observerId}/packets',
  broker_username: null,
  broker_password_ciphertext: null,
  enabled: true,
};

const wedgeRow = {
  id: '1',
  owner_username: 'node1',
  node_id: packet.rxNodeId!,
  destination_id: 'remote-site',
  event_key: 'k',
  packet_hash: packet.packetHash,
  source_topic: packet.topic,
  destination_topic: `replicated/ukmesh/${'A'.repeat(64)}/packets`,
  payload: { raw: '0102' } as Record<string, unknown>,
  attempts: 1,
};

test('forwarder survives a hung publish: times out, fails for retry, then delivers on a fresh publisher', async () => {
  let claims = 0;
  const failed: Array<{ id: string; message: string }> = [];
  const succeeded: string[] = [];
  const repository = {
    reclaimExpiredClaims: async () => 0,
    claimDue: async () => {
      claims += 1;
      return claims <= 2 ? [wedgeRow] : [];
    },
    getDestination: async () => wedgeDestination,
    markSucceeded: async (id: string) => { succeeded.push(id); },
    markFailed: async (id: string, _claimToken: string, _attempts: number, message: string) => {
      failed.push({ id, message });
    },
  } as unknown as PacketShareRepository;
  const published: string[] = [];
  let factories = 0;
  const forwarder = createPacketShareForwarder({
    isOwnerNodeGrantActive: async () => true,
    repository,
    publisherFactory: () => {
      factories += 1;
      if (factories === 1) {
        return {
          // Settles only after the publish timeout has fired: the late
          // rejection must be swallowed, not crash the process.
          publish: () => new Promise<void>((_resolve, reject) => {
            setTimeout(() => reject(new Error('late publish failure')), 50);
          }),
          close: async () => undefined,
        };
      }
      return {
        publish: async (topic: string) => { published.push(topic); },
        close: async () => undefined,
      };
    },
    publishAckTimeoutMs: 20,
    deliverTimeoutMs: 2_000,
  });

  await withGuard(forwarder.processDue(), 'first processDue');
  assert.equal(failed.length, 1, 'hung publish must fail the delivery after the timeout');
  assert.match(failed[0]!.message, /publish timed out/);
  await new Promise((resolve) => setTimeout(resolve, 80));

  await withGuard(forwarder.processDue(), 'second processDue');
  await forwarder.stop();

  assert.equal(factories, 2, 'retry must use a fresh publisher');
  assert.equal(succeeded.length, 1, 'retry should deliver');
  assert.equal(published.length, 1);
  assert.equal(published[0], `replicated/ukmesh/${'A'.repeat(64)}/packets`);
});

test('forwarder aborts a delivery past the per-row deadline even when the publisher never settles', async () => {
  let claims = 0;
  const failed: Array<{ id: string; message: string }> = [];
  const repository = {
    reclaimExpiredClaims: async () => 0,
    claimDue: async () => {
      claims += 1;
      return claims === 1 ? [wedgeRow] : [];
    },
    getDestination: async () => wedgeDestination,
    markSucceeded: async () => undefined,
    markFailed: async (id: string, _claimToken: string, _attempts: number, message: string) => {
      failed.push({ id, message });
    },
  } as unknown as PacketShareRepository;
  const forwarder = createPacketShareForwarder({
    isOwnerNodeGrantActive: async () => true,
    repository,
    publisherFactory: () => ({
      publish: () => new Promise<void>(() => undefined),
      close: async () => undefined,
    }),
    publishAckTimeoutMs: 10_000,
    deliverTimeoutMs: 30,
  });

  await withGuard(forwarder.processDue(), 'processDue');
  await forwarder.stop();

  assert.equal(failed.length, 1, 'the per-row deadline must release the claim');
  assert.match(failed[0]!.message, /delivery timed out/);
});
