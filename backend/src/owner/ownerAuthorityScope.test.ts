import assert from 'node:assert/strict';
import test from 'node:test';
import type { LivePacket } from '../types/index.js';
import { createPacketShareForwarder } from './packetShareForwarder.js';
import {
  createPacketShareRepository,
  type PacketShareDeliveryRow,
  type PacketShareQueryFn,
  type PacketShareRepository,
} from './packetShareRepository.js';
import { createOwnerRepository } from './ownerRepository.js';

const OWNER = 'node1';
const NODE = 'A'.repeat(64);
const DESTINATION = 'remote-site';

const packet: LivePacket = {
  id: 'packet-event',
  packetHash: 'B'.repeat(16),
  rxNodeId: NODE,
  topic: `meshcore/LHR/${NODE}/packets`,
  iata: 'LHR',
  network: 'ukmesh',
  packetType: 5,
  visibilityOk: true,
  ts: Date.parse('2026-09-16T12:00:00Z'),
};

const targetRow = {
  owner_username: OWNER,
  node_id: NODE,
  destination_id: DESTINATION,
  display_name: 'Remote site',
  website_url: null,
  description: null,
  broker_url: 'mqtts://remote.example.test:8883',
  topic_template: 'replicated/{network}/{observerId}/packets',
  broker_username: null,
  broker_password_ciphertext: null,
  enabled: true,
  allowed_owner_username: null,
  allowed_node_id: null,
  auth_mode: 'password' as const,
  auth_audience: null,
  tx_mode: 'on' as const,
};

const deliveryRow: PacketShareDeliveryRow = {
  id: '1',
  owner_username: OWNER,
  node_id: NODE,
  destination_id: DESTINATION,
  event_key: 'event-key',
  packet_hash: packet.packetHash,
  source_topic: packet.topic,
  destination_topic: `replicated/ukmesh/${NODE}/packets`,
  payload: { raw: '0102' },
  attempts: 1,
};

const enqueueInput = {
  ownerUsername: OWNER,
  nodeId: NODE,
  destinationId: DESTINATION,
  eventKey: 'event-key',
  packet,
  destinationTopic: `replicated/ukmesh/${NODE}/packets`,
  payload: { raw: '0102' },
};

test('an inactive owner grant blocks repository enqueue and dead-letters pending rows', async () => {
  const calls: Array<{ text: string; params?: unknown[] }> = [];
  const query = (async (text: string, params?: unknown[]) => {
    calls.push({ text, params });
    if (text.includes('FROM owner_packet_share_rules')) return { rows: [targetRow] };
    return { rows: [] };
  }) as unknown as PacketShareQueryFn;
  let grantChecks = 0;
  const repository = createPacketShareRepository(query, undefined, {
    isOwnerNodeGrantActive: async (ownerUsername, nodeId) => {
      grantChecks += 1;
      assert.equal(ownerUsername, OWNER);
      assert.equal(nodeId, NODE);
      return false;
    },
  });

  // activeTargets is the enqueue selector: the persisted rule row must not
  // survive a revoked grant, and queued work for the pair must be terminal.
  assert.deepEqual(await repository.activeTargets(NODE), []);
  assert.equal(grantChecks, 1);
  const deadLetters = calls.filter((call) => call.text.includes("status = 'dead_lettered'"));
  assert.equal(deadLetters.length, 1);
  assert.deepEqual(deadLetters[0]?.params?.slice(1, 3), [NODE, 'owner node grant is not active']);

  assert.equal(await repository.enqueueDelivery(enqueueInput), false);
  assert.equal(grantChecks, 2);
  assert.equal(
    calls.some((call) => call.text.includes('INSERT INTO owner_packet_share_deliveries')),
    false,
    'no delivery row may be inserted without a live grant',
  );
  assert.equal(calls.filter((call) => call.text.includes("status = 'dead_lettered'")).length, 2);
});

test('the forwarder also refuses to enqueue for an inactive owner grant', async () => {
  const enqueued: unknown[] = [];
  const deadLettered: string[] = [];
  const repository = {
    activeTargets: async () => [targetRow],
    enqueueDelivery: async (input: unknown) => { enqueued.push(input); return true; },
    reclaimExpiredClaims: async () => 0,
    deadLetterOwnerNodeDeliveries: async (ownerUsername: string, nodeId: string) => {
      deadLettered.push(`${ownerUsername}:${nodeId}`);
      return 1;
    },
  } as unknown as PacketShareRepository;
  const forwarder = createPacketShareForwarder({
    repository,
    isOwnerNodeGrantActive: async () => false,
  });

  await forwarder.enqueuePacket(packet, { raw: '0102' });
  await forwarder.stop();

  assert.deepEqual(enqueued, []);
  assert.deepEqual(deadLettered, [`${OWNER}:${NODE}`]);
});

test('an inactive owner grant blocks delivery and dead-letters the claimed row', async () => {
  let published = 0;
  let claimed = false;
  const deadLettered: string[] = [];
  const failures: string[] = [];
  const repository = {
    reclaimExpiredClaims: async () => 0,
    claimDue: async () => {
      if (claimed) return [];
      claimed = true;
      return [deliveryRow];
    },
    getDestination: async () => {
      throw new Error('destination must not be read after revocation');
    },
    markSucceeded: async () => undefined,
    markFailed: async () => { failures.push('markFailed'); },
    deadLetterOwnerNodeDeliveries: async (ownerUsername: string, nodeId: string) => {
      deadLettered.push(`${ownerUsername}:${nodeId}`);
      return 1;
    },
  } as unknown as PacketShareRepository;
  const forwarder = createPacketShareForwarder({
    repository,
    isOwnerNodeGrantActive: async () => false,
    publisherFactory: () => {
      published += 1;
      throw new Error('revoked delivery reached publisher');
    },
  });

  await forwarder.processDue();
  await forwarder.stop();

  assert.equal(published, 0);
  assert.deepEqual(deadLettered, [`${OWNER}:${NODE}`]);
  assert.deepEqual(failures, []);
});

test('owner live reads stay on the granted node id and never expand canonical aliases', async () => {
  const queries: Array<{ text: string; params?: unknown[] }> = [];
  const query = (async (text: string, params?: unknown[]) => {
    queries.push({ text, params });
    if (text.includes('ARRAY[$1::text] AS members')) {
      return {
        rows: [{
          node_id: NODE,
          name: 'Owner node',
          network: 'ukmesh',
          iata: 'LHR',
          advert_count: 1,
          last_seen: null,
          lat: null,
          lon: null,
          role: 2,
          members: [NODE],
        }],
      };
    }
    return { rows: [] };
  }) as unknown as Parameters<typeof createOwnerRepository>[0]['query'];
  const repository = createOwnerRepository({ query });

  const data = await repository.fetchOwnerLiveData(NODE.toLowerCase());

  assert.equal(queries.length, 10);
  for (const call of queries) {
    assert.doesNotMatch(call.text, /node_identity_aliases/);
    assert.doesNotMatch(call.text, /meshcore_canonical_node_id/);
    assert.deepEqual(call.params, [NODE], 'every selector binds the granted id verbatim');
  }
  assert.deepEqual(data.identityScope, {
    grantedNodeId: NODE,
    aliasExpansion: 'unverified',
    includedAliasNodeIds: [],
  });
  assert.deepEqual(data.ownerNodeResult.rows[0]?.members, [NODE]);
});

test('an owner grant lookup failure suppresses forwarding without dead-lettering', async () => {
  const calls: Array<{ text: string; params?: unknown[] }> = [];
  const query = (async (text: string, params?: unknown[]) => {
    calls.push({ text, params });
    if (text.includes('FROM owner_packet_share_rules')) return { rows: [targetRow] };
    return { rows: [] };
  }) as unknown as PacketShareQueryFn;
  const repository = createPacketShareRepository(query, undefined, {
    isOwnerNodeGrantActive: async () => { throw new Error('owner auth unavailable'); },
  });

  assert.deepEqual(await repository.activeTargets(NODE), []);
  assert.equal(calls.some((call) => call.text.includes("status = 'dead_lettered'")), false);
});

test('an active owner grant still permits enqueue', async () => {
  const calls: Array<{ text: string; params?: unknown[] }> = [];
  const query = (async (text: string, params?: unknown[]) => {
    calls.push({ text, params });
    if (text.includes('FROM owner_packet_share_rules')) return { rows: [targetRow] };
    if (text.includes('INSERT INTO owner_packet_share_deliveries')) return { rows: [{ id: '1' }] };
    return { rows: [] };
  }) as unknown as PacketShareQueryFn;
  const repository = createPacketShareRepository(query, undefined, {
    isOwnerNodeGrantActive: async () => true,
  });

  assert.equal((await repository.activeTargets(NODE)).length, 1);
  assert.equal(await repository.enqueueDelivery(enqueueInput), true);
  assert.equal(calls.some((call) => call.text.includes('INSERT INTO owner_packet_share_deliveries')), true);
  assert.equal(calls.some((call) => call.text.includes("status = 'dead_lettered'")), false);
});
