import assert from 'node:assert/strict';
import test from 'node:test';
import {
  createRealtimeMessageCoordinator,
  type RealtimeMessageActions,
} from './useAppMessageHandler.js';
import type { WSMessage } from './useWebSocket.js';

const NODE_ID = 'AB'.repeat(32);

function message(type: WSMessage['type'], data: unknown): WSMessage {
  return { type, data, ts: Date.now() };
}

function runFixture(liveBeforeSnapshot: boolean) {
  const state = {
    nodes: [] as string[],
    packets: [] as string[],
    links: [] as string[],
    observedCount: 0,
  };
  const actions: RealtimeMessageActions = {
    handleInitialState: (data) => {
      state.nodes = data.nodes.map((node) => node.node_id);
      state.packets = data.packets.map((packet) => packet.packet_hash);
    },
    handlePacket: (value) => {
      const packets = Array.isArray(value) ? value : [value];
      state.packets.push(...packets.map((packet) => packet.packetHash));
    },
    handleNodeUpdate: () => {},
    handleNodeUpsert: (node) => { state.nodes.push(node.node_id); },
    applyInitialViablePairs: (pairs = []) => {
      state.links = pairs.map(([a, b]) => `${a}:${b}`);
    },
    applyInitialViableLinks: (links = []) => {
      state.links = links.map((link) => `${link.node_a_id}:${link.node_b_id}`);
    },
    applyLinkUpdate: () => {},
    onPacketObserved: (count) => { state.observedCount += count; },
  };
  const coordinator = createRealtimeMessageCoordinator(() => actions);
  const liveMessages = [
    message('node_upsert', {
      node_id: NODE_ID.toLowerCase(),
      last_seen: '2026-07-29T12:00:00.000Z',
      is_online: true,
    }),
    message('packet', {
      id: 'one',
      packetHash: 'PACKET-ONE',
      topic: 'mesh/packet',
      ts: 1,
    }),
    message('packet', {
      id: 'two',
      packetHash: 'PACKET-TWO',
      topic: 'mesh/packet',
      ts: 2,
    }),
  ];
  const snapshot = message('initial_state', {
    nodes: [],
    packets: [],
    viable_links: [],
    viable_pairs: [['stale-a', 'stale-b']],
  });

  if (liveBeforeSnapshot) {
    liveMessages.forEach(coordinator.handle);
    assert.equal(coordinator.pendingPacketCount(), 2);
    coordinator.handle(snapshot);
  } else {
    coordinator.handle(snapshot);
    liveMessages.forEach(coordinator.handle);
    coordinator.flush();
  }
  return state;
}

test('live-before-snapshot and snapshot-before-live fixtures converge', () => {
  const before = runFixture(true);
  const after = runFixture(false);
  assert.deepEqual(before, after);
  assert.deepEqual(before, {
    nodes: [NODE_ID.toLowerCase()],
    packets: ['PACKET-ONE', 'PACKET-TWO'],
    links: [],
    observedCount: 2,
  });
});

test('coordinator resets pending events at a scope boundary', () => {
  let packets = 0;
  const actions: RealtimeMessageActions = {
    handleInitialState: () => {},
    handlePacket: (value) => { packets += Array.isArray(value) ? value.length : 1; },
    handleNodeUpdate: () => {},
    handleNodeUpsert: () => {},
    applyInitialViablePairs: () => {},
    applyInitialViableLinks: () => {},
    applyLinkUpdate: () => {},
  };
  const coordinator = createRealtimeMessageCoordinator(() => actions);
  coordinator.handle(message('packet', {
    id: 'old',
    packetHash: 'OLD',
    topic: 'mesh/packet',
    ts: 1,
  }));
  coordinator.reset();
  coordinator.handle(message('initial_state', { nodes: [], packets: [], viable_links: [] }));
  assert.equal(packets, 0);
});
