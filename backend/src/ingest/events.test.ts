import assert from 'node:assert/strict';
import test from 'node:test';
import type { LivePacket } from '../types/index.js';
import { createIngestEvents } from './events.js';

test('ingest subscriptions are independent, removable and preserve node coalescing', t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const first = createIngestEvents(), second = createIngestEvents();
  t.after(first.close); t.after(second.close);
  const packets: string[] = [], nodes: unknown[] = [], upserts: unknown[] = [];
  const receive = (packet: LivePacket) => { packets.push(packet.packetHash); };
  const unsubscribe = first.onPacket(receive);
  first.onPacket(receive);
  second.onPacket(packet => { packets.push('second:' + packet.packetHash); });
  first.emitCommittedPacket({ packetHash: 'one' } as LivePacket);
  unsubscribe();
  first.emitCommittedPacket({ packetHash: 'two' } as LivePacket);
  assert.deepEqual(packets, ['one', 'one', 'two']);
  first.onNodeSeen((id, meta) => { nodes.push([id, meta]); });
  first.onNodeUpsert(node => { upserts.push(node); });
  first.emitNode('A', { network: 'ukmesh' });
  first.emitNode('A', { network: 'test' });
  first.emitNodeUpsert({ node_id: 'A', name: 'before' });
  first.emitNodeUpsert({ node_id: 'A', name: 'after' });
  t.mock.timers.tick(499);
  assert.equal(nodes.length, 0);
  t.mock.timers.tick(1);
  assert.deepEqual(nodes, [['A', { network: 'test' }]]);
  assert.deepEqual(upserts, [{ node_id: 'A', name: 'after' }]);
  first.close();
  first.emitNode('closed'); first.emitCommittedPacket({ packetHash: 'closed' } as LivePacket);
  second.emitCommittedPacket({ packetHash: 'three' } as LivePacket);
  t.mock.timers.tick(500);
  assert.deepEqual(packets, ['one', 'one', 'two', 'second:three']);
  assert.equal(nodes.length, 1);
});


test('final node flush cannot re-enter admission or retain new subscriptions', t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const events = createIngestEvents();
  const nodes: string[] = [];
  let lateSubscriptions = 0;
  events.onNodeSeen(id => {
    nodes.push(id);
    events.emitNode('reentered');
    events.emitNodeUpsert({ node_id: 'reentered' });
    events.onNodeSeen(() => { lateSubscriptions++; });
    events.onPacket(() => { lateSubscriptions++; });
    events.onNodeUpsert(() => { lateSubscriptions++; });
  });
  events.emitNode('accepted');
  events.close();
  events.close();
  events.emitCommittedPacket({ packetHash: 'closed' } as LivePacket);
  events.emitNode('closed');
  events.emitNodeUpsert({ node_id: 'closed' });
  t.mock.timers.tick(5_000);
  assert.deepEqual(nodes, ['accepted']);
  assert.equal(lateSubscriptions, 0);
});
