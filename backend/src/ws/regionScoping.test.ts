import assert from 'node:assert/strict';
import test from 'node:test';
import type { WSMessage } from '../types/index.js';
import { packetMatchesScope, shouldSendMessage, trackScopedNodes, type ClientScope } from './server.js';

test('WebSocket region intersects network/observer scope without growing membership from packet endpoints', () => {
  const scope: ClientScope = { network: 'ukmesh', region: 'Europe', nodeIds: new Set(['aa', 'bb']) };
  assert.equal(packetMatchesScope({ network: 'northeast', regionScope: '#Europe' }, scope), true);
  for (const packet of [{ regionScope: '#eng-ne' }, {}, { network: 'test', regionScope: '#Europe' }]) {
    assert.equal(packetMatchesScope(packet, scope), false);
  }
  assert.equal(packetMatchesScope({ network: 'ukmesh' }, { network: 'ukmesh', nodeIds: new Set() }), true);
  assert.equal(packetMatchesScope({ regionScope: '#Europe', rxNodeId: 'cc' }, { ...scope, observer: 'dd' }), false);
  for (const [type, data, expected] of [
    ['node_upsert', { node_id: 'aa' }, true], ['node_upsert', { node_id: 'cc' }, false],
    ['node_update', { nodeId: 'cc' }, false],
    ['link_update', { node_a_id: 'aa', node_b_id: 'bb' }, true],
    ['link_update', { node_a_id: 'aa', node_b_id: 'cc' }, false],
  ] as const) {
    assert.equal(shouldSendMessage({ type, data, ts: 0 } as WSMessage, scope), expected);
  }
  trackScopedNodes({ type: 'packet', data: { rxNodeId: 'cc', srcNodeId: 'dd' }, ts: 0 }, scope);
  assert.deepEqual([...scope.nodeIds], ['aa', 'bb']);
  assert.equal(shouldSendMessage({ type: 'node_upsert', data: { node_id: 'cc', observer_id: 'cc' }, ts: 0 }, { ...scope, observer: 'cc' }), false);
});
