import assert from 'node:assert/strict';
import test from 'node:test';
import {
  cleanPacketBody,
  isOwnerLiveResponse,
  isOwnerSessionResponse,
  isValidMapCoord,
  nodeRoleLabel,
} from './ownerPortalModel.js';

test('owner map coordinate policy rejects placeholders and invalid coordinates', () => {
  assert.equal(isValidMapCoord(51.5, -0.1), true);
  assert.equal(isValidMapCoord(0, 0), false);
  assert.equal(isValidMapCoord(91, 1), false);
  assert.equal(isValidMapCoord(Number.NaN, 1), false);
});

test('owner role and packet presentation remain stable', () => {
  assert.equal(nodeRoleLabel(1), 'Companion');
  assert.equal(nodeRoleLabel(3), 'Room Server');
  assert.equal(nodeRoleLabel(null), 'Repeater');
  assert.equal(cleanPacketBody({
    time: '2026-01-01T00:00:00Z',
    packet_type: 4,
    route_type: 0,
    hop_count: 0,
    packet_hash: null,
    src_node_id: null,
    src_node_name: null,
    sender: null,
    body: '4',
  }), null);
});

test('owner response guards reject structurally incomplete payloads', () => {
  assert.equal(isOwnerSessionResponse({ ok: true, dashboard: {} }), false);
  assert.equal(isOwnerLiveResponse({ nodeId: 'aa' }), false);
  assert.equal(isOwnerSessionResponse({
    ok: true,
    dashboard: {
      nodes: [],
      totals: {},
      roadmap: [],
    },
  }), true);
});
