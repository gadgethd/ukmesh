import assert from 'node:assert/strict';
import test from 'node:test';
import { nextNodePresenceTransition, nodeMapNeedsSourceRefresh, buildNodeGeoJSON } from './geojsonBuilders.js';
import { NODE_HIDE_AFTER_MS, NODE_STALE_AFTER_MS } from './mapConfig.js';
import type { MeshNode } from '../../types/domain.js';
import { PROHIBITED_NODE_MARKER } from '../../utils/pathing.js';

const now = Date.parse('2026-11-12T00:00:00Z');
const node: MeshNode = { node_id: 'A'.repeat(64), name: 'Invented relay', lat: 54, lon: -2, role: 2,
  last_seen: new Date(now - 1000).toISOString(), is_online: true };
const map = (value: MeshNode) => new Map([[value.node_id, value]]);

test('ordinary presence updates retain sources and immutable store snapshots', () => {
  const before = map(node);
  const updated = { ...node, last_seen: new Date(now).toISOString() };
  const after = map(updated);
  assert.equal(nodeMapNeedsSourceRefresh(before, after, now), false);
  assert.equal(before.get(node.node_id)?.last_seen, node.last_seen);
  assert.equal(nodeMapNeedsSourceRefresh(before, new Map(), now), true);
  for (const patch of [{ lat: 55 }, { name: 'Renamed relay' }, { is_online: false }, { role: 1 }, { public_key: node.node_id }]) {
    assert.equal(nodeMapNeedsSourceRefresh(before, map({ ...updated, ...patch }), now), true);
  }
  const native = buildNodeGeoJSON(after, new Map(), true, new Set(), new Set(), false, null, null, now);
  assert.equal('last_seen' in native.features[0]!.properties!, false, 'popups read exact timestamps from the store');
  assert.equal(nodeMapNeedsSourceRefresh(after, before, now), true, 'an older authoritative snapshot schedules an earlier cutoff');
});

test('private mask movement and revived stale/hidden nodes always invalidate all sources', () => {
  for (const age of [NODE_STALE_AFTER_MS + 1, NODE_HIDE_AFTER_MS + 1]) {
    assert.equal(nodeMapNeedsSourceRefresh(map({ ...node, last_seen: new Date(now - age).toISOString() }), map(node), now), true);
  }
  const privateName = { ...node, name: `Private ${PROHIBITED_NODE_MARKER}` };
  assert.equal(nodeMapNeedsSourceRefresh(map(privateName), map({ ...privateName, last_seen: new Date(now).toISOString() }), now), true);
});

test('one boundary deadline covers stale and hidden transitions, including future clocks', () => {
  assert.equal(nextNodePresenceTransition(map(node), now), now - 1000 + NODE_STALE_AFTER_MS + 1);
  const old = { ...node, last_seen: new Date(now - NODE_STALE_AFTER_MS - 1).toISOString() };
  assert.equal(nextNodePresenceTransition(map(old), now), now - NODE_STALE_AFTER_MS - 1 + NODE_HIDE_AFTER_MS + 1);
  assert.equal(nextNodePresenceTransition(map({ ...node, last_seen: new Date(now - NODE_HIDE_AFTER_MS - 1).toISOString() }), now), null);
  assert.equal(nextNodePresenceTransition(new Map(), now), null);
  assert.equal(nextNodePresenceTransition(map({ ...node, last_seen: 'invalid' }), now), null);
  assert.equal(nextNodePresenceTransition(map({ ...node, last_seen: new Date(now + 60000).toISOString() }), now), now + 60000 + NODE_STALE_AFTER_MS + 1);
});
