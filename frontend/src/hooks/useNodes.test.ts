import assert from 'node:assert/strict';
import test from 'node:test';
import { buildNodeGeoJSON } from '../components/Map/geojsonBuilders.js';
import {
  NODE_HIDE_AFTER_MS,
  NODE_STALE_AFTER_MS,
} from '../components/Map/mapConfig.js';
import {
  PACKET_ARC_TTL_MS,
  canonicalNodeId,
  nodeStore,
  type MeshNode,
} from './useNodes.js';

const NODE_ID = 'A1'.repeat(32);
const OTHER_NODE_ID = 'B2'.repeat(32);
const NOW = Date.parse('2026-07-25T12:00:00Z');

function meshNode(
  nodeId: string,
  ageMs: number,
  overrides: Partial<MeshNode> = {},
): MeshNode {
  return {
    node_id: nodeId,
    name: nodeId.slice(0, 4),
    lat: 54,
    lon: -1,
    role: 2,
    last_seen: new Date(NOW - ageMs).toISOString(),
    is_online: true,
    ...overrides,
  };
}

test('node store canonicalizes live adverts and treats every snapshot as authoritative', () => {
  const epoch = nodeStore.reset('test-authoritative');
  const staleSnapshot = meshNode(NODE_ID, 20 * 24 * 60 * 60 * 1000, {
    name: 'Old name',
    public_key: NODE_ID,
    advert_count: 4,
  });
  nodeStore.handleInitialState({ nodes: [staleSnapshot], packets: [] }, epoch);

  const liveSeen = new Date(NOW).toISOString();
  nodeStore.handleNodeUpsert({
    node_id: NODE_ID.toLowerCase(),
    public_key: NODE_ID.toLowerCase(),
    name: 'Current name',
    last_seen: liveSeen,
    advert_count: 5,
    is_online: true,
  }, epoch);

  let nodes = nodeStore.getState().nodes;
  assert.equal(nodes.size, 1);
  assert.equal(nodes.get(NODE_ID)?.last_seen, liveSeen);
  assert.equal(nodes.get(NODE_ID)?.name, 'Current name');
  assert.equal(nodes.get(NODE_ID)?.public_key, NODE_ID);
  assert.equal(nodes.get(NODE_ID)?.advert_count, 5);

  nodeStore.handleNodeUpsert({
    node_id: OTHER_NODE_ID.toLowerCase(),
    last_seen: liveSeen,
    is_online: true,
  }, epoch);
  nodeStore.handleInitialState({ nodes: [staleSnapshot], packets: [] }, epoch);

  nodes = nodeStore.getState().nodes;
  assert.equal(nodes.size, 1, 'the snapshot must replace live state once the coordinator replays pending events');
  assert.equal(nodes.get(NODE_ID)?.last_seen, staleSnapshot.last_seen);
  assert.equal(nodes.has(OTHER_NODE_ID), false);
  assert.equal(canonicalNodeId(NODE_ID.toLowerCase()), NODE_ID);

  nodeStore.handleInitialState({ nodes: [], packets: [] }, epoch);
  assert.equal(nodeStore.getState().nodes.size, 0, 'an empty snapshot must clear prior nodes');
});

test('node store ignores late updates from a prior scope epoch', () => {
  const oldEpoch = nodeStore.reset('scope-old');
  const currentEpoch = nodeStore.reset('scope-current');
  nodeStore.handleNodeUpsert(meshNode(NODE_ID, 0), oldEpoch);
  assert.equal(nodeStore.getState().nodes.size, 0);
  nodeStore.handleNodeUpsert(meshNode(OTHER_NODE_ID, 0), currentEpoch);
  assert.equal(nodeStore.getState().nodes.has(OTHER_NODE_ID), true);
});

test('map marks nodes stale after 14 days and hides ordinary nodes after 28 days', () => {
  const nodes = new Map<string, MeshNode>([
    [NODE_ID, meshNode(NODE_ID, NODE_STALE_AFTER_MS - 1, { role: 1 })],
    [OTHER_NODE_ID, meshNode(OTHER_NODE_ID, NODE_STALE_AFTER_MS + 1)],
    ['C3'.repeat(32), meshNode('C3'.repeat(32), NODE_HIDE_AFTER_MS + 1)],
  ]);

  const geojson = buildNodeGeoJSON(
    nodes,
    new Map(),
    true,
    false,
    new Set(),
    new Set(),
    new Set(),
    false,
    null,
    null,
    NOW,
  );

  assert.equal(geojson.features.length, 2);
  const byId = new Map(geojson.features.map((feature) => [
    String(feature.properties?.['node_id']),
    feature.properties,
  ]));
  assert.equal(byId.get(NODE_ID)?.['is_stale'], false);
  assert.equal(byId.get(OTHER_NODE_ID)?.['is_stale'], true);
});

test('inferred fixtures appear, update known-node styling, disappear, and exclude private rows', () => {
  const known = meshNode(NODE_ID, 0);
  const inferred = meshNode('inferred:2:C3D4', 0, {
    name: 'Inferred C3D4',
    is_inferred: true,
  });
  const privateInferred = meshNode('inferred:2:FFFF', 0, {
    name: 'Hidden 🚫',
    is_inferred: true,
  });
  const nodes = new Map([[known.node_id, known]]);

  const populated = buildNodeGeoJSON(
    nodes,
    new Map(),
    true,
    false,
    new Set(),
    new Set(),
    new Set(),
    false,
    null,
    null,
    NOW,
    [inferred, privateInferred],
    new Set([NODE_ID.toLowerCase()]),
  );
  assert.deepEqual(
    populated.features.map((feature) => feature.properties?.['node_id']),
    [NODE_ID, inferred.node_id],
  );
  assert.equal(populated.features[0]?.properties?.['is_inferred'], true);
  assert.equal(populated.features[1]?.properties?.['is_inferred'], true);

  const cleared = buildNodeGeoJSON(
    nodes,
    new Map(),
    true,
    false,
    new Set(),
    new Set(),
    new Set(),
    false,
    null,
    null,
    NOW,
    [],
    new Set(),
  );
  assert.deepEqual(
    cleared.features.map((feature) => feature.properties?.['node_id']),
    [NODE_ID],
  );
  assert.equal(cleared.features[0]?.properties?.['is_inferred'], false);
});

test('one privacy-safe packet creates a bounded arc and expiry removes it', () => {
  const sourceId = 'D4'.repeat(32);
  const receiverId = 'E5'.repeat(32);
  const privateId = 'F6'.repeat(32);
  const now = Date.now();
  const epoch = nodeStore.reset('arc-test');
  nodeStore.setArcCollectionEnabled(false);
  nodeStore.handleInitialState({
    nodes: [
      meshNode(sourceId, 0, { lat: 54, lon: -1 }),
      meshNode(receiverId, 0, { lat: 55, lon: -2 }),
      meshNode(privateId, 0, { lat: 56, lon: -3, name: 'Private 🚫' }),
    ],
    packets: [],
  }, epoch);
  nodeStore.setArcCollectionEnabled(true);
  nodeStore.handlePacket({
    id: 'packet-one',
    packetHash: 'abcd',
    srcNodeId: sourceId.toLowerCase(),
    rxNodeId: receiverId.toLowerCase(),
    topic: 'mesh/packet',
    hopCount: 2,
    ts: now,
  }, epoch);

  assert.deepEqual(nodeStore.getState().arcs, [{
    id: `ABCD:${receiverId}:${now}`,
    from: [-1, 54],
    to: [-2, 55],
    hopCount: 2,
    ts: now,
    packetHash: 'ABCD',
  }]);

  nodeStore.handlePacket({
    id: 'packet-private',
    packetHash: 'dcba',
    srcNodeId: sourceId,
    rxNodeId: privateId,
    topic: 'mesh/packet',
    ts: now,
  }, epoch);
  assert.equal(nodeStore.getState().arcs.length, 1, 'private endpoints must not enter arc state');

  nodeStore.pruneExpiredArcs(now + PACKET_ARC_TTL_MS);
  assert.equal(nodeStore.getState().arcs.length, 0);
  nodeStore.setArcCollectionEnabled(false);
});
