import assert from 'node:assert/strict';
import test from 'node:test';
import { buildNodeGeoJSON } from '../components/Map/geojsonBuilders.js';
import {
  NODE_HIDE_AFTER_MS,
  NODE_STALE_AFTER_MS,
} from '../components/Map/mapConfig.js';
import { canonicalNodeId, nodeStore, type MeshNode } from './useNodes.js';

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

test('node store collapses mixed-case live adverts and never regresses to a stale snapshot', () => {
  const staleSnapshot = meshNode(NODE_ID, 20 * 24 * 60 * 60 * 1000, {
    name: 'Old name',
    public_key: NODE_ID,
    advert_count: 4,
  });
  nodeStore.handleInitialState({ nodes: [staleSnapshot], packets: [] });

  const liveSeen = new Date(NOW).toISOString();
  nodeStore.handleNodeUpsert({
    node_id: NODE_ID.toLowerCase(),
    public_key: NODE_ID.toLowerCase(),
    name: 'Current name',
    last_seen: liveSeen,
    advert_count: 5,
    is_online: true,
  });

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
  });
  nodeStore.handleInitialState({ nodes: [staleSnapshot], packets: [] });

  nodes = nodeStore.getState().nodes;
  assert.equal(nodes.size, 2, 'a live node received before the snapshot must be retained');
  assert.equal(nodes.get(NODE_ID)?.last_seen, liveSeen);
  assert.equal(canonicalNodeId(NODE_ID.toLowerCase()), NODE_ID);
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
