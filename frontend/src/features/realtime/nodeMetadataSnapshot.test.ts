import assert from 'node:assert/strict';
import test from 'node:test';
import type { MeshNode } from '../../types/domain.js';
import { PROHIBITED_NODE_MARKER } from '../../utils/pathing.js';
import { createNodeMetadataSnapshot } from './nodeMetadataSnapshot.js';

const node: MeshNode = { node_id: 'fixture', name: 'Invented relay', lat: 54, lon: -2,
  last_seen: '2026-11-12T00:00:00Z', is_online: true };

function fixture() {
  let state = { epoch: 1, nodes: new Map([[node.node_id, node]]),
    nodeChanges: { full: true, ids: [] as readonly string[], previous: null as Map<string, MeshNode> | null } };
  const read = createNodeMetadataSnapshot(() => state);
  const update = (nodes: Map<string, MeshNode>, ids = [...nodes.keys()], full = false, epoch = state.epoch) => {
    state = { epoch, nodes, nodeChanges: { full, ids, previous: state.nodes } };
  };
  return { read, update, state: () => state };
}

test('metadata consumers stay stable through consecutive presence updates without mutating live data', () => {
  const f = fixture();
  const first = f.read();
  for (const time of ['2026-11-12T00:00:01Z', '2026-11-12T00:00:02Z']) {
    f.update(new Map([[node.node_id, { ...node, last_seen: time }]]));
    assert.equal(f.read(), first);
    assert.equal(f.state().nodes.get(node.node_id)?.last_seen, time);
  }
  assert.equal(first.get(node.node_id)?.last_seen, node.last_seen);
});

test('names, privacy, search keys, coordinates and other metadata invalidate the view immediately', () => {
  for (const patch of [{ name: 'Renamed relay' }, { name: `Private ${PROHIBITED_NODE_MARKER}` }, { lat: 55 }, { lon: -3 },
    { public_key: 'invented-public-key' }, { iata: 'TST' }, { elevation_m: 100 }, { is_online: false }, { role: 1 }]) {
    const f = fixture();
    const first = f.read();
    const next = new Map([[node.node_id, { ...node, ...patch }]]);
    f.update(next);
    assert.equal(f.read(), next);
    assert.notEqual(f.read(), first);
  }
});

test('same-size membership replacement cannot retain a removed node', () => {
  const f = fixture();
  f.read();
  const next = new Map([['replacement', { ...node, node_id: 'replacement' }]]);
  f.update(next);
  assert.equal(f.read(), next);
  assert.equal(f.read().has(node.node_id), false);
});

test('a missed delta still discovers an earlier privacy or name change', () => {
  const f = fixture();
  f.read();
  const changed = { ...node, name: 'Changed while rendering was paused' };
  f.update(new Map([[node.node_id, changed]]));
  const latest = new Map([[node.node_id, { ...changed, last_seen: '2026-11-12T00:00:03Z' }]]);
  f.update(latest);
  assert.equal(f.read(), latest);
});

test('full snapshots and scope resets discard the previous view', () => {
  const f = fixture();
  const first = f.read();
  const refreshed = new Map([[node.node_id, { ...node }]]);
  f.update(refreshed, [], true);
  assert.equal(f.read(), refreshed);
  assert.notEqual(f.read(), first);
  const empty = new Map<string, MeshNode>();
  f.update(empty, [], true, 2);
  assert.equal(f.read(), empty);
  assert.equal(f.read().size, 0);
});
