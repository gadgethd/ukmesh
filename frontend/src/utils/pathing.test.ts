import assert from 'node:assert/strict';
import test from 'node:test';
import type { MeshNode } from '../hooks/useNodes.js';
import { resolvePathWaypoints } from './pathing.js';

function fakeNode(id: string, name: string, lat: number, lon: number): MeshNode & { lat: number; lon: number } {
  return { node_id: id, name, lat, lon } as MeshNode & { lat: number; lon: number };
}

const RX = fakeNode('5100000000000000000000000000000000000000000000000000000000000000AA', 'RX', 51.5, -2.5);

test('1-byte hop with multiple candidate nodes stays unresolved (no invented relay)', () => {
  const nodes = new Map<string, MeshNode>([
    ['a', fakeNode('9E1CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'A', 55.0, -3.0)],
    ['b', fakeNode('9E99BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB', 'B', 56.0, -4.0)],
    ['rx', RX],
  ]);
  const waypoints = resolvePathWaypoints(['9E'], null, RX, nodes);
  assert.deepEqual(waypoints, [[RX.lat, RX.lon]]);
});

test('1-byte hop with a single live candidate is still resolved', () => {
  const only = fakeNode('9E1CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'ONLY', 55.0, -3.0);
  const nodes = new Map<string, MeshNode>([
    ['a', only],
    ['rx', RX],
  ]);
  const waypoints = resolvePathWaypoints(['9E'], null, RX, nodes);
  assert.deepEqual(waypoints, [[only.lat, only.lon], [RX.lat, RX.lon]]);
});

test('multi-byte hop keeps the nearest-to-corridor candidate pick', () => {
  const near = fakeNode('B4CE01AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'NEAR', 54.0, -2.0);
  const far = fakeNode('B4CE99BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB', 'FAR', 40.0, 5.0);
  const nodes = new Map<string, MeshNode>([
    ['near', near],
    ['far', far],
    ['rx', RX],
  ]);
  const waypoints = resolvePathWaypoints(['B4CE'], null, RX, nodes);
  assert.deepEqual(waypoints, [[near.lat, near.lon], [RX.lat, RX.lon]]);
});

test('unresolved 1-byte hops keep the src and rx anchors only', () => {
  const src = fakeNode('AA00000000000000000000000000000000000000000000000000000000000000BB', 'SRC', 50.0, -3.0);
  const nodes = new Map<string, MeshNode>([
    ['src', src],
    ['rx', RX],
  ]);
  const waypoints = resolvePathWaypoints(['9E', '91'], src, RX, nodes);
  assert.deepEqual(waypoints, [[src.lat, src.lon], [RX.lat, RX.lon]]);
});

test('empty/malformed hop is skipped instead of matching every node', () => {
  const nodes = new Map<string, MeshNode>([
    ['a', fakeNode('9E1CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'A', 55.0, -3.0)],
    ['rx', RX],
  ]);
  const waypoints = resolvePathWaypoints([''], null, RX, nodes);
  assert.deepEqual(waypoints, [[RX.lat, RX.lon]]);
});
