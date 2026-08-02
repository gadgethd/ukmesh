import assert from 'node:assert/strict';
import test from 'node:test';
import { filterTopologyLinks } from './topologyModel.js';

test('topology simulation excludes links to nodes without plot coordinates', () => {
  const links = [
    { source: 'located-a', target: 'located-b', observations: 4 },
    { source: 'located-a', target: 'unlocated-c', observations: 3 },
    { source: 'unlocated-d', target: 'located-b', observations: 2 },
  ];

  assert.deepEqual(
    filterTopologyLinks(['located-a', 'located-b'], links),
    [links[0]],
  );
});
