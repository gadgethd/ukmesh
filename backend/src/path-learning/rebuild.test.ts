import assert from 'node:assert/strict';
import test from 'node:test';
import { buildBoundedLearningPathHashIndex } from './rebuild.js';

test('learning prefix buckets overflow closed at the 129th candidate', () => {
  const nodes = Array.from({ length: 130 }, (_, index) => ({
    node_id: `aa${index.toString(16).padStart(62, '0')}`,
  }));
  const index = buildBoundedLearningPathHashIndex(nodes);
  assert.deepEqual(index.get('AA'), []);
  assert.ok((index.get('AA0000')?.length ?? 0) <= 128);
});
