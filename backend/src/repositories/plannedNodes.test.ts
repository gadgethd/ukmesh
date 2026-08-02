import assert from 'node:assert/strict';
import test from 'node:test';
import {
  decodePlannedNodeCursor,
  encodePlannedNodeCursor,
} from './plannedNodes.js';

test('planned-node cursors round trip canonical keyset values', () => {
  const source = {
    publishedAt: '2026-07-29T12:00:00.000Z',
    id: '123e4567-e89b-42d3-a456-426614174000',
  };
  assert.deepEqual(decodePlannedNodeCursor(encodePlannedNodeCursor(source)), source);
  assert.equal(decodePlannedNodeCursor(undefined), null);
});

test('planned-node cursors reject malformed and non-UUID payloads', () => {
  assert.throws(() => decodePlannedNodeCursor('not!base64'), /INVALID_PLANNED_NODE_CURSOR/);
  const invalid = Buffer.from(JSON.stringify({
    publishedAt: 'yesterday',
    id: 'not-a-uuid',
  })).toString('base64url');
  assert.throws(() => decodePlannedNodeCursor(invalid), /INVALID_PLANNED_NODE_CURSOR/);
});
