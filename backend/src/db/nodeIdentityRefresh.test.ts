import assert from 'node:assert/strict';
import test from 'node:test';
import { automaticIdentityAliasSetsEqual, type IdentityAlias } from './nodeIdentity.js';

const desired: IdentityAlias = {
  sourceNodeId: 'A'.repeat(64),
  canonicalNodeId: 'B'.repeat(64),
  confidence: 'high',
  reason: 'rotation',
  evidence: { z: [2, { b: true, a: 1 }], a: 'stable' },
};

test('automatic identity refresh skips a byte-equivalent semantic alias generation', () => {
  const current = [{
    source_node_id: desired.sourceNodeId,
    canonical_node_id: desired.canonicalNodeId,
    confidence: desired.confidence,
    reason: desired.reason,
    evidence: { a: 'stable', z: [2, { a: 1, b: true }] },
  }];
  assert.equal(automaticIdentityAliasSetsEqual(current, [desired]), true);
  assert.equal(automaticIdentityAliasSetsEqual([{
    ...current[0]!, confidence: 'medium', reason: 'new audit evidence', evidence: { changed: true },
  }], [desired]), true);
  assert.equal(automaticIdentityAliasSetsEqual(current, [
    { ...desired, canonicalNodeId: 'C'.repeat(64) },
  ]), false);
  assert.equal(automaticIdentityAliasSetsEqual([], [desired]), false);
});
