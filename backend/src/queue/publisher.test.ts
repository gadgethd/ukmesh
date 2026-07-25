import assert from 'node:assert/strict';
import test from 'node:test';
import { buildLinkJobKey } from './publisher.js';
import { toPublicPlannedCoverage } from '../api/routes/plannedCoverage.js';

test('physical link canonicalization produces an order-independent logical key', () => {
  const a = 'a'.repeat(64);
  const b = 'b'.repeat(64);
  const forward = buildLinkJobKey({ type: 'physical_pair', node_a_id: a, node_b_id: b });
  const reverse = buildLinkJobKey({
    type: 'physical_pair',
    node_a_id: b,
    node_b_id: a,
  });
  assert.equal(forward, reverse);
});

test('observation link keys preserve path order and frequency-independent identity', () => {
  const base = {
    type: 'observe',
    rx_node_id: 'a'.repeat(64),
    src_node_id: 'b'.repeat(64),
    path_hashes: ['ABCD', '1234'],
    hop_count: 2,
    path_hash_size_bytes: 2,
  };
  assert.equal(buildLinkJobKey(base), buildLinkJobKey({ ...base }));
  assert.notEqual(
    buildLinkJobKey(base),
    buildLinkJobKey({ ...base, path_hashes: [...base.path_hashes].reverse() }),
  );
});

test('planned coverage projection replaces the shared internal job identity', () => {
  const projected = toPublicPlannedCoverage('plan_1111111111111111', {
    node_id: 'plan_aaaaaaaaaaaaaaaa',
    geom: { type: 'Polygon', coordinates: [] },
    radius_m: 12_000,
  });
  assert.equal(projected.node_id, 'plan_1111111111111111');
  assert.equal(JSON.stringify(projected).includes('plan_aaaaaaaaaaaaaaaa'), false);
  assert.equal(projected.radius_m, 12_000);
});
