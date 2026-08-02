import assert from 'node:assert/strict';
import test from 'node:test';
import { coverageStore } from './useCoverage.js';
import { linkStateStore } from './useLinkState.js';

const A = 'a1'.repeat(32);
const B = 'b2'.repeat(32);
const KEY = `${A.toUpperCase()}:${B.toUpperCase()}`;

test('empty link snapshots clear state and viability transitions are exact', () => {
  const epoch = linkStateStore.reset('network-a');
  linkStateStore.applyInitialViableLinks([{
    node_a_id: B,
    node_b_id: A,
    observed_count: 4,
    itm_viable: true,
  }], epoch);
  assert.deepEqual(linkStateStore.getState().viablePairsArr, [[A.toUpperCase(), B.toUpperCase()]]);
  assert.equal(linkStateStore.getState().linkPairs.has(KEY), true);

  linkStateStore.applyInitialViableLinks([], epoch);
  assert.equal(linkStateStore.getState().linkPairs.size, 0);
  assert.equal(linkStateStore.getState().linkMetrics.size, 0);

  linkStateStore.applyLinkUpdate({
    node_a_id: A,
    node_b_id: B,
    observed_count: 5,
    itm_viable: true,
  }, epoch);
  assert.equal(linkStateStore.getState().linkPairs.has(KEY), true);

  linkStateStore.applyLinkUpdate({
    node_a_id: A,
    node_b_id: B,
    observed_count: 6,
    itm_viable: false,
  }, epoch);
  assert.equal(linkStateStore.getState().linkPairs.has(KEY), false);
  assert.equal(linkStateStore.getState().linkMetrics.has(KEY), false);

  linkStateStore.applyLinkUpdate({
    node_a_id: A,
    node_b_id: B,
    observed_count: 7,
    itm_viable: null,
  }, epoch);
  assert.equal(linkStateStore.getState().linkPairs.has(KEY), false, 'unknown must not add membership');
  assert.equal(linkStateStore.getState().linkMetrics.get(KEY)?.itm_viable, null);

  linkStateStore.applyLinkUpdate({
    node_a_id: A,
    node_b_id: B,
    observed_count: 8,
    itm_viable: true,
  }, epoch);
  assert.equal(linkStateStore.getState().linkPairs.has(KEY), true);
});

test('link and coverage stores reject late epochs and reset all scoped state', () => {
  const oldLinkEpoch = linkStateStore.reset('network-old');
  const currentLinkEpoch = linkStateStore.reset('network-current');
  linkStateStore.applyInitialViablePairs([[A, B]], oldLinkEpoch);
  assert.equal(linkStateStore.getState().linkPairs.size, 0);
  linkStateStore.applyInitialViablePairs([[A, B]], currentLinkEpoch);
  assert.equal(linkStateStore.getState().linkPairs.size, 1);

  const oldCoverageEpoch = coverageStore.reset('network-old|all');
  const currentCoverageEpoch = coverageStore.reset('network-current|all');
  coverageStore.handleCoverageUpdate({
    node_id: A,
    geom: { type: 'Polygon', coordinates: [] },
  }, oldCoverageEpoch);
  assert.equal(coverageStore.getState().coverage.length, 0);
  coverageStore.handleCoverageUpdate({
    node_id: A,
    geom: { type: 'Polygon', coordinates: [] },
  }, currentCoverageEpoch);
  assert.equal(coverageStore.getState().coverage[0]?.node_id, A.toUpperCase());

  coverageStore.reset('network-next|all');
  assert.equal(coverageStore.getState().coverage.length, 0);
  assert.equal(coverageStore.getState().loadedScopeKey, null);
});
