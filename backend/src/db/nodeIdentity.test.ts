import assert from 'node:assert/strict';
import test from 'node:test';
import {
  assessIdentityPair,
  buildIdentityGroups,
  type IdentityEvidence,
  type IdentityEvidenceMaps,
  type IdentityNode,
} from './nodeIdentity.js';

const key = (character: string) => character.repeat(64);

const evidence = (overrides: Partial<IdentityEvidence> = {}): IdentityEvidence => ({
  statusCount: 0,
  statusFirstAt: null,
  statusLastAt: null,
  selfAdvertCount: 0,
  selfFirstAt: null,
  selfLastAt: null,
  pairAdvertCount: 0,
  pairPacketCount: 0,
  pairFirstAt: null,
  pairLastAt: null,
  ...overrides,
});

const node = (overrides: Partial<IdentityNode> = {}): IdentityNode => ({
  node_id: key('A'),
  name: 'GNOME-MSG-RPT',
  lat: 54.52,
  lon: -1.47,
  role: 2,
  advert_count: 100,
  last_seen: '2026-08-06T00:00:00Z',
  network: 'ukmesh',
  hardware_model: null,
  firmware_version: null,
  ...overrides,
});

const emptyMaps = (): IdentityEvidenceMaps => ({
  statuses: new Map(),
  selfAdverts: new Map(),
  pairs: new Map(),
});

test('accepts a same-position one-nibble key rotation', () => {
  const assessment = assessIdentityPair(
    node(),
    node({ node_id: `${'A'.repeat(63)}B` }),
    evidence(),
    evidence(),
  );

  assert.equal(assessment.accepted, true);
  assert.equal(assessment.confidence, 'high');
  assert.match(assessment.reason, /one-nibble-key-rotation/);
});

test('accepts a status observer paired with a positioned advert identity', () => {
  const assessment = assessIdentityPair(
    node({ node_id: key('B'), role: null, lat: null, lon: null, advert_count: 0 }),
    node({ node_id: key('C') }),
    evidence({ statusCount: 4 }),
    evidence(),
    { pairAdvertCount: 6, pairPacketCount: 6, pairFirstAt: Date.parse('2026-07-01'), pairLastAt: Date.parse('2026-07-02') },
  );

  assert.equal(assessment.accepted, true);
  assert.equal(assessment.confidence, 'high');
  assert.match(assessment.reason, /observer-advert-pair/);
});

test('does not merge same-name positioned repeaters without independent evidence', () => {
  const left = node({ node_id: key('D') });
  const right = node({ node_id: key('E'), lat: 55.1, lon: -1.47 });
  const result = buildIdentityGroups([left, right], emptyMaps());

  assert.equal(result.aliases.length, 0);
  assert.equal(result.assessments.length, 1);
  assert.equal(result.assessments[0]?.accepted, false);
  assert.equal(result.assessments[0]?.confidence, 'low');
});

test('does not merge colocated ordinal repeaters from observer packet pairing', () => {
  const assessment = assessIdentityPair(
    node({ node_id: key('J'), name: 'Dunston-1' }),
    node({ node_id: key('K'), name: 'Dunston-2' }),
    evidence({ statusCount: 10 }),
    evidence({ statusCount: 10 }),
    { pairAdvertCount: 20, pairPacketCount: 20, pairFirstAt: Date.parse('2026-07-01'), pairLastAt: Date.parse('2026-08-01') },
  );

  assert.equal(assessment.accepted, false);
  assert.match(assessment.reason, /ordinal-or-direction-variant/);
});

test('folds a metadata-only status row only when the active repeater is unique', () => {
  const stale = node({ node_id: key('F'), role: null, lat: null, lon: null, advert_count: 0 });
  const active = node({ node_id: key('G') });
  const assessment = assessIdentityPair(
    stale,
    active,
    evidence({ statusCount: 12 }),
    evidence(),
    undefined,
    { uniqueActiveRepeaterId: active.node_id },
  );

  assert.equal(assessment.accepted, true);
  assert.equal(assessment.confidence, 'medium');
});

test('canonical selection prefers the active positioned repeater row', () => {
  const stale = node({ node_id: key('H'), role: null, lat: null, lon: null, advert_count: 0 });
  const active = node({ node_id: key('I') });
  const maps = emptyMaps();
  maps.statuses.set(stale.node_id, { count: 1, firstAt: Date.parse('2026-01-01'), lastAt: Date.parse('2026-08-01') });

  const result = buildIdentityGroups([stale, active], maps);
  assert.equal(result.aliases.length, 1);
  assert.equal(result.aliases[0]?.sourceNodeId, stale.node_id);
  assert.equal(result.aliases[0]?.canonicalNodeId, active.node_id);
  assert.equal(result.aliases[0]?.confidence, 'medium');
});
