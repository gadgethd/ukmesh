import assert from 'node:assert/strict';
import test from 'node:test';
import { groupOwnerNodes, type OwnerDashboardRow } from './ownerDashboard.js';

const row = (overrides: Partial<OwnerDashboardRow>): OwnerDashboardRow => ({
  canonical_id: 'A1'.repeat(32),
  name: null,
  network: 'ukmesh',
  last_seen: null,
  advert_count: 0,
  lat: null,
  lon: null,
  iata: null,
  role: 2,
  members: [],
  ...overrides,
});

test('owner dashboard groups same-name identities and preserves every member key', () => {
  const olderKey = 'B2'.repeat(32);
  const newerKey = 'C3'.repeat(32);
  const gnomeCanonical = 'D4'.repeat(32);
  const gnomeRotation = 'E5'.repeat(32);

  const nodes = groupOwnerNodes([
    row({
      canonical_id: olderKey,
      name: '2E0MTU RPT Hilperton',
      members: [olderKey],
      last_seen: '2026-07-25T12:00:00Z',
      advert_count: 6,
    }),
    row({
      canonical_id: newerKey,
      name: ' 2E0MTU   RPT Hilperton ',
      members: [newerKey],
      last_seen: '2026-07-31T12:00:00Z',
      advert_count: 3,
    }),
    row({
      canonical_id: gnomeCanonical,
      name: 'GNOME-MSG-RPT',
      members: [gnomeCanonical, gnomeRotation],
      last_seen: '2026-08-06T12:00:00Z',
      advert_count: 10,
    }),
  ], [olderKey, gnomeRotation]);

  assert.equal(nodes.length, 2);
  const hilperton = nodes.find((node) => node.name === ' 2E0MTU   RPT Hilperton ' || node.name === '2E0MTU RPT Hilperton');
  assert.ok(hilperton);
  assert.equal(hilperton.node_id, olderKey);
  assert.equal(hilperton.canonicalId, newerKey);
  assert.deepEqual(hilperton.members, [olderKey, newerKey].sort());
  assert.equal(hilperton.advert_count, 9);

  const gnome = nodes.find((node) => node.name === 'GNOME-MSG-RPT');
  assert.ok(gnome);
  assert.equal(gnome.node_id, gnomeRotation);
  assert.deepEqual(gnome.members, [gnomeCanonical, gnomeRotation].sort());
});

test('owner dashboard keeps unnamed canonical identities separate', () => {
  const first = 'F6'.repeat(32);
  const second = '07'.repeat(32);
  const nodes = groupOwnerNodes([
    row({ canonical_id: first, members: [first] }),
    row({ canonical_id: second, members: [second] }),
  ], [first, second]);

  assert.deepEqual(nodes.map((node) => node.canonicalId).sort(), [first, second].sort());
  assert.deepEqual(nodes.map((node) => node.members), [[first], [second]].sort());
});

test('owner dashboard prefers the freshest authorized key when same-name keys are both owned', () => {
  // Regression (2026-09-18, Thinford Repeater): two same-name keys are both
  // authorized; owner auth returns node ids ORDER BY node_id ASC and the stale
  // key sorts first. The dashboard must select the freshest member so the live
  // endpoints load the active key's data instead of the dead key's zeros.
  const staleKey = '35CD1CDD'.repeat(8);
  const liveKey = 'F518585D'.repeat(8);

  const nodes = groupOwnerNodes([
    row({
      canonical_id: liveKey,
      name: 'Thinford Repeater',
      members: [liveKey],
      last_seen: '2026-09-18T11:38:09Z',
      advert_count: 2147,
    }),
    row({
      canonical_id: staleKey,
      name: 'Thinford Repeater',
      members: [staleKey],
      last_seen: '2026-04-14T08:54:50Z',
      advert_count: 0,
    }),
  ], [staleKey, liveKey].sort());

  assert.equal(nodes.length, 1);
  assert.equal(nodes[0].node_id, liveKey);
  assert.equal(nodes[0].canonicalId, liveKey);
  assert.deepEqual(nodes[0].members, [liveKey, staleKey].sort());
  assert.equal(nodes[0].advert_count, 2147);
});
