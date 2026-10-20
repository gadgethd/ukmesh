import assert from 'node:assert/strict';
import test from 'node:test';
import {
  confidenceFor,
  isWithinUkDisplayEnvelope,
  shapeRegionRepeatersPayload,
  shapeRegionScopePayload,
  shapeRegionSummaryPayload,
  type RegionMemberRow,
  type RegionStatRow,
} from './regions.js';

const NODE_ID = 'A'.repeat(64);
const PRIVATE_ID = 'B'.repeat(64);
const OTHER_ID = 'C'.repeat(64);

test('confidenceFor applies confirmed / probable / single tiers', () => {
  assert.equal(confidenceFor(12), 'confirmed');
  assert.equal(confidenceFor(4), 'confirmed');
  assert.equal(confidenceFor(3), 'probable');
  assert.equal(confidenceFor(2), 'probable');
  assert.equal(confidenceFor(1), 'single');
  assert.equal(confidenceFor(0), 'single');
});

test('isWithinUkDisplayEnvelope keeps UK positions and rejects foreign, junk and placeholder coordinates', () => {
  assert.equal(isWithinUkDisplayEnvelope(54.2, -1.2), true);
  assert.equal(isWithinUkDisplayEnvelope(54.6, -5.9), true);
  assert.equal(isWithinUkDisplayEnvelope(60.1, -1.3), true);
  assert.equal(isWithinUkDisplayEnvelope(59.2, 9.5), false);
  assert.equal(isWithinUkDisplayEnvelope(0, 0), false);
  assert.equal(isWithinUkDisplayEnvelope(1910, -1426), false);
  assert.equal(isWithinUkDisplayEnvelope(null, -1), false);
});

test('shapeRegionSummaryPayload nests members, applies confidence and drops opted-out nodes', () => {
  const payload = shapeRegionSummaryPayload(
    { finished_at: new Date('2026-10-06T12:00:00.000Z'), window_days: 92 },
    [
      {
        region: 'yorkshire', packets_total: 8967, member_count: 117, repeater_member_count: 116,
        first_seen: new Date('2026-09-05T00:01:44.000Z'), last_seen: new Date('2026-10-06T00:00:00.000Z'),
      },
      {
        region: 'ren', packets_total: 4, member_count: 23, repeater_member_count: 21,
        first_seen: null, last_seen: null,
      },
    ],
    [
      { region: 'yorkshire', node_id: NODE_ID, packets: 7416, name: 'Hub One', role: 2, lat: 53.9, lon: -1.7 },
      { region: 'yorkshire', node_id: PRIVATE_ID, packets: 900, name: 'Opted Out 🚫', role: 2, lat: 54.1, lon: -1.2 },
      { region: 'yorkshire', node_id: OTHER_ID, packets: 120, name: null, role: null, lat: null, lon: null },
      { region: 'yorkshire', node_id: 'D'.repeat(64), packets: 50, name: 'Nordic Node', role: 2, lat: 59.2, lon: 9.5 },
      { region: 'yorkshire', node_id: 'E'.repeat(64), packets: 40, name: 'Null Island', role: 2, lat: 0, lon: 0 },
    ],
  );
  assert.equal(payload.generatedAt, '2026-10-06T12:00:00.000Z');
  assert.equal(payload.windowDays, 92);
  assert.equal(payload.regions.length, 2);
  const yorkshire = payload.regions[0]!;
  assert.equal(yorkshire.name, 'yorkshire');
  assert.equal(yorkshire.confidence, 'confirmed');
  assert.equal(yorkshire.firstSeen, '2026-09-05T00:01:44.000Z');
  assert.equal(yorkshire.members.length, 2);
  assert.deepEqual(yorkshire.members.map((member) => member.nodeId), [NODE_ID, OTHER_ID]);
  assert.equal(yorkshire.members.every((member) => member.isBridge === false), true);
  assert.equal(payload.regions[1]?.confidence, 'confirmed');
});

test('shapeRegionSummaryPayload tolerates a missing run row', () => {
  const payload = shapeRegionSummaryPayload(null, [], []);
  assert.equal(payload.generatedAt, null);
  assert.equal(payload.windowDays, null);
  assert.deepEqual(payload.regions, []);
});

test('shapeRegionRepeatersPayload filters opted-out nodes and normalises regions json', () => {
  const payload = shapeRegionRepeatersPayload(
    { finished_at: '2026-10-06T12:00:00.000Z', window_days: 92 },
    [
      {
        node_id: NODE_ID, status: 'green', evidence: 'scoped', scoped_packets: 7781, unscoped_packets_7d: 123,
        regions: { yorkshire: 7416, 'eng-ne': 220, junk: 0 as unknown as number }, name: 'Hub One', role: 2, lat: 53.9, lon: -1.7,
      },
      {
        node_id: PRIVATE_ID, status: 'green', evidence: 'scoped', scoped_packets: 10, unscoped_packets_7d: 2,
        regions: { yorkshire: 10 }, name: 'Hidden 🚫', role: 2, lat: 54, lon: -1,
      },
      {
        node_id: OTHER_ID, status: 'red', evidence: 'unscoped-only', scoped_packets: 0, unscoped_packets_7d: 44,
        regions: {}, name: 'Quiet Repeater', role: 2, lat: 53, lon: -2,
      },
      {
        node_id: 'F'.repeat(64), status: 'red', evidence: 'unscoped-only', scoped_packets: 0, unscoped_packets_7d: 900,
        regions: {}, name: 'DK_7620_Lemvig_R02', role: 2, lat: 56.52, lon: 8.22,
      },
    ],
  );
  assert.equal(payload.repeaters.length, 2);
  assert.deepEqual(payload.repeaters[0]?.regions, { yorkshire: 7416, 'eng-ne': 220 });
  assert.equal(payload.repeaters[1]?.status, 'red');
  assert.equal(payload.repeaters[1]?.evidence, 'unscoped-only');
});

test('shapeRegionScopePayload reports green, unobserved and missing states', () => {
  const green = shapeRegionScopePayload(
    null,
    {
      node_id: NODE_ID, status: 'green', evidence: 'scoped', scoped_packets: 6816, unscoped_packets_7d: 54,
      regions: { yorkshire: 6816 }, name: 'Hub One', role: 2, lat: 53.9, lon: -1.7,
    },
    [{ region: 'yorkshire', packets: 6816 }, { region: 'eng-ne', packets: 33 }],
  );
  assert.equal(green?.status, 'green');
  assert.deepEqual(green?.regions, [{ region: 'yorkshire', packets: 6816 }, { region: 'eng-ne', packets: 33 }]);

  const unobserved = shapeRegionScopePayload(
    { node_id: OTHER_ID, name: 'New Repeater', role: 2, lat: 52, lon: -1 },
    null,
    [],
  );
  assert.equal(unobserved?.status, 'unobserved');
  assert.equal(unobserved?.scopedPackets, 0);
  assert.deepEqual(unobserved?.regions, []);

  const redacted = shapeRegionScopePayload(
    null,
    {
      node_id: PRIVATE_ID, status: 'red', evidence: 'unscoped-only', scoped_packets: 0, unscoped_packets_7d: 7,
      regions: {}, name: 'Hidden 🚫', role: 2, lat: 54, lon: -1,
    },
    [],
  );
  assert.equal(redacted?.name, 'Private Node');
  assert.equal(redacted?.lat, null);
  assert.equal(redacted?.lon, null);
  assert.equal(redacted?.nodeId, PRIVATE_ID);

  assert.equal(shapeRegionScopePayload(null, null, []), null);
});

test('shapeRegionSummaryPayload flags universal bridge members at the 12-region threshold', () => {
  const stats: RegionStatRow[] = [];
  const members: RegionMemberRow[] = [];
  for (let index = 0; index < 12; index += 1) {
    const region = `bridge-${index}`;
    stats.push({
      region, packets_total: 10, member_count: 2, repeater_member_count: 2,
      first_seen: null, last_seen: null,
    });
    members.push({ region, node_id: NODE_ID, packets: 10, name: 'Universal Hub', role: 2, lat: 54, lon: -1 });
  }
  for (let index = 0; index < 11; index += 1) {
    const region = `local-${index}`;
    stats.push({
      region, packets_total: 5, member_count: 1, repeater_member_count: 1,
      first_seen: null, last_seen: null,
    });
    members.push({ region, node_id: OTHER_ID, packets: 5, name: 'Local Repeater', role: 2, lat: 53, lon: -2 });
  }
  const payload = shapeRegionSummaryPayload(null, stats, members);
  const bridgeRegion = payload.regions.find((region) => region.name === 'bridge-0');
  const localRegion = payload.regions.find((region) => region.name === 'local-0');
  assert.equal(bridgeRegion?.members[0]?.isBridge, true);
  assert.equal(localRegion?.members[0]?.isBridge, false);
});
