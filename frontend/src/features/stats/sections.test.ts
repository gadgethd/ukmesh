import assert from 'node:assert/strict';
import test from 'node:test';
import { parseStatsSection } from './sections.js';
import { chartStats } from '../../../test/e2e/fixtures/chartStats.mjs';

const scope = { network: 'ukmesh', observer: null, generation: 1 };
// Independent wire fixtures: deriving these fields from the implementation
// would allow both producer and parser tests to omit the same field.
const wire = {
  overview: { packetsPerHour: chartStats.packetsPerHour, packetsPerDay: chartStats.packetsPerDay,
    radiosPerHour: chartStats.radiosPerHour, radiosPerDay: chartStats.radiosPerDay, observerDiversity: chartStats.observerDiversity },
  traffic: { packetTypes: chartStats.packetTypes, channelTraffic: chartStats.channelTraffic,
    routeTypes: chartStats.routeTypes, transportCodes: chartStats.transportCodes },
  observers: { observerRegions: chartStats.observerRegions, observerDiversity: chartStats.observerDiversity },
  paths: { pathHashes: chartStats.pathHashes, hopDistribution: chartStats.hopDistribution,
    prefixCollisions: chartStats.prefixCollisions, pathDecodeTrend: chartStats.pathDecodeTrend },
  signal: { signalSummary: chartStats.signalSummary },
};
function partial(section: keyof typeof wire) {
  return { snapshot: chartStats.snapshot, summary: chartStats.summary,
    representation: { sections: [section], scope: { network: 'ukmesh', observer: null }, visibilityGeneration: 1 },
    ...wire[section] };
}

test('partial stats validate every active tab and retain full-server compatibility', () => {
  for (const section of Object.keys(wire) as (keyof typeof wire)[]) {
    const parsed = parseStatsSection(partial(section), section, scope);
    assert.ok(parsed, section);
    assert.equal(parsed.data.summary.totalPackets24h, 120);
    assert.ok(parseStatsSection(chartStats, section, scope), `legacy full ${section}`);
  }
});

test('every independently specified wire field is required in its selected section', () => {
  for (const section of Object.keys(wire) as (keyof typeof wire)[]) {
    for (const field of ['summary', ...Object.keys(wire[section])]) {
      assert.equal(parseStatsSection({ ...partial(section), [field]: undefined }, section, scope), null, `${section}.${field}`);
    }
  }
});

test('selected stats reject mismatched representation, scope, generation and malformed nested values', () => {
  const value = partial('overview');
  assert.equal(parseStatsSection(value, 'paths', scope), null);
  assert.equal(parseStatsSection(value, 'overview', { ...scope, generation: 2 }), null);
  assert.equal(parseStatsSection(value, 'overview', { ...scope, network: 'test' }), null);
  assert.equal(parseStatsSection(value, 'overview', { ...scope, observer: 'synthetic' }), null);
  assert.equal(parseStatsSection({ ...value, packetsPerHour: [{ hour: '12:00', count: '24' }] }, 'overview', scope), null);
  assert.equal(parseStatsSection({ ...partial('paths'), pathHashes: { ...chartStats.pathHashes, latestFullyDecodedNodes: [{}] } }, 'paths', scope), null);
  assert.equal(parseStatsSection({ ...partial('signal'), signalSummary: { ...chartStats.signalSummary, avgSnr: NaN } }, 'signal', scope), null);
  assert.equal(parseStatsSection({ ...partial('observers'), observerRegions: [{}] }, 'observers', scope), null);
  assert.equal(parseStatsSection({ ...value, radiosPerDay: undefined }, 'overview', scope), null);
});
