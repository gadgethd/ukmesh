import assert from 'node:assert/strict';
import test from 'node:test';
import { parseStatsSection, STATS_SECTION_FIELDS } from './statsSections.js';
import { chartStats } from '../../test/e2e/fixtures/chartStats.mjs';

const scope = { network: 'ukmesh', observer: null, generation: 1 };
function partial(section: keyof typeof STATS_SECTION_FIELDS) {
  return { snapshot: chartStats.snapshot, summary: chartStats.summary,
    representation: { sections: [section], scope: { network: 'ukmesh', observer: null }, visibilityGeneration: 1 },
    ...Object.fromEntries(STATS_SECTION_FIELDS[section].map(key => [key, chartStats[key]])) };
}

test('partial stats validate every active tab and retain full-server compatibility', () => {
  for (const section of Object.keys(STATS_SECTION_FIELDS) as (keyof typeof STATS_SECTION_FIELDS)[]) {
    const parsed = parseStatsSection(partial(section), section, scope);
    assert.ok(parsed, section);
    assert.equal(parsed.data.summary.totalPackets24h, 120);
    assert.ok(parseStatsSection(chartStats, section, scope), `legacy full ${section}`);
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
