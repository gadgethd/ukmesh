import assert from 'node:assert/strict';
import test from 'node:test';
import { BoundedTtlMap } from '../cache/boundedTtlMap.js';
import { pathContextWeight, PATH_CONTEXT_MAX_WEIGHT } from './contextWeight.js';
import { buildChampionPriors } from '../path-shared/priors.js';
import type { BetaResolveContext, LinkMetrics, MeshNode } from './types.js';
import { LinkMetricsIndex, type LinkMetricRow } from './linkMetricsIndex.js';

function fixture(): BetaResolveContext & { linkMetrics: Map<string, LinkMetrics> } {
  return {
    loadedAt: 0, visibilityGeneration: 1, nodesById: new Map(), repeaterNodes: [], linkMetrics: new Map(),
    learningModel: { prefixProbabilities: new Map(), transitionProbabilities: new Map(), edgeScores: new Map(), motifProbabilities: new Map(), confidenceScale: 1, confidenceBias: 0, bucketHours: 6,
      champion: buildChampionPriors({ positionPrefixes: [], corridors: [], transitions: [], positionTransitions: [] }) },
  };
}

test('context weight includes companion nodes, links, learned and champion priors', () => {
  const context = fixture();
  let previous = pathContextWeight('ukmesh', context);
  const grows = (change: () => void) => { change(); const next = pathContextWeight('ukmesh', context); assert.ok(next > previous); previous = next; };
  const node: MeshNode = { node_id: 'A'.repeat(64), name: 'Invented companion', lat: 54, lon: -1, iata: 'LHR', role: 1, elevation_m: null, last_seen: null };
  grows(() => { context.nodesById.set(node.node_id, node); });
  grows(() => { context.linkMetrics.set('invented-edge', { observed_count: 1, multibyte_observed_count: 1, itm_path_loss_db: 100, itm_viable: true, count_a_to_b: 1, count_b_to_a: 0 }); });
  for (const map of [context.learningModel.prefixProbabilities, context.learningModel.transitionProbabilities, context.learningModel.edgeScores, context.learningModel.motifProbabilities, ...Object.values(context.learningModel.champion!)]) {
    grows(() => { map.set('invented-prior', 1); });
  }
  const beforeReference = pathContextWeight('ukmesh', context);
  context.repeaterNodes.push(node);
  assert.equal(pathContextWeight('ukmesh', context) - beforeReference, 8, 'the array reference does not retain a second node object');
});

test('large hidden prior maps enforce byte eviction and rejection', () => {
  const small = fixture(), large = fixture();
  for (let i = 0; i < 1_000; i++) large.learningModel.prefixProbabilities.set(String(i).padEnd(1024, 'x'), 1);
  assert.ok(Buffer.byteLength(JSON.stringify(large)) < 1000, 'old JSON estimate misses Map entries');
  assert.ok(pathContextWeight('ukmesh', large) > 2 * 1024 * 1024);
  const cache = new BoundedTtlMap<string, BetaResolveContext>({ maxEntries: 16, maxWeight: 1024 * 1024, ttlMs: 900_000, weightOf: pathContextWeight });
  try {
    cache.set('ukmesh', small);
    cache.set('other', large);
    assert.equal(cache.get('ukmesh'), small);
    assert.equal(cache.has('other'), false);
    assert.equal(cache.metrics().rejections, 1);
    cache.set('second', small);
    assert.ok(cache.weight() > pathContextWeight('ukmesh', small));
  } finally { cache.shutdown(); }
  assert.equal(PATH_CONTEXT_MAX_WEIGHT, 128 * 1024 * 1024);
});

test('packed graphs account for all backing stores and remain subject to the same byte cap', () => {
  const rows: LinkMetricRow[] = Array.from({ length: 20_000 }, (_, index) => ({
    node_a_id: String(index % 1000).padStart(64, '0'), node_b_id: String(index + 1000).padStart(64, '0'),
    observed_count: 1, multibyte_observed_count: 2, itm_path_loss_db: 100, itm_viable: true, count_a_to_b: 3, count_b_to_a: 4,
  }));
  const empty = fixture(), packed: BetaResolveContext = { ...empty, linkMetrics: new LinkMetricsIndex(rows) };
  const weight = pathContextWeight('ukmesh', packed) - pathContextWeight('ukmesh', empty);
  const buffers = (packed.linkMetrics as LinkMetricsIndex).retainedReferences()
    .filter(ArrayBuffer.isView).reduce((sum, view) => sum + view.byteLength, 0);
  assert.ok(weight > buffers + 20_000 * 48, 'includes numeric storage and Map entries');
  const cache = new BoundedTtlMap<string, BetaResolveContext>({ maxEntries: 16, maxWeight: weight - 1, ttlMs: 900_000, weightOf: pathContextWeight });
  try {
    cache.set('ukmesh', packed);
    assert.equal(cache.has('ukmesh'), false);
    assert.equal(cache.metrics().rejections, 1);
  } finally { cache.shutdown(); }
  assert.equal(PATH_CONTEXT_MAX_WEIGHT, 128 * 1024 * 1024);
});
