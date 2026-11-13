import assert from 'node:assert/strict';
import test from 'node:test';
import { buildOneBytePrefixProbabilities, canReusePathContext, loadPathContext } from './context.js';
import { mockRuntimeQuery } from '../db/fixtures/mockRuntimeQuery.js';
import { pathingConfig } from '../platform/config/pathing.js';

test('collapses multibyte path facts into calibrated one-byte prefix priors', () => {
  const probabilities = buildOneBytePrefixProbabilities([
    { prefix: 'AA01', receiver_region: 'LON', prev_prefix: 'BB01', node_id: 'node-a', count: 3 },
    { prefix: 'AA02', receiver_region: 'LON', prev_prefix: 'BBFF', node_id: 'node-a', count: 2 },
    { prefix: 'AAFF', receiver_region: 'LON', prev_prefix: 'BB02', node_id: 'node-b', count: 5 },
    { prefix: 'CC01', receiver_region: 'LON', prev_prefix: null, node_id: 'node-c', count: 4 },
  ]);

  assert.equal(probabilities.get('LON|AA|BB|node-a'), 0.5);
  assert.equal(probabilities.get('LON|AA|BB|node-b'), 0.5);
  assert.equal(probabilities.get('LON|CC||node-c'), 1);
  assert.equal([...probabilities.keys()].some((key) => key.includes('AA01')), false);
});

test('interactive path context reuse expires normally', () => {
  assert.equal(canReusePathContext({
    cachedVisibilityGeneration: 7,
    currentVisibilityGeneration: 7,
    ageMs: 1,
    pinForBatch: false,
  }), true);
  assert.equal(canReusePathContext({
    cachedVisibilityGeneration: 7,
    currentVisibilityGeneration: 7,
    ageMs: 60 * 60_000,
    pinForBatch: false,
  }), false);
});

test('batch path context stays pinned only within the same visibility generation', () => {
  assert.equal(canReusePathContext({
    cachedVisibilityGeneration: 7,
    currentVisibilityGeneration: 7,
    ageMs: 60 * 60_000,
    pinForBatch: true,
  }), true);
  assert.equal(canReusePathContext({
    cachedVisibilityGeneration: 7,
    currentVisibilityGeneration: 8,
    ageMs: 1,
    pinForBatch: true,
  }), false);
});

test('context loading owns cache reuse, visibility fences and cancellation', async (t) => {
  let now = Date.now();
  let nodeLoads = 0;
  let generation = 7;
  let generationReads: number[] = [];
  let onNodeRead: (() => void) | undefined;
  t.mock.method(Date, 'now', () => now);
  mockRuntimeQuery(t, async (sql) => {
    if (sql.includes('FROM ops.public_visibility_state')) {
      return { rows: [{ generation: String(generationReads.shift() ?? generation) }] };
    }
    if (sql.includes('FROM core.nodes')) {
      nodeLoads += 1;
      onNodeRead?.();
      return { rows: [{ node_id: 'relay', name: null, lat: 54, lon: -1, role: 2 }] };
    }
    return { rows: [] };
  });

  await t.test('same-generation calls share one snapshot and a new generation replaces it', async () => {
    const first = await loadPathContext('context-reuse');
    assert.equal(first.nodesById.get('relay')?.node_id, 'relay');
    assert.equal(first.repeaterNodes.length, 1);
    assert.equal(await loadPathContext('context-reuse', { pinForBatch: true }), first);
    assert.equal(nodeLoads, 1);
    generation += 1;
    const refreshed = await loadPathContext('context-reuse', { pinForBatch: true });
    assert.notEqual(refreshed, first);
    assert.equal(refreshed.visibilityGeneration, generation);
    assert.equal(nodeLoads, 2);
  });

  await t.test('interactive loads refresh after the configured TTL', async () => {
    const first = await loadPathContext('context-expiry');
    const before = nodeLoads;
    now += pathingConfig.contextTtlMs + 1;
    const refreshed = await loadPathContext('context-expiry');
    assert.notEqual(refreshed, first);
    assert.equal(nodeLoads, before + 1);
  });

  await t.test('a required-generation mismatch fails before graph reads', async () => {
    const before = nodeLoads;
    await assert.rejects(loadPathContext('context-required', {
      requiredVisibilityGeneration: generation - 1,
    }), /PUBLIC_VISIBILITY_CHANGED_DURING_RESOLUTION/);
    assert.equal(nodeLoads, before);
  });

  await t.test('one visibility change retries and only the confirmed snapshot is cached', async () => {
    const before = nodeLoads;
    generationReads = [11, 12, 12, 12];
    const context = await loadPathContext('context-retry');
    assert.equal(context.visibilityGeneration, 12);
    assert.equal(nodeLoads, before + 2);
    assert.equal(await loadPathContext('context-retry', { currentVisibilityGeneration: 12 }), context);
    assert.equal(nodeLoads, before + 2);
  });

  await t.test('repeated visibility changes stop after two attempts without caching either', async () => {
    const before = nodeLoads;
    generationReads = [21, 22, 23, 24];
    await assert.rejects(loadPathContext('context-churn'), /PUBLIC_VISIBILITY_CHANGED_DURING_CONTEXT_LOAD/);
    assert.equal(nodeLoads, before + 2);
    await loadPathContext('context-churn', { currentVisibilityGeneration: 24 });
    assert.equal(nodeLoads, before + 3);
  });

  await t.test('an already-aborted request performs no graph reads', async () => {
    const controller = new AbortController();
    controller.abort(new Error('context cancelled before loading'));
    const before = nodeLoads;
    await assert.rejects(loadPathContext('context-pre-abort', { signal: controller.signal }), /context cancelled before loading/);
    assert.equal(nodeLoads, before);
  });

  await t.test('cancellation during graph loading cannot populate the cache', async () => {
    const controller = new AbortController();
    onNodeRead = () => controller.abort(new Error('context cancelled during loading'));
    await assert.rejects(loadPathContext('context-abort', {
      currentVisibilityGeneration: generation, signal: controller.signal,
    }), /context cancelled during loading/);
    onNodeRead = undefined;
    const before = nodeLoads;
    await loadPathContext('context-abort', { currentVisibilityGeneration: generation });
    assert.equal(nodeLoads, before + 1);
  });
});
