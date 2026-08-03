import assert from 'node:assert/strict';
import test from 'node:test';
import {
  FAIL_CLOSED_RUNTIME_FEATURES,
  getRuntimeFeatureSnapshot,
  parseRuntimeFeatureConfig,
  refreshRuntimeFeatures,
  resetRuntimeFeaturesForTests,
} from './runtimeFeatures.js';

test('runtime feature parser accepts only the bounded versioned DTO', () => {
  assert.deepEqual(parseRuntimeFeatureConfig({
    version: 1,
    packetArcs: false,
    heatmap: true,
    privacyGeneration: 9,
    refreshAfterSeconds: 15,
    ignored: 'safe',
  }), {
    packetArcs: false,
    heatmap: true,
    privacyGeneration: 9,
    refreshAfterSeconds: 15,
  });

  for (const malformed of [
    null,
    {},
    { version: 2, packetArcs: true, heatmap: true, privacyGeneration: 1, refreshAfterSeconds: 30 },
    { version: 1, packetArcs: 'true', heatmap: true, privacyGeneration: 1, refreshAfterSeconds: 30 },
    { version: 1, packetArcs: true, heatmap: true, privacyGeneration: 1, refreshAfterSeconds: 301 },
  ]) {
    assert.deepEqual(parseRuntimeFeatureConfig(malformed), FAIL_CLOSED_RUNTIME_FEATURES);
  }
});

test('runtime feature refresh fails closed on transport and malformed responses', async () => {
  resetRuntimeFeaturesForTests();
  const enabledResponse = new Response(JSON.stringify({
    version: 1,
    packetArcs: true,
    heatmap: true,
    privacyGeneration: 3,
    refreshAfterSeconds: 5,
  }), { status: 200 });
  await refreshRuntimeFeatures(async () => enabledResponse);
  assert.equal(getRuntimeFeatureSnapshot().packetArcs, true);

  await refreshRuntimeFeatures(async () => {
    throw new Error('offline');
  });
  assert.deepEqual(getRuntimeFeatureSnapshot(), FAIL_CLOSED_RUNTIME_FEATURES);

  await refreshRuntimeFeatures(async () => new Response('{}', { status: 200 }));
  assert.deepEqual(getRuntimeFeatureSnapshot(), FAIL_CLOSED_RUNTIME_FEATURES);
});
