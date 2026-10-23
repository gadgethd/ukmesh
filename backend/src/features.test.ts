import assert from 'node:assert/strict';
import test from 'node:test';
import {
  envFlagEnabled,
  getPublicRuntimeFeatureConfig,
} from './features.js';

test('runtime map features are fail-closed by default', () => {
  assert.deepEqual(getPublicRuntimeFeatureConfig({}), {
    version: 1,
    inferredNodes: false,
    packetArcs: false,
    heatmap: false,
    regionScoping: false,
    privacyGeneration: 0,
    refreshAfterSeconds: 30,
  });
});

test('runtime map features parse independent flags and bound the refresh TTL', () => {
  assert.deepEqual(getPublicRuntimeFeatureConfig({
    PUBLIC_FEATURE_INFERRED_NODES_ENABLED: 'true',
    PUBLIC_FEATURE_PACKET_ARCS_ENABLED: '1',
    PUBLIC_FEATURE_HEATMAP_ENABLED: 'off',
    PUBLIC_FEATURE_REGION_SCOPING_ENABLED: 'true',
    PUBLIC_FEATURE_CONFIG_TTL_SECONDS: '9999',
  }, 17), {
    version: 1,
    inferredNodes: true,
    packetArcs: true,
    heatmap: false,
    regionScoping: true,
    privacyGeneration: 17,
    refreshAfterSeconds: 300,
  });
  assert.equal(
    getPublicRuntimeFeatureConfig({ PUBLIC_FEATURE_CONFIG_TTL_SECONDS: '1' }).refreshAfterSeconds,
    5,
  );
  assert.equal(envFlagEnabled('disabled'), false);
});
