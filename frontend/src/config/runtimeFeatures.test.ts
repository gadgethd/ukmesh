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
    regionScoping: false,
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

test('region scoping requires an explicit boolean true from runtime config', () => {
  for (const regionScoping of [undefined, false, 'true', 1, true]) {
    assert.equal(parseRuntimeFeatureConfig({
      version: 1, packetArcs: false, heatmap: false, privacyGeneration: 0,
      refreshAfterSeconds: 30, regionScoping,
    }).regionScoping, regionScoping === true);
  }
});

test('oversized runtime responses cancel streaming and invalidate the previous authority', async () => {
  resetRuntimeFeaturesForTests();
  await refreshRuntimeFeatures(async () => new Response(JSON.stringify({
    version: 1, packetArcs: true, heatmap: true, privacyGeneration: 4, refreshAfterSeconds: 5,
  })));
  assert.equal(getRuntimeFeatureSnapshot().privacyGeneration, 4);
  let cancelled = false;
  await refreshRuntimeFeatures(async (_input, init) => {
    assert.equal(init?.cache, 'no-store');
    assert.equal(init?.credentials, 'same-origin');
    return new Response(new ReadableStream({
      pull(controller) { controller.enqueue(new Uint8Array(4096)); },
      cancel() { cancelled = true; },
    }));
  });
  assert.equal(cancelled, true);
  assert.deepEqual(getRuntimeFeatureSnapshot(), FAIL_CLOSED_RUNTIME_FEATURES);
});

test('hidden/stopped authority reads cannot publish late data; a request deadline fails closed', async () => {
  const body = (privacyGeneration: number) => new Response(JSON.stringify({
    version: 1, packetArcs: true, heatmap: true, privacyGeneration, refreshAfterSeconds: 5,
  }));
  await refreshRuntimeFeatures(async () => body(3));
  const cancelled = new AbortController();
  let complete!: (response: Response) => void;
  const pending = refreshRuntimeFeatures(() => new Promise(resolve => { complete = resolve; }), cancelled.signal);
  cancelled.abort(new DOMException('Page hidden', 'AbortError'));
  complete(body(9));
  await pending;
  assert.equal(getRuntimeFeatureSnapshot().privacyGeneration, 3);
  const deadline = new AbortController();
  deadline.abort(new DOMException('Poll timed out', 'TimeoutError'));
  await refreshRuntimeFeatures(async () => body(9), deadline.signal);
  assert.deepEqual(getRuntimeFeatureSnapshot(), FAIL_CLOSED_RUNTIME_FEATURES);
});
