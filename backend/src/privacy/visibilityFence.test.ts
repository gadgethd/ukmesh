import assert from 'node:assert/strict';
import test from 'node:test';
import {
  PublicVisibilityChangedError,
  withStablePublicVisibility,
} from './visibilityFence.js';

test('public cache loads retry rather than returning a stale privacy generation', async () => {
  let generation = 1;
  const loaded: number[] = [];
  const result = await withStablePublicVisibility(
    async () => generation,
    async (observedGeneration) => {
      loaded.push(observedGeneration);
      if (observedGeneration === 1) generation = 2;
      return `generation-${observedGeneration}`;
    },
  );

  assert.equal(result, 'generation-2');
  assert.deepEqual(loaded, [1, 2]);
});

test('public cache loads fail closed during repeated privacy churn', async () => {
  let generation = 1;
  await assert.rejects(
    withStablePublicVisibility(
      async () => generation,
      async () => {
        generation += 1;
        return 'stale';
      },
    ),
    PublicVisibilityChangedError,
  );
});
