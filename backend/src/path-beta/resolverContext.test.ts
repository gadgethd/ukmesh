import assert from 'node:assert/strict';
import test from 'node:test';
import { canReusePathContext } from './resolver.js';

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
