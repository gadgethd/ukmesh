import assert from 'node:assert/strict';
import test from 'node:test';
import { addPathExplanation } from './pathingService.js';

test('addPathExplanation describes resolved and partially unresolved evidence', () => {
  const result = addPathExplanation({
    mode: 'resolved', confidence: 0.8, threshold: 0.45, permutationCount: 3, remainingHops: 1,
    debug: { hopsRequested: 4, hopsUsed: 3 },
  }) as { explanation: { evidenceLevel: string; reasons: string[]; alternativesConsidered: number } };
  assert.equal(result.explanation.evidenceLevel, 'high');
  assert.equal(result.explanation.alternativesConsidered, 3);
  assert.ok(result.explanation.reasons.some((reason) => reason.includes('remain below')));
});

test('addPathExplanation summarizes multi-observer resolution', () => {
  const result = addPathExplanation({ results: [
    { mode: 'resolved', confidence: 0.7, threshold: 0.45, debug: { hopsRequested: 2, hopsUsed: 2 } },
    { mode: 'none', confidence: null, debug: { hopsRequested: 2, hopsUsed: 0 } },
  ] }) as { explanation: { summary: string }; results: Array<{ explanation: unknown }> };
  assert.match(result.explanation.summary, /1 of 2/);
  assert.equal(result.results.length, 2);
  assert.ok(result.results.every((entry) => entry.explanation));
});
