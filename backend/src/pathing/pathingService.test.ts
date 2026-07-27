import assert from 'node:assert/strict';
import test from 'node:test';
import { addPathExplanation, createPathingService } from './pathingService.js';
import type { PathingRepository } from './pathingRepository.js';

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

test('path history memory entries are bound to the current privacy generation', async () => {
  let generation = 1;
  let durableReads = 0;
  const repository = {
    fetchVisibilityGeneration: async () => generation,
    fetchPathHistory: async (_scope: string, requestedGeneration: number) => {
      durableReads += 1;
      return {
        window_start: '2026-01-01T00:00:00.000Z',
        updated_at: '2026-01-01T01:00:00.000Z',
        packet_count: requestedGeneration,
        resolved_packet_count: requestedGeneration,
        segment_counts: [],
        visibility_generation: requestedGeneration,
      };
    },
    fetchPathLearning: async () => {
      throw new Error('not used');
    },
  } as unknown as PathingRepository;
  const service = createPathingService({
    pathHistoryCache: new Map(),
    pathHistoryCacheTtlMs: 60_000,
    getResolveCache: () => undefined,
    setResolveCache: () => undefined,
    resolvePool: { run: async () => null },
    repository,
  });

  const first = await service.getPathHistory('ukmesh') as { packetCount: number };
  const repeated = await service.getPathHistory('ukmesh') as { packetCount: number };
  assert.equal(first.packetCount, 1);
  assert.equal(repeated.packetCount, 1);
  assert.equal(durableReads, 1);

  generation = 2;
  const afterOptOut = await service.getPathHistory('ukmesh') as { packetCount: number };
  assert.equal(afterOptOut.packetCount, 2);
  assert.equal(durableReads, 2);
});
