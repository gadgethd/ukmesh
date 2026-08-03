import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildObserverBounds,
  decodePath,
  indexCandidatesByHash,
  type CandidateNode,
  type PathDecoderEvidence,
} from './decoder.js';
import {
  AMBIG_DELTA,
  DIST_DECAY_KM,
  MAX_COL,
  MAX_HOP_KM,
  ML_DOMINANT_THRESHOLD,
  NULL_BASELINE,
  SCORE,
} from '../path-shared/scoring.js';

const node = (nodeId: string, lat: number, lon: number): CandidateNode => ({
  nodeId,
  name: nodeId,
  lat,
  lon,
});

function evidence(
  candidatesByHash: ReadonlyMap<string, readonly CandidateNode[]>,
  overrides: Partial<PathDecoderEvidence> = {},
): PathDecoderEvidence {
  return {
    weights: {
      ...SCORE,
      prefix: 0.30,
      positionPrefixFrequency: 0,
      observerDistance: 0,
      anchor: 0.12,
      corridorPrior: 0,
      corridorInterpolation: 0,
      transition: 0.25,
      positionConditionalTransition: 0,
      itmViability: 0,
      dist: 0.10,
    },
    maxHopKm: MAX_HOP_KM,
    distanceDecayKm: DIST_DECAY_KM,
    mlDominantThreshold: ML_DOMINANT_THRESHOLD,
    unresolvedBaseline: NULL_BASELINE,
    ambiguityDelta: AMBIG_DELTA,
    maxColumnCandidates: MAX_COL,
    candidatesByHash,
    directAnchors: () => [],
    prefixProbability: () => 0,
    mlPrefixScore: () => 0,
    directedEdgeScore: () => 0,
    transitionProbability: () => 0,
    motifProbability: () => 0,
    hasObservedLink: () => false,
    ...overrides,
  };
}

test('decodes the strongest coherent chain from supplied evidence', () => {
  const candidates = new Map([
    ['AA', [node('A', 51, 0), node('B', 51, 0)]],
    ['BB', [node('C', 51.1, 0), node('D', 51.1, 0)]],
  ]);
  const decoded = decodePath(['AA', 'BB'], evidence(candidates, {
    prefixProbability: (nodeId) => ({ A: 0.2, B: 0.3, C: 0.2, D: 0.3 })[nodeId] ?? 0,
    transitionProbability: (from, to) => from === 'A' && to === 'C' ? 1 : 0,
    corridorInterpolation: () => { throw new Error('zero-weight corridor evidence was read'); },
    positionConditionalTransition: () => {
      throw new Error('zero-weight position-transition evidence was read');
    },
    isItmViable: () => { throw new Error('zero-weight ITM evidence was read'); },
  }));

  assert.equal(decoded.get(0)?.nodeId, 'A');
  assert.equal(decoded.get(1)?.nodeId, 'C');
});

test('chooses the real argmax even when the candidate has no prior support', () => {
  const decoded = decodePath(
    ['AA'],
    evidence(new Map([['AA', [node('A', 51, 0)]]])),
  );
  assert.equal(decoded.get(0)?.nodeId, 'A');
  assert.equal(decoded.get(0)?.ambiguous, false);
});

test('uses unresolved only when a column has no real candidates', () => {
  const decoded = decodePath(['AA', 'BB', 'CC'], evidence(new Map([
    ['AA', [node('A', 51, 0)]],
    ['CC', [node('C', 51.2, 0)]],
  ])));
  assert.equal(decoded.get(0)?.nodeId, 'A');
  assert.equal(decoded.get(1)?.nodeId, null);
  assert.equal(decoded.get(2)?.nodeId, 'C');
});

test('hard-gates candidates outside a direct observer anchor hop', () => {
  const decoded = decodePath(
    ['AA'],
    evidence(new Map([['AA', [node('near', 51, 1), node('far', 55, 1)]]]), {
      directAnchors: () => [{ lat: 51, lon: 1 }],
      prefixProbability: (nodeId) => nodeId === 'far' ? 1 : 0.2,
    }),
  );
  assert.equal(decoded.get(0)?.nodeId, 'near');
});

test('returns the marginal gap and flags a near-equal real candidate', () => {
  const decoded = decodePath(
    ['AA'],
    evidence(new Map([['AA', [node('A', 51, 0), node('B', 51, 0)]]]), {
      prefixProbability: (nodeId) => nodeId === 'A' ? 0.30 : 0.27,
    }),
  );
  const hop = decoded.get(0)!;
  assert.equal(hop.nodeId, 'A');
  const expectedMargin = 0.30 * (Math.log1p(0.30) - Math.log1p(0.27));
  assert.ok(Math.abs(hop.margin - expectedMargin) < 1e-12);
  assert.equal(hop.ambiguous, true);
});

test('builds observer bounds and filters unpositioned or remote candidates', () => {
  const bounds = buildObserverBounds([{ lat: 51, lon: 1 }], MAX_HOP_KM);
  const indexed = indexCandidatesByHash(['AA'], [
    { nodeId: 'AA-near', name: null, lat: 51.1, lon: 1 },
    { nodeId: 'AA-remote', name: null, lat: 60, lon: 1 },
    { nodeId: 'AA-zero', name: null, lat: 0, lon: 0 },
  ], bounds);

  assert.deepEqual(indexed.get('AA')?.map((candidate) => candidate.nodeId), ['AA-near']);
});
