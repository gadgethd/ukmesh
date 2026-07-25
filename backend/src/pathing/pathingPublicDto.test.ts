import assert from 'node:assert/strict';
import test from 'node:test';
import { toPublicBetaResultDto, toPublicMultiObserverDto } from './pathingPublicDto.js';

const single = {
  ok: true,
  packetHash: 'abc123',
  mode: 'resolved',
  confidence: 0.8,
  permutationCount: 2,
  remainingHops: 0,
  purplePath: [[54, -1], [55, -2]],
  extraPurplePaths: [],
  redPath: null,
  redSegments: [],
  completionPaths: [],
  stickyUpdates: { deadbeef: 'private-node-id' },
  internalDebug: 'must-not-escape',
  explanation: {
    evidenceLevel: 'high',
    summary: 'resolved',
    reasons: ['evidence'],
    alternativesConsidered: 2,
    secret: 'must-not-escape',
  },
};

test('public path projectors expose only allowlisted deep-copied fields', () => {
  const projected = toPublicBetaResultDto(single);
  assert.deepEqual(Object.keys(projected).sort(), [
    'completionPaths',
    'confidence',
    'explanation',
    'extraPurplePaths',
    'mode',
    'ok',
    'packetHash',
    'permutationCount',
    'purplePath',
    'redPath',
    'redSegments',
    'remainingHops',
  ]);
  assert.equal(JSON.stringify(projected).includes('private-node-id'), false);
  assert.notEqual(projected.purplePath, single.purplePath);
  assert.deepEqual(Object.keys(projected.explanation ?? {}).sort(), [
    'alternativesConsidered',
    'evidenceLevel',
    'reasons',
    'summary',
  ]);
});

test('multi-observer projector removes sticky and unknown nested fields', () => {
  const projected = toPublicMultiObserverDto({
    ok: true,
    packetHash: 'abc123',
    observerCount: 1,
    sharedPrefixLength: 2,
    results: [single],
    stickyUpdates: { aa: 'private-node-id' },
    regionLinks: [{ private: true }],
  });
  assert.deepEqual(Object.keys(projected).sort(), [
    'observerCount',
    'ok',
    'packetHash',
    'results',
    'sharedPrefixLength',
  ]);
  assert.equal(JSON.stringify(projected).includes('private-node-id'), false);
});

test('projector rejects inherited records and does not invoke getters', () => {
  let getterCalls = 0;
  const inherited = Object.create({ packetHash: 'abc123' }) as Record<string, unknown>;
  Object.defineProperty(inherited, 'mode', {
    enumerable: true,
    get() {
      getterCalls += 1;
      return 'resolved';
    },
  });
  assert.throws(() => toPublicBetaResultDto(inherited), /INVALID_PATH_RESULT/);
  assert.equal(getterCalls, 0);
});
