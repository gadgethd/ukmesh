import assert from 'node:assert/strict';
import test from 'node:test';
import { toPublicBetaResultDto, toPublicMultiObserverDto } from './pathingPublicDto.js';

test('path DTO allowlists public fields and strips internal resolution state', () => {
  const projected = toPublicBetaResultDto({
    ok: true,
    packetHash: 'ABCD',
    mode: 'resolved',
    confidence: 0.8,
    permutationCount: 3,
    remainingHops: 0,
    purplePath: [[51, -1], [52, -2]],
    extraPurplePaths: [],
    redPath: null,
    redSegments: [],
    completionPaths: [],
    debug: { rxNodeId: 'PRIVATE' },
    stickyUpdates: { AA: 'PRIVATE' },
    rawCandidates: ['PRIVATE'],
  });
  assert.equal('debug' in projected, false);
  assert.equal('stickyUpdates' in projected, false);
  assert.equal('rawCandidates' in projected, false);
});

test('multi-observer DTO projects every nested result through the same allowlist', () => {
  const projected = toPublicMultiObserverDto({
    ok: true,
    packetHash: 'ABCD',
    observerCount: 1,
    sharedPrefixLength: 0,
    stickyUpdates: { AA: 'PRIVATE' },
    results: [{
      ok: true,
      packetHash: 'ABCD',
      mode: 'none',
      confidence: null,
      permutationCount: 0,
      remainingHops: null,
      purplePath: null,
      extraPurplePaths: [],
      redPath: null,
      redSegments: [],
      completionPaths: [],
      debug: { rxNodeId: 'PRIVATE' },
    }],
  });
  assert.equal('stickyUpdates' in projected, false);
  assert.equal('debug' in projected.results[0]!, false);
});
