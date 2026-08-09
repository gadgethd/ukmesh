import assert from 'node:assert/strict';
import test from 'node:test';
import { BoundedSegmentCounter } from '../analysis/boundedSegmentCounter.js';
import {
  PATH_HISTORY_MODEL_GENERATION,
  pathHistoryBatches,
  pathHistorySelectionIdentity,
  resumablePathHistoryCheckpoint,
  type PathHistoryCheckpoint,
} from './pathHistoryCheckpoint.js';

test('path history batching preserves selection order and resumes at a batch boundary', () => {
  const hashes = ['h0', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'];
  assert.deepEqual(pathHistoryBatches(hashes, 0, 3), [
    { startIndex: 0, endIndex: 3, items: ['h0', 'h1', 'h2'] },
    { startIndex: 3, endIndex: 6, items: ['h3', 'h4', 'h5'] },
    { startIndex: 6, endIndex: 7, items: ['h6'] },
  ]);
  assert.deepEqual(pathHistoryBatches(hashes, 3, 3).flatMap((batch) => batch.items), hashes.slice(3));
});

test('path history checkpoint resumes only for the identical fixed selection', () => {
  const windowStart = new Date('2026-08-01T00:00:00.000Z');
  const windowEnd = new Date('2026-08-08T00:00:00.000Z');
  const packetHashes = ['b', 'a', 'c'];
  const selectionIdentity = pathHistorySelectionIdentity({
    scope: 'ukmesh',
    windowStart,
    windowEnd,
    modelGeneration: PATH_HISTORY_MODEL_GENERATION,
    privacyGeneration: 7,
    packetHashes,
  });
  const counter = new BoundedSegmentCounter(8);
  counter.observe('1,2|3,4');
  const checkpoint: PathHistoryCheckpoint = {
    version: 1,
    scope: 'ukmesh',
    windowStart: windowStart.toISOString(),
    windowEnd: windowEnd.toISOString(),
    modelGeneration: PATH_HISTORY_MODEL_GENERATION,
    privacyGeneration: 7,
    selectionIdentity,
    packetCount: 3,
    nextIndex: 2,
    resolvedPacketCount: 1,
    skippedPacketCount: 0,
    segmentCounter: counter.snapshot(),
  };
  const metadata = { pathHistoryCheckpoint: checkpoint };
  assert.deepEqual(resumablePathHistoryCheckpoint(metadata, {
    scope: 'ukmesh',
    modelGeneration: PATH_HISTORY_MODEL_GENERATION,
    privacyGeneration: 7,
    selectionIdentity,
    packetCount: 3,
  }), checkpoint);
  assert.equal(resumablePathHistoryCheckpoint(metadata, {
    scope: 'ukmesh',
    modelGeneration: PATH_HISTORY_MODEL_GENERATION,
    privacyGeneration: 8,
  }), null);
  assert.equal(resumablePathHistoryCheckpoint(metadata, {
    scope: 'ukmesh',
    modelGeneration: PATH_HISTORY_MODEL_GENERATION,
    privacyGeneration: 7,
    selectionIdentity: pathHistorySelectionIdentity({
      scope: 'ukmesh',
      windowStart,
      windowEnd,
      modelGeneration: PATH_HISTORY_MODEL_GENERATION,
      privacyGeneration: 7,
      packetHashes: ['a', 'b', 'c'],
    }),
  }), null);
});
