import { createHash } from 'node:crypto';
import type { BoundedSegmentCounterSnapshot } from '../analysis/boundedSegmentCounter.js';

export const PATH_HISTORY_MODEL_GENERATION = 'path-history-v2';

export type PathHistoryCheckpoint = {
  version: 1;
  scope: string;
  windowStart: string;
  windowEnd: string;
  modelGeneration: string;
  privacyGeneration: number;
  selectionIdentity: string;
  packetCount: number;
  nextIndex: number;
  resolvedPacketCount: number;
  skippedPacketCount: number;
  segmentCounter: BoundedSegmentCounterSnapshot;
};

export function pathHistorySelectionIdentity(input: {
  scope: string;
  windowStart: Date;
  windowEnd: Date;
  modelGeneration: string;
  privacyGeneration: number;
  packetHashes: readonly string[];
}): string {
  return createHash('sha256').update(JSON.stringify({
    scope: input.scope,
    windowStart: input.windowStart.toISOString(),
    windowEnd: input.windowEnd.toISOString(),
    modelGeneration: input.modelGeneration,
    privacyGeneration: input.privacyGeneration,
    packetHashes: input.packetHashes,
  })).digest('hex');
}

function integerAtLeast(value: unknown, minimum: number): value is number {
  return Number.isSafeInteger(value) && Number(value) >= minimum;
}

export function resumablePathHistoryCheckpoint(
  metadata: unknown,
  expected: {
    scope: string;
    modelGeneration: string;
    privacyGeneration: number;
    selectionIdentity?: string;
    packetCount?: number;
  },
): PathHistoryCheckpoint | null {
  if (!metadata || typeof metadata !== 'object') return null;
  const value = (metadata as { pathHistoryCheckpoint?: unknown }).pathHistoryCheckpoint;
  if (!value || typeof value !== 'object') return null;
  const checkpoint = value as Partial<PathHistoryCheckpoint>;
  if (
    checkpoint.version !== 1
    || checkpoint.scope !== expected.scope
    || checkpoint.modelGeneration !== expected.modelGeneration
    || checkpoint.privacyGeneration !== expected.privacyGeneration
    || typeof checkpoint.windowStart !== 'string'
    || !Number.isFinite(Date.parse(checkpoint.windowStart))
    || typeof checkpoint.windowEnd !== 'string'
    || !Number.isFinite(Date.parse(checkpoint.windowEnd))
    || Date.parse(checkpoint.windowStart) > Date.parse(checkpoint.windowEnd)
    || typeof checkpoint.selectionIdentity !== 'string'
    || !/^[a-f0-9]{64}$/.test(checkpoint.selectionIdentity)
    || !integerAtLeast(checkpoint.packetCount, 1)
    || !integerAtLeast(checkpoint.nextIndex, 0)
    || checkpoint.nextIndex > checkpoint.packetCount
    || !integerAtLeast(checkpoint.resolvedPacketCount, 0)
    || checkpoint.resolvedPacketCount > checkpoint.nextIndex
    || !integerAtLeast(checkpoint.skippedPacketCount, 0)
    || checkpoint.skippedPacketCount > checkpoint.nextIndex
    || !checkpoint.segmentCounter
    || typeof checkpoint.segmentCounter !== 'object'
  ) return null;
  if (
    expected.selectionIdentity !== undefined
    && checkpoint.selectionIdentity !== expected.selectionIdentity
  ) return null;
  if (expected.packetCount !== undefined && checkpoint.packetCount !== expected.packetCount) return null;
  return checkpoint as PathHistoryCheckpoint;
}

export function pathHistoryBatches<T>(
  items: readonly T[],
  startIndex: number,
  batchSize: number,
): Array<{ startIndex: number; endIndex: number; items: T[] }> {
  if (!Number.isSafeInteger(startIndex) || startIndex < 0 || startIndex > items.length) {
    throw new Error('INVALID_PATH_HISTORY_BATCH_START');
  }
  if (!Number.isSafeInteger(batchSize) || batchSize < 1) {
    throw new Error('INVALID_PATH_HISTORY_BATCH_SIZE');
  }
  const batches: Array<{ startIndex: number; endIndex: number; items: T[] }> = [];
  for (let index = startIndex; index < items.length; index += batchSize) {
    const endIndex = Math.min(items.length, index + batchSize);
    batches.push({ startIndex: index, endIndex, items: items.slice(index, endIndex) });
  }
  return batches;
}
