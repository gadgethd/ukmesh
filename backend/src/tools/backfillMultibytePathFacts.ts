import { getPublicVisibilityGeneration, pool, query } from '../db/index.js';
import {
  backfillMultibyteObservationIds,
  backfillMultibytePathFacts,
  listMultibyteFactChunks,
  selectMultibyteFactChunkBatch,
  setMultibyteFactChunkCompression,
} from '../stats/multibytePathFacts.js';

function boundedDays(raw: string | undefined): number {
  const value = Number(raw ?? 8);
  if (!Number.isFinite(value)) return 8;
  return Math.max(7, Math.min(31, Math.trunc(value)));
}

function boundedBatchSize(raw: string | undefined): number {
  const value = Number(raw ?? 500);
  if (!Number.isFinite(value)) return 500;
  return Math.max(50, Math.min(10_000, Math.trunc(value)));
}

function boundedThrottleMs(raw: string | undefined): number {
  const value = Number(raw ?? 250);
  if (!Number.isFinite(value)) return 250;
  return Math.max(0, Math.min(5_000, Math.trunc(value)));
}

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function boundedChunkIndex(raw: string | undefined): number {
  const value = Number(raw ?? 0);
  if (!Number.isSafeInteger(value) || value < 0 || value > 10_000) {
    throw new Error('INVALID_MULTIBYTE_FACT_CHUNK_INDEX');
  }
  return value;
}

function boundedChunkLimit(raw: string | undefined): number {
  const value = Number(raw ?? 1);
  if (!Number.isSafeInteger(value) || value < 1 || value > 4) {
    throw new Error('INVALID_MULTIBYTE_FACT_CHUNK_LIMIT');
  }
  return value;
}

async function main(): Promise<void> {
  const cutoff = process.env['MULTIBYTE_FACTS_AS_OF']
    ? new Date(process.env['MULTIBYTE_FACTS_AS_OF'])
    : new Date();
  if (!Number.isFinite(cutoff.getTime())) throw new Error('INVALID_MULTIBYTE_FACTS_AS_OF');
  const days = boundedDays(process.env['MULTIBYTE_FACTS_DAYS']);
  const windowStart = new Date(cutoff.getTime() - days * 24 * 60 * 60_000);
  const batchSize = boundedBatchSize(process.env['MULTIBYTE_FACTS_ID_BATCH_SIZE']);
  const throttleMs = boundedThrottleMs(process.env['MULTIBYTE_FACTS_ID_THROTTLE_MS']);
  const chunkStartIndex = boundedChunkIndex(process.env['MULTIBYTE_FACTS_CHUNK_INDEX']);
  const chunkLimit = boundedChunkLimit(process.env['MULTIBYTE_FACTS_CHUNK_LIMIT']);
  const visibilityGeneration = await getPublicVisibilityGeneration();
  const chunks = await listMultibyteFactChunks(query, { windowStart, cutoff });
  const selectedChunks = selectMultibyteFactChunkBatch(chunks, chunkStartIndex, chunkLimit);
  let observationIdsBackfilled = 0;
  let factsBackfilled = 0;
  let batch = 0;
  for (const [selectionIndex, chunk] of selectedChunks.entries()) {
    const chunkIndex = chunkStartIndex + selectionIndex;
    if (chunk.rangeEnd < chunk.rangeStart) continue;
    if (chunk.wasCompressed) {
      console.log(JSON.stringify({
        status: 'chunk-decompressing',
        chunk: `${chunk.chunkSchema}.${chunk.chunkName}`,
      }));
      await setMultibyteFactChunkCompression(query, chunk, false);
    }
    try {
      while (true) {
        const affectedRows = await backfillMultibyteObservationIds(query, {
          windowStart: chunk.rangeStart,
          cutoff: chunk.rangeEnd,
          batchSize,
        });
        if (affectedRows === 0) break;
        batch += 1;
        observationIdsBackfilled += affectedRows;
        console.log(JSON.stringify({
          status: 'observation-id-batch',
          chunk: `${chunk.chunkSchema}.${chunk.chunkName}`,
          chunkIndex: chunkIndex + 1,
          chunkCount: chunks.length,
          batch,
          affectedRows,
          totalAffectedRows: observationIdsBackfilled,
        }));
        if (throttleMs > 0) await wait(throttleMs);
      }
      const result = await backfillMultibytePathFacts(query, {
        windowStart: chunk.rangeStart,
        cutoff: chunk.rangeEnd,
        visibilityGeneration,
      });
      factsBackfilled += result.affectedRows;
    } finally {
      if (chunk.wasCompressed) {
        console.log(JSON.stringify({
          status: 'chunk-recompressing',
          chunk: `${chunk.chunkSchema}.${chunk.chunkName}`,
        }));
        await setMultibyteFactChunkCompression(query, chunk, true);
      }
    }
  }
  const confirmedGeneration = await getPublicVisibilityGeneration();
  if (confirmedGeneration !== visibilityGeneration) {
    throw new Error(
      `PUBLIC_VISIBILITY_CHANGED_DURING_MULTIBYTE_BACKFILL:${visibilityGeneration}:${confirmedGeneration}`,
    );
  }
  console.log(JSON.stringify({
    status: 'complete',
    visibilityGeneration,
    windowStart: windowStart.toISOString(),
    cutoff: cutoff.toISOString(),
    observationIdsBackfilled,
    observationIdBatches: batch,
    chunkStartIndex,
    chunksProcessed: selectedChunks.length,
    chunksAvailable: chunks.length,
    chunksRemaining: Math.max(0, chunks.length - chunkStartIndex - selectedChunks.length),
    affectedRows: factsBackfilled,
  }));
}

main()
  .catch((error) => {
    console.error('[multibyte-facts] backfill failed', error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
