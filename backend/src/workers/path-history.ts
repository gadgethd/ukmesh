import 'node:process';
import {
  getPublicVisibilityGeneration,
  getRecentPathHistoryPacketHashes,
  initDb,
  refreshRecentPathEvidence,
  upsertPathHistoryCache,
  type PathHistorySegmentRow,
} from '../db/index.js';
import {
  createMultiObserverPathBatchResolver,
  type BetaResolvedPayload,
} from '../path-beta/resolver.js';
import { BoundedSegmentCounter } from '../analysis/boundedSegmentCounter.js';
import {
  AnalysisRunDeadlineExceededError,
  AnalysisRunAlreadyActiveError,
  analysisGeneration,
  beginAnalysisRun,
  checkpointAnalysisRun,
  finishAnalysisRun,
  getLatestTimedOutAnalysisCheckpoint,
  startAnalysisRunHeartbeat,
} from '../analysis/runState.js';
import { observeWorkerOutcome } from '../metrics.js';
import { startWorkerMetrics } from './workerMetrics.js';
import {
  pathHistoryNextDelayMs,
  pathHistoryRetryIntervalMs,
} from './pathHistorySchedule.js';
import {
  PATH_HISTORY_MODEL_GENERATION,
  pathHistoryBatches,
  pathHistorySelectionIdentity,
  resumablePathHistoryCheckpoint,
  type PathHistoryCheckpoint,
} from './pathHistoryCheckpoint.js';

const RETRY_INTERVAL_MS = pathHistoryRetryIntervalMs(process.env['PATH_HISTORY_RETRY_INTERVAL_MS']);
const WINDOW_HOURS = 168;
const MIN_SEGMENT_COUNT = 30;
const MAX_PACKET_HASHES = 12000;
const MAX_SEGMENTS = 3000;
const SEGMENT_COUNTER_CAPACITY = Math.max(
  MAX_SEGMENTS,
  Math.min(
    131_072,
    Math.trunc(Number(process.env['PATH_HISTORY_SEGMENT_COUNTER_CAPACITY'] ?? 65_536) || 65_536),
  ),
);
const MIN_HISTORY_PATH_HASH_BYTES = 2;
const RUN_DEADLINE_MS = Math.max(
  60_000,
  Number(process.env['PATH_HISTORY_RUN_DEADLINE_MS'] ?? 120 * 60_000) || 120 * 60_000,
);
const BATCH_SIZE = Math.max(
  8,
  Math.min(512, Math.trunc(Number(process.env['PATH_HISTORY_BATCH_SIZE'] ?? 64) || 64)),
);
const SCOPES = ['ukmesh', 'test'] as const;

type ScopeName = (typeof SCOPES)[number];

type SegmentCount = {
  positions: [[number, number], [number, number]];
  count: number;
};

function roundCoord(value: number): number {
  return Math.round(value * 1_000_000) / 1_000_000;
}

function normalizePoint(point: [number, number]): [number, number] {
  return [roundCoord(point[0]), roundCoord(point[1])];
}

function segmentKey(a: [number, number], b: [number, number]): string {
  const pa = normalizePoint(a);
  const pb = normalizePoint(b);
  const aKey = `${pa[0]},${pa[1]}`;
  const bKey = `${pb[0]},${pb[1]}`;
  return aKey <= bKey ? `${aKey}|${bKey}` : `${bKey}|${aKey}`;
}

function segmentPositions(key: string): [[number, number], [number, number]] {
  const [first, second] = key.split('|');
  const parsePoint = (value: string): [number, number] => {
    const [lat, lon] = value.split(',').map(Number);
    return [lat ?? 0, lon ?? 0];
  };
  return [parsePoint(first ?? '0,0'), parsePoint(second ?? '0,0')];
}

function collectPurpleSegments(result: BetaResolvedPayload, sink: Set<string>): void {
  const paths: Array<[number, number][]> = [];
  if (Array.isArray(result.purplePath) && result.purplePath.length >= 2) {
    paths.push(result.purplePath);
  }
  if (Array.isArray(result.extraPurplePaths)) {
    for (const path of result.extraPurplePaths) {
      if (Array.isArray(path) && path.length >= 2) paths.push(path);
    }
  }

  for (const path of paths) {
    for (let i = 0; i < path.length - 1; i += 1) {
      const a = path[i];
      const b = path[i + 1];
      if (!a || !b) continue;
      sink.add(segmentKey(a, b));
    }
  }
}

async function refreshScope(scope: ScopeName): Promise<'finished' | 'active-run' | 'retry'> {
  const visibilityGeneration = await getPublicVisibilityGeneration();
  const latestTimedOut = await getLatestTimedOutAnalysisCheckpoint('path-history', scope);
  const parsedCheckpointCandidate = resumablePathHistoryCheckpoint(latestTimedOut?.metadata, {
    scope,
    modelGeneration: PATH_HISTORY_MODEL_GENERATION,
    privacyGeneration: visibilityGeneration,
  });
  const checkpointCandidate = parsedCheckpointCandidate
    && latestTimedOut
    && Date.parse(latestTimedOut?.windowStart ?? '') === Date.parse(parsedCheckpointCandidate.windowStart)
    && Date.parse(latestTimedOut?.windowEnd ?? '') === Date.parse(parsedCheckpointCandidate.windowEnd)
    && latestTimedOut.modelGeneration === parsedCheckpointCandidate.modelGeneration
    && latestTimedOut.privacyGeneration === parsedCheckpointCandidate.privacyGeneration
    && latestTimedOut.checkpoint === parsedCheckpointCandidate.nextIndex
    ? parsedCheckpointCandidate
    : null;

  let windowEnd = checkpointCandidate
    ? new Date(checkpointCandidate.windowEnd)
    : new Date();
  let windowStart = checkpointCandidate
    ? new Date(checkpointCandidate.windowStart)
    : new Date(windowEnd.getTime() - WINDOW_HOURS * 60 * 60 * 1000);
  let packetHashes = await getRecentPathHistoryPacketHashes(
    windowStart,
    windowEnd,
    scope,
    MAX_PACKET_HASHES,
    MIN_HISTORY_PATH_HASH_BYTES,
  );
  let selectionIdentity = pathHistorySelectionIdentity({
    scope,
    windowStart,
    windowEnd,
    modelGeneration: PATH_HISTORY_MODEL_GENERATION,
    privacyGeneration: visibilityGeneration,
    packetHashes,
  });
  let resume = resumablePathHistoryCheckpoint(latestTimedOut?.metadata, {
    scope,
    modelGeneration: PATH_HISTORY_MODEL_GENERATION,
    privacyGeneration: visibilityGeneration,
    selectionIdentity,
    packetCount: packetHashes.length,
  });
  let counts: BoundedSegmentCounter;
  try {
    counts = resume
      ? BoundedSegmentCounter.fromSnapshot(SEGMENT_COUNTER_CAPACITY, resume.segmentCounter)
      : new BoundedSegmentCounter(SEGMENT_COUNTER_CAPACITY);
  } catch {
    resume = null;
    counts = new BoundedSegmentCounter(SEGMENT_COUNTER_CAPACITY);
  }

  if (checkpointCandidate && !resume) {
    // Late-arriving rows, a capacity change, or damaged metadata invalidates the
    // old fixed selection. Start a fresh current window rather than mixing it
    // with partial state from another generation.
    windowEnd = new Date();
    windowStart = new Date(windowEnd.getTime() - WINDOW_HOURS * 60 * 60 * 1000);
    packetHashes = await getRecentPathHistoryPacketHashes(
      windowStart,
      windowEnd,
      scope,
      MAX_PACKET_HASHES,
      MIN_HISTORY_PATH_HASH_BYTES,
    );
    selectionIdentity = pathHistorySelectionIdentity({
      scope,
      windowStart,
      windowEnd,
      modelGeneration: PATH_HISTORY_MODEL_GENERATION,
      privacyGeneration: visibilityGeneration,
      packetHashes,
    });
    counts = new BoundedSegmentCounter(SEGMENT_COUNTER_CAPACITY);
  }
  let nextIndex = resume?.nextIndex ?? 0;
  let resolvedPacketCount = resume?.resolvedPacketCount ?? 0;
  let skippedPacketCount = resume?.skippedPacketCount ?? 0;

  const makeCheckpoint = (): PathHistoryCheckpoint => ({
    version: 1,
    scope,
    windowStart: windowStart.toISOString(),
    windowEnd: windowEnd.toISOString(),
    modelGeneration: PATH_HISTORY_MODEL_GENERATION,
    privacyGeneration: visibilityGeneration,
    selectionIdentity,
    packetCount: packetHashes.length,
    nextIndex,
    resolvedPacketCount,
    skippedPacketCount,
    segmentCounter: counts.snapshot(),
  });
  let run;
  try {
    run = await beginAnalysisRun({
      workload: 'path-history',
      scope,
      windowStart,
      windowEnd,
      totalItems: packetHashes.length,
      deadlineMs: RUN_DEADLINE_MS,
      privacyGeneration: visibilityGeneration,
      modelGeneration: PATH_HISTORY_MODEL_GENERATION,
    });
  } catch (error) {
    if (error instanceof AnalysisRunAlreadyActiveError || (error as { code?: string }).code === '55P03') {
      console.warn(`[path-history] scope=${scope} refresh skipped; another analysis run is active`);
      return 'active-run';
    }
    throw error;
  }
  const stopHeartbeat = startAnalysisRunHeartbeat(run);
  const finish = async (
    input: Parameters<typeof finishAnalysisRun>[1],
  ): Promise<void> => {
    if (input.status === 'timed_out') await stopHeartbeat.stopForTerminal();
    else await stopHeartbeat();
    await finishAnalysisRun(run, input);
  };
  try {
    if (packetHashes.length === 0) {
      await finish({
        status: 'stale',
        checkpoint: 0,
        error: 'empty selection',
      });
      console.warn(`[path-history] scope=${scope} selected no packets; preserving last complete snapshot`);
      return 'finished';
    }
    const resolver = await createMultiObserverPathBatchResolver(
      scope,
      visibilityGeneration,
      stopHeartbeat.signal,
    );
    if (resume) {
      console.log(
        `[path-history] scope=${scope} resuming checkpoint=${nextIndex}/${packetHashes.length} `
          + `window=${windowStart.toISOString()}..${windowEnd.toISOString()}`,
      );
    }
    for (const batch of pathHistoryBatches(packetHashes, nextIndex, BATCH_SIZE)) {
      stopHeartbeat.assertOwned();
      stopHeartbeat.signal.throwIfAborted();
      // Exactly one visibility read fences each observation batch. The context
      // was preloaded against the same generation and publication rechecks it.
      const batchVisibilityGeneration = await getPublicVisibilityGeneration(stopHeartbeat.signal);
      if (batchVisibilityGeneration !== visibilityGeneration) {
        await finish({
          status: 'stale',
          checkpoint: nextIndex,
          error: 'public visibility changed during generation',
          metadata: { pathHistoryCheckpoint: makeCheckpoint() },
        });
        console.warn(
          `[path-history] scope=${scope} visibility changed during run; preserving the current snapshot`,
        );
        return 'finished';
      }
      const resolvedBatch = await resolver.resolveBatch(
        batch.items,
        windowStart,
        windowEnd,
        stopHeartbeat.signal,
      );
      stopHeartbeat.assertOwned();
      for (const packetHash of batch.items) {
        if (resolvedBatch.limitedPacketHashes.has(packetHash)) {
          skippedPacketCount += 1;
          continue;
        }
        const resolved = resolvedBatch.results.get(packetHash) ?? null;
        stopHeartbeat.assertOwned();
        if (!resolved?.ok || resolved.results.length < 1) {
          continue;
        }
        const packetSegments = new Set<string>();
        for (const result of resolved.results) collectPurpleSegments(result, packetSegments);
        if (packetSegments.size > 0) {
          resolvedPacketCount += 1;
          for (const key of packetSegments) counts.observe(key);
        }
      }
      nextIndex = batch.endIndex;
      await checkpointAnalysisRun(
        run,
        nextIndex,
        { pathHistoryCheckpoint: makeCheckpoint() },
        stopHeartbeat.signal,
      );
    }

    const segmentCounts: SegmentCount[] = counts.candidates(MIN_SEGMENT_COUNT)
      .map(({ key, count }) => ({
        positions: segmentPositions(key),
        count,
      }))
      .slice(0, MAX_SEGMENTS);

    const publicationVisibilityGeneration = await getPublicVisibilityGeneration();
    if (publicationVisibilityGeneration !== visibilityGeneration) {
      await finish({
        status: 'stale',
        checkpoint: nextIndex,
        error: 'public visibility changed before publication',
        metadata: { pathHistoryCheckpoint: makeCheckpoint() },
      });
      console.warn(
        `[path-history] scope=${scope} visibility changed before publication; preserving the current snapshot`,
      );
      return 'finished';
    }

    const published = await upsertPathHistoryCache({
      scope,
      windowStart,
      packetCount: packetHashes.length,
      resolvedPacketCount,
      segmentCounts: segmentCounts as PathHistorySegmentRow[],
      visibilityGeneration,
      analysisRun: run,
    });
    if (!published) {
      await finish({
        status: 'stale',
        checkpoint: nextIndex,
        error: 'public visibility changed during generation',
        metadata: { visibilityGeneration },
      });
      console.warn(
        `[path-history] scope=${scope} visibility changed during run; preserving the current snapshot`,
      );
      return 'finished';
    }
    const generation = analysisGeneration({
      scope,
      windowStart: windowStart.toISOString(),
      segmentCounts,
    });
    await finish({
      status: 'complete',
      checkpoint: nextIndex,
      generation,
      metadata: {
        skippedPacketCount,
        resolvedPacketCount,
        segmentCount: segmentCounts.length,
        trackedSegmentCount: counts.size(),
        segmentCounterReplacements: counts.replacementCount(),
      },
    });

    console.log(
      `[path-history] scope=${scope} run=${run.runId} packets=${packetHashes.length} skipped=${skippedPacketCount} resolved=${resolvedPacketCount} segments=${segmentCounts.length}`,
    );
    return 'finished';
  } catch (error) {
    try {
      const timedOut = Date.now() >= run.deadlineAt.getTime()
        || error instanceof AnalysisRunDeadlineExceededError
        || (error as Error).message === 'analysis run deadline exceeded';
      await finish({
        status: timedOut ? 'timed_out' : 'failed',
        checkpoint: nextIndex,
        error: error instanceof Error ? error.message : String(error),
        metadata: { pathHistoryCheckpoint: makeCheckpoint() },
      });
      if (timedOut) {
        console.warn(
          `[path-history] scope=${scope} timed out at checkpoint=${nextIndex}/${packetHashes.length}; retry will resume`,
        );
        return 'retry';
      }
    } catch (finishError) {
      console.error('[path-history] could not record failed run', (finishError as Error).message);
    }
    throw error;
  } finally {
    await stopHeartbeat().catch(() => {});
  }
}

let isRunning = false;

async function refreshAll(tag: 'initial' | 'scheduled'): Promise<boolean> {
  if (isRunning) {
    observeWorkerOutcome('path_history', 'refresh', 'skipped');
    console.warn(`[path-history] ${tag} refresh skipped; previous refresh still running`);
    return true;
  }
  isRunning = true;
  try {
    let publicEvidenceUpdates = 0;
    let testEvidenceUpdates = 0;
    try {
      publicEvidenceUpdates = await refreshRecentPathEvidence(1);
      testEvidenceUpdates = await refreshRecentPathEvidence(1, 'test');
      console.log(
        `[path-history] ${tag} path-evidence public=${publicEvidenceUpdates} test=${testEvidenceUpdates}`,
      );
    } catch (evidenceErr) {
      console.warn(`[path-history] ${tag} path-evidence skipped:`, (evidenceErr as Error).message);
    }
    let retrySoon = false;
    for (const scope of SCOPES) {
      const result = await refreshScope(scope);
      if (result === 'active-run' || result === 'retry') retrySoon = true;
    }
    observeWorkerOutcome('path_history', 'refresh', 'success');
    return retrySoon;
  } catch (err) {
    observeWorkerOutcome('path_history', 'refresh', 'failure');
    console.error(`[path-history] ${tag} refresh failed`, (err as Error).message);
    return true;
  } finally {
    isRunning = false;
  }
}

async function main() {
  startWorkerMetrics();
  await initDb();
  let consecutiveRetries = 0;
  const scheduleNext = (retrySoon: boolean) => {
    consecutiveRetries = retrySoon ? consecutiveRetries + 1 : 0;
    setTimeout(() => {
      void refreshAll('scheduled').then(scheduleNext);
    }, pathHistoryNextDelayMs(retrySoon, RETRY_INTERVAL_MS, consecutiveRetries));
  };
  scheduleNext(await refreshAll('initial'));
}

main().catch((err) => {
  console.error('[path-history] fatal startup error:', err);
  process.exit(1);
});
