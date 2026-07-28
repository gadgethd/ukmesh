import 'node:process';
import {
  getPublicVisibilityGeneration,
  getRecentPathHistoryPacketHashes,
  initDb,
  refreshRecentPathEvidence,
  upsertPathHistoryCache,
  type PathHistorySegmentRow,
} from '../db/index.js';
import { resolveMultiObserverBetaPath, type BetaResolvedPayload } from '../path-beta/resolver.js';
import { runBoundedItems } from '../analysis/boundedRun.js';
import { BoundedSegmentCounter } from '../analysis/boundedSegmentCounter.js';
import {
  AnalysisRunAlreadyActiveError,
  analysisGeneration,
  beginAnalysisRun,
  finishAnalysisRun,
} from '../analysis/runState.js';

const REFRESH_INTERVAL_MS = 60 * 60 * 1000; // 1 hour — 7-day window changes slowly
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
const CONCURRENCY = Math.max(
  1,
  Math.min(16, Math.trunc(Number(process.env['PATH_HISTORY_CONCURRENCY'] ?? 2) || 2)),
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

async function refreshScope(scope: ScopeName): Promise<void> {
  const visibilityGeneration = await getPublicVisibilityGeneration();
  const packetHashes = await getRecentPathHistoryPacketHashes(
    WINDOW_HOURS,
    scope,
    MAX_PACKET_HASHES,
    MIN_HISTORY_PATH_HASH_BYTES,
  );
  const counts = new BoundedSegmentCounter(SEGMENT_COUNTER_CAPACITY);
  let resolvedPacketCount = 0;
  let skippedPacketCount = 0;

  const windowEnd = new Date();
  const windowStart = new Date(windowEnd.getTime() - WINDOW_HOURS * 60 * 60 * 1000);
  let run;
  try {
    run = await beginAnalysisRun({
      workload: 'path-history',
      scope,
      windowStart,
      windowEnd,
      totalItems: packetHashes.length,
    });
  } catch (error) {
    if (error instanceof AnalysisRunAlreadyActiveError || (error as { code?: string }).code === '55P03') {
      console.warn(`[path-history] scope=${scope} refresh skipped; another analysis run is active`);
      return;
    }
    throw error;
  }
  try {
    if (packetHashes.length === 0) {
      await finishAnalysisRun(run, {
        status: 'stale',
        checkpoint: 0,
        error: 'empty selection',
      });
      console.warn(`[path-history] scope=${scope} selected no packets; preserving last complete snapshot`);
      return;
    }
    const outcome = await runBoundedItems(packetHashes, async (packetHash) => {
      try {
        const resolved = await resolveMultiObserverBetaPath(packetHash, scope, undefined, undefined, {
          touchPredictedOnline: false,
          log: false,
          pinContextForBatch: true,
          requiredVisibilityGeneration: visibilityGeneration,
        });
        if (!resolved?.ok || resolved.results.length < 1) {
          return;
        }
        const packetSegments = new Set<string>();
        for (const result of resolved.results) collectPurpleSegments(result, packetSegments);
        if (packetSegments.size > 0) {
          resolvedPacketCount += 1;
          for (const key of packetSegments) counts.observe(key);
        }
        return;
      } catch (error) {
        if ((error as Error).message === 'PATH_HISTORY_LIMIT') {
          skippedPacketCount += 1;
          return;
        }
        throw error;
      }
    }, {
      windowStart,
      windowEnd,
      deadlineMs: RUN_DEADLINE_MS,
      concurrency: CONCURRENCY,
      collectResults: false,
      maxErrors: 100,
      runId: run.runId,
    });
    if (outcome.status !== 'complete') {
      await finishAnalysisRun(run, {
        status: outcome.status,
        checkpoint: outcome.checkpoint,
        error: outcome.errors[0]?.message,
        metadata: { errors: outcome.errors.slice(0, 20) },
      });
      console.error('[path-history] incomplete generation; preserving last complete snapshot', {
        scope,
        runId: outcome.runId,
        status: outcome.status,
        checkpoint: outcome.checkpoint,
        errors: outcome.errors.slice(0, 5),
      });
      return;
    }

    const segmentCounts: SegmentCount[] = counts.candidates(MIN_SEGMENT_COUNT)
      .map(({ key, count }) => ({
        positions: segmentPositions(key),
        count,
      }))
      .slice(0, MAX_SEGMENTS);

    const published = await upsertPathHistoryCache({
      scope,
      windowStart,
      packetCount: packetHashes.length,
      resolvedPacketCount,
      segmentCounts: segmentCounts as PathHistorySegmentRow[],
      visibilityGeneration,
    });
    if (!published) {
      await finishAnalysisRun(run, {
        status: 'stale',
        checkpoint: outcome.checkpoint,
        error: 'public visibility changed during generation',
        metadata: { visibilityGeneration },
      });
      console.warn(
        `[path-history] scope=${scope} visibility changed during run; preserving the current snapshot`,
      );
      return;
    }
    const generation = analysisGeneration({
      scope,
      windowStart: windowStart.toISOString(),
      segmentCounts,
    });
    await finishAnalysisRun(run, {
      status: 'complete',
      checkpoint: outcome.checkpoint,
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
      `[path-history] scope=${scope} run=${outcome.runId} packets=${packetHashes.length} skipped=${skippedPacketCount} resolved=${resolvedPacketCount} segments=${segmentCounts.length}`,
    );
  } catch (error) {
    try {
      await finishAnalysisRun(run, {
        status: 'failed',
        checkpoint: 0,
        error: error instanceof Error ? error.message : String(error),
      });
    } catch (finishError) {
      console.error('[path-history] could not record failed run', (finishError as Error).message);
    }
    throw error;
  }
}

let isRunning = false;

async function refreshAll(tag: 'initial' | 'scheduled') {
  if (isRunning) {
    console.warn(`[path-history] ${tag} refresh skipped; previous refresh still running`);
    return;
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
    for (const scope of SCOPES) {
      await refreshScope(scope);
    }
  } catch (err) {
    console.error(`[path-history] ${tag} refresh failed`, (err as Error).message);
  } finally {
    isRunning = false;
  }
}

async function main() {
  await initDb();
  await refreshAll('initial');

  setInterval(() => {
    void refreshAll('scheduled');
  }, REFRESH_INTERVAL_MS);
}

main().catch((err) => {
  console.error('[path-history] fatal startup error:', err);
  process.exit(1);
});
