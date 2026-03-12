import 'node:process';
import { getRecentPathHistoryPacketHashes, initDb, upsertPathHistoryCache, type PathHistorySegmentRow } from '../db/index.js';
import { resolveMultiObserverBetaPath, type BetaResolvedPayload } from '../path-beta/resolver.js';

const REFRESH_INTERVAL_MS = 5 * 60 * 1000;
const WINDOW_HOURS = 1;
const MAX_PACKET_HASHES = 1200;
const MAX_SEGMENTS = 1200;
const SCOPES = ['all', 'teesside', 'ukmesh', 'test'] as const;

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
  const packetHashes = await getRecentPathHistoryPacketHashes(WINDOW_HOURS, scope === 'all' ? undefined : scope, MAX_PACKET_HASHES);
  const counts = new Map<string, number>();
  let resolvedPacketCount = 0;

  for (const packetHash of packetHashes) {
    const resolved = await resolveMultiObserverBetaPath(packetHash, scope);
    if (!resolved?.ok || resolved.results.length < 1) continue;

    const packetSegments = new Set<string>();
    for (const result of resolved.results) {
      collectPurpleSegments(result, packetSegments);
    }
    if (packetSegments.size < 1) continue;

    resolvedPacketCount += 1;
    for (const key of packetSegments) {
      counts.set(key, (counts.get(key) ?? 0) + 1);
    }
  }

  const segmentCounts: SegmentCount[] = Array.from(counts.entries())
    .map(([key, count]) => ({
      positions: segmentPositions(key),
      count,
    }))
    .sort((a, b) => b.count - a.count)
    .slice(0, MAX_SEGMENTS);

  await upsertPathHistoryCache({
    scope,
    windowStart: new Date(Date.now() - WINDOW_HOURS * 60 * 60 * 1000),
    packetCount: packetHashes.length,
    resolvedPacketCount,
    segmentCounts: segmentCounts as PathHistorySegmentRow[],
  });

  console.log(
    `[path-history] scope=${scope} packets=${packetHashes.length} resolved=${resolvedPacketCount} segments=${segmentCounts.length}`,
  );
}

let isRunning = false;

async function refreshAll(tag: 'initial' | 'scheduled') {
  if (isRunning) {
    console.warn(`[path-history] ${tag} refresh skipped; previous refresh still running`);
    return;
  }
  isRunning = true;
  try {
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
