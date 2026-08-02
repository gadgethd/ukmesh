import { pool, query } from '../db/index.js';
import {
  CHART_SNAPSHOT_MAX_BYTES,
  saveStoredChartSnapshot,
  validateChartSnapshotPayload,
} from '../stats/chartSnapshot.js';

const DEFAULT_MAX_AGE_MS = 6 * 60 * 60_000;

function argValue(flag: string): string | undefined {
  const index = process.argv.indexOf(flag);
  return index >= 0 ? process.argv[index + 1] : undefined;
}

async function readBoundedStdin(): Promise<string> {
  const chunks: Buffer[] = [];
  let totalBytes = 0;
  for await (const value of process.stdin) {
    const chunk = Buffer.isBuffer(value) ? value : Buffer.from(String(value));
    totalBytes += chunk.byteLength;
    if (totalBytes > CHART_SNAPSHOT_MAX_BYTES) {
      throw new Error(`snapshot input exceeds ${CHART_SNAPSHOT_MAX_BYTES} bytes`);
    }
    chunks.push(chunk);
  }
  if (totalBytes < 2) throw new Error('snapshot JSON is required on stdin');
  return Buffer.concat(chunks).toString('utf8');
}

async function main(): Promise<void> {
  const scope = argValue('--network');
  if (!scope) throw new Error('--network is required');
  const raw = await readBoundedStdin();
  let payload: unknown;
  try {
    payload = JSON.parse(raw);
  } catch {
    throw new Error('snapshot input is not valid JSON');
  }
  const source = validateChartSnapshotPayload(payload, scope, DEFAULT_MAX_AGE_MS);
  if (!source) {
    throw new Error('snapshot is incomplete, stale, future-dated, oversized, or for another scope');
  }
  const visibilityResult = await query<{ generation: string; updated_at: string }>(
    `SELECT generation::text, updated_at::text
       FROM public_visibility_state
      WHERE singleton = TRUE`,
  );
  const visibilityGeneration = Number(visibilityResult.rows[0]?.generation);
  const visibilityUpdatedAtMs = Date.parse(visibilityResult.rows[0]?.updated_at ?? '');
  if (
    !Number.isSafeInteger(visibilityGeneration)
    || visibilityGeneration < 1
    || !Number.isFinite(visibilityUpdatedAtMs)
    || source.generatedAtMs < visibilityUpdatedAtMs
  ) {
    throw new Error('snapshot predates the current public visibility generation');
  }
  const snapshot = source.payload['snapshot'] as Record<string, unknown>;
  const versionedPayload = {
    ...source.payload,
    snapshot: { ...snapshot, visibilityGeneration },
  };
  const validated = validateChartSnapshotPayload(
    versionedPayload,
    scope,
    DEFAULT_MAX_AGE_MS,
    Date.now(),
    visibilityGeneration,
  );
  if (!validated) throw new Error('snapshot failed visibility-generation binding');
  const published = await saveStoredChartSnapshot(
    query,
    scope,
    validated.payload,
    visibilityGeneration,
    DEFAULT_MAX_AGE_MS,
  );
  if (!published) throw new Error('public visibility generation changed during seed');
  console.log(JSON.stringify({
    seeded: true,
    scope,
    visibilityGeneration,
    generatedAt: new Date(validated.generatedAtMs).toISOString(),
    bytes: Buffer.byteLength(raw, 'utf8'),
  }));
}

main()
  .catch((error: unknown) => {
    console.error(
      '[stats-chart-snapshot-seed] failed:',
      error instanceof Error ? error.message : 'unknown error',
    );
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
