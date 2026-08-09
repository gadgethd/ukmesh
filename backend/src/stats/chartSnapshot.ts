import type { QueryResultRow } from 'pg';

export const CHART_SNAPSHOT_SCHEMA_VERSION = 2;
export const CHART_SNAPSHOT_MAX_BYTES = 2 * 1024 * 1024;
export const CHART_SNAPSHOT_MAX_FUTURE_SKEW_MS = 60_000;

const CHART_SNAPSHOT_SCOPE_PATTERN = /^[a-z0-9][a-z0-9_-]{0,63}$/;

export type ChartSnapshotQuery = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

export type StoredChartSnapshotRow = {
  scope_key: string;
  schema_version: number;
  visibility_generation: string | number;
  generated_at: string | Date;
  payload: unknown;
};

export type ValidChartSnapshot = {
  generatedAtMs: number;
  payload: Record<string, unknown>;
};

export function assertChartSnapshotScope(scope: string): void {
  if (!CHART_SNAPSHOT_SCOPE_PATTERN.test(scope)) {
    throw new Error('chart snapshot scope is invalid');
  }
}

export function validateChartSnapshotPayload(
  payload: unknown,
  expectedScope: string,
  maxAgeMs: number,
  nowMs = Date.now(),
  expectedVisibilityGeneration?: number,
  options: { allowExpired?: boolean } = {},
): ValidChartSnapshot | null {
  if (
    !payload
    || typeof payload !== 'object'
    || Array.isArray(payload)
    || !Number.isFinite(maxAgeMs)
    || maxAgeMs < 1
  ) {
    return null;
  }
  const record = payload as Record<string, unknown>;
  const snapshot = record['snapshot'];
  if (!snapshot || typeof snapshot !== 'object' || Array.isArray(snapshot)) return null;
  const metadata = snapshot as Record<string, unknown>;
  if (metadata['status'] !== 'complete' || metadata['scope'] !== expectedScope) return null;
  if (
    expectedVisibilityGeneration !== undefined
    && (
      !Number.isSafeInteger(expectedVisibilityGeneration)
      || expectedVisibilityGeneration < 1
      || metadata['visibilityGeneration'] !== expectedVisibilityGeneration
    )
  ) {
    return null;
  }
  const generatedAtMs = Date.parse(String(metadata['generatedAt'] ?? ''));
  if (
    !Number.isFinite(generatedAtMs)
    || generatedAtMs > nowMs + CHART_SNAPSHOT_MAX_FUTURE_SKEW_MS
    || (!options.allowExpired && nowMs - generatedAtMs > maxAgeMs)
  ) {
    return null;
  }
  let encoded: string;
  try {
    encoded = JSON.stringify(record);
  } catch {
    return null;
  }
  if (Buffer.byteLength(encoded, 'utf8') > CHART_SNAPSHOT_MAX_BYTES) return null;
  return { generatedAtMs, payload: record };
}

export async function loadStoredChartSnapshot(
  query: ChartSnapshotQuery,
  scope: string,
  visibilityGeneration: number,
): Promise<StoredChartSnapshotRow | null> {
  assertChartSnapshotScope(scope);
  if (!Number.isSafeInteger(visibilityGeneration) || visibilityGeneration < 1) {
    throw new Error('chart snapshot visibility generation is invalid');
  }
  const result = await query<StoredChartSnapshotRow>(
    `SELECT scope_key, schema_version, visibility_generation, generated_at, payload
      FROM stats_chart_snapshots
      WHERE scope_key = $1
        AND schema_version = $2
      LIMIT 1`,
    [scope, CHART_SNAPSHOT_SCHEMA_VERSION],
  );
  return result.rows[0] ?? null;
}

export async function saveStoredChartSnapshot(
  query: ChartSnapshotQuery,
  scope: string,
  payload: unknown,
  visibilityGeneration: number,
  maxAgeMs: number,
  nowMs = Date.now(),
): Promise<boolean> {
  assertChartSnapshotScope(scope);
  if (!Number.isSafeInteger(visibilityGeneration) || visibilityGeneration < 1) {
    throw new Error('chart snapshot visibility generation is invalid');
  }
  const validated = validateChartSnapshotPayload(
    payload,
    scope,
    maxAgeMs,
    nowMs,
    visibilityGeneration,
  );
  if (!validated) throw new Error('chart snapshot payload is invalid');
  const encoded = JSON.stringify(validated.payload);
  const result = await query<{ scope_key: string }>(
    `INSERT INTO stats_chart_snapshots
       (scope_key, schema_version, visibility_generation, generated_at, payload, updated_at)
     SELECT $1, $2, $3, $4::timestamptz, $5::jsonb, NOW()
       FROM public_visibility_state
      WHERE singleton = TRUE
        AND generation = $3
     ON CONFLICT (scope_key) DO UPDATE SET
       schema_version = EXCLUDED.schema_version,
       visibility_generation = EXCLUDED.visibility_generation,
       generated_at = EXCLUDED.generated_at,
       payload = EXCLUDED.payload,
       updated_at = NOW()
     WHERE stats_chart_snapshots.visibility_generation < EXCLUDED.visibility_generation
        OR (
          stats_chart_snapshots.visibility_generation = EXCLUDED.visibility_generation
          AND stats_chart_snapshots.generated_at <= EXCLUDED.generated_at
        )
     RETURNING scope_key`,
    [
      scope,
      CHART_SNAPSHOT_SCHEMA_VERSION,
      visibilityGeneration,
      new Date(validated.generatedAtMs).toISOString(),
      encoded,
    ],
  );
  return result.rows.length === 1;
}
