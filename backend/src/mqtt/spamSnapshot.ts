import type { DatabaseQueryFn } from '../db/index.js';

export const SPAM_SNAPSHOT_MAX_ROWS = 100_000;
export const SPAM_SNAPSHOT_MAX_BYTES = 16 * 1024 * 1024;

/** Reject an incomplete snapshot before its rows cross the database connection. */
export async function readSpamSnapshot<T>(
  query: DatabaseQueryFn,
  sql: string,
  params: unknown[] = [],
  limits = { rows: SPAM_SNAPSHOT_MAX_ROWS, bytes: SPAM_SNAPSHOT_MAX_BYTES },
): Promise<T[]> {
  if (!Number.isSafeInteger(limits.rows) || limits.rows < 1
    || !Number.isSafeInteger(limits.bytes) || limits.bytes < 1) {
    throw new Error('invalid spam snapshot limits');
  }
  const limitIndex = params.length + 1;
  const result = await query<{ row_count: string; byte_count: string; snapshot: T[] | null }>(`
    WITH spam_candidates AS MATERIALIZED (
      ${sql}
      LIMIT $${limitIndex}
    ), spam_budget AS (
      SELECT COUNT(*) AS row_count,
        COALESCE(SUM(octet_length(row_to_json(candidate)::text)), 0) AS byte_count
      FROM spam_candidates candidate
    )
    SELECT row_count, byte_count,
      CASE WHEN row_count < $${limitIndex} AND byte_count <= $${limitIndex + 1}
        THEN (SELECT COALESCE(json_agg(candidate), '[]'::json) FROM spam_candidates candidate)
        ELSE NULL END AS snapshot
    FROM spam_budget
  `, [...params, limits.rows + 1, limits.bytes]);
  const row = result.rows[0];
  if (!row || row.snapshot === null) throw new Error('spam snapshot exceeds row or byte capacity');
  if (!Array.isArray(row.snapshot)) throw new Error('invalid spam snapshot result');
  return row.snapshot;
}
