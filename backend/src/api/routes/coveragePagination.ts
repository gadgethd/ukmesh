export type CoverageBounds = {
  minLon: number;
  minLat: number;
  maxLon: number;
  maxLat: number;
};

export const COVERAGE_DEFAULT_LIMIT = 12;
export const COVERAGE_MAX_LIMIT = 25;
export const COVERAGE_MAX_VIEWPORT_SPAN_DEGREES = 20;
export const COVERAGE_MAX_PAGE_BYTES = 5 * 1024 * 1024;

export function parseCoverageBounds(value: unknown): CoverageBounds | null {
  if (typeof value !== 'string') return null;
  const values = value.split(',').map((part) => Number(part.trim()));
  if (values.length !== 4 || values.some((part) => !Number.isFinite(part))) return null;

  const [minLon, minLat, maxLon, maxLat] = values as [number, number, number, number];
  if (minLon < -180 || maxLon > 180 || minLat < -90 || maxLat > 90) return null;
  if (minLon >= maxLon || minLat >= maxLat) return null;
  if (
    maxLon - minLon > COVERAGE_MAX_VIEWPORT_SPAN_DEGREES
    || maxLat - minLat > COVERAGE_MAX_VIEWPORT_SPAN_DEGREES
  ) return null;
  return { minLon, minLat, maxLon, maxLat };
}

export function parseCoverageLimit(value: unknown): number {
  if (typeof value !== 'string' || value.trim() === '') return COVERAGE_DEFAULT_LIMIT;
  const parsed = Number(value);
  if (!Number.isInteger(parsed)) return COVERAGE_DEFAULT_LIMIT;
  return Math.min(COVERAGE_MAX_LIMIT, Math.max(1, parsed));
}

export function parseCoverageCursor(value: unknown): string | null | undefined {
  if (value == null || value === '') return null;
  if (typeof value !== 'string') return undefined;
  const normalized = value.trim().toUpperCase();
  return /^[0-9A-F]{64}$/.test(normalized) ? normalized : undefined;
}

export function boundCoveragePage<T extends { node_id: string }>(
  rows: T[],
  requestedLimit: number,
  maxBytes = COVERAGE_MAX_PAGE_BYTES,
): { items: T[]; hasMore: boolean; nextCursor: string | null } {
  const candidates = rows.slice(0, requestedLimit);
  const items: T[] = [];
  let bytes = 2;

  for (const row of candidates) {
    const rowBytes = Buffer.byteLength(JSON.stringify(row), 'utf8') + (items.length > 0 ? 1 : 0);
    if (bytes + rowBytes > maxBytes) {
      if (items.length === 0) {
        // Advance the cursor without violating the page budget. The full
        // exceptional geometry remains available from /coverage/:nodeId.
        items.push({ node_id: row.node_id, truncated: true } as unknown as T);
      }
      break;
    }
    items.push(row);
    bytes += rowBytes;
  }

  const hasMore = items.length < rows.length;
  return {
    items,
    hasMore,
    nextCursor: hasMore && items.length > 0 ? items[items.length - 1]!.node_id : null,
  };
}
