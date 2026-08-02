export const PUBLIC_MAP_ALLOWED_FIELDS = [
  'node_id',
  'name',
  'lat',
  'lon',
  'role',
  'iata',
  'last_seen',
  'is_online',
  'hardware_model',
  'firmware_version',
  'advert_count',
  'elevation_m',
] as const;

export type PublicMapField = typeof PUBLIC_MAP_ALLOWED_FIELDS[number];

export const PUBLIC_MAP_DEFAULT_PAGE_ROWS = 1000;
export const PUBLIC_MAP_MAX_PAGE_ROWS = 2000;
export const PUBLIC_MAP_MAX_PAGE_BYTES = 1_500_000;
export const PUBLIC_MAP_SNAPSHOT_MAX_AGE_MS = 30 * 60_000;

export class PublicMapInputError extends Error {}

function safeAlias(alias: string): string {
  if (!/^[a-z][a-z0-9_]*$/i.test(alias)) {
    throw new Error(`invalid SQL alias: ${alias}`);
  }
  return alias;
}

export function publicMapBasePredicate(alias = 'n'): string {
  const table = safeAlias(alias);
  return `(
    ${table}.lat BETWEEN -90 AND 90
    AND ${table}.lon BETWEEN -180 AND 180
    AND NOT (ABS(${table}.lat) < 5 AND ABS(${table}.lon) < 5)
    AND (${table}.name IS NULL OR ${table}.name NOT LIKE '%🚫%')
    AND (${table}.role IS NULL OR ${table}.role NOT IN (1, 3))
  )`;
}

export function publicMapFreshPredicate(
  alias = 'n',
  referenceSql = 'NOW()',
): string {
  const table = safeAlias(alias);
  return `(
    ${publicMapBasePredicate(table)}
    AND GREATEST(${table}.last_seen, ${table}.last_path_evidence_at)
      > ${referenceSql} - INTERVAL '28 days'
    AND GREATEST(${table}.last_seen, ${table}.last_path_evidence_at)
      <= ${referenceSql}
  )`;
}

export function parsePublicMapFields(value: unknown): PublicMapField[] {
  if (value === undefined || value === null || value === '') {
    return [...PUBLIC_MAP_ALLOWED_FIELDS];
  }
  if (typeof value !== 'string' || value.length > 512) {
    throw new PublicMapInputError('fields is invalid');
  }
  const rawFields = value
    .split(',')
    .map((field) => field.trim())
    .filter(Boolean);
  if (
    rawFields.length < 1
    || rawFields.some((field) =>
      !PUBLIC_MAP_ALLOWED_FIELDS.includes(field as PublicMapField))
  ) {
    throw new PublicMapInputError('fields contains an unsupported field');
  }
  const requested = rawFields as PublicMapField[];
  return requested.length > 0
    ? [...new Set<PublicMapField>(['node_id', ...requested])]
    : [...PUBLIC_MAP_ALLOWED_FIELDS];
}

export function parsePublicMapLimit(value: unknown): number {
  if (value === undefined || value === null || value === '') {
    return PUBLIC_MAP_DEFAULT_PAGE_ROWS;
  }
  if (typeof value !== 'string' || !/^[1-9]\d*$/.test(value)) {
    throw new PublicMapInputError('limit must be a positive integer');
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed > PUBLIC_MAP_MAX_PAGE_ROWS) {
    throw new PublicMapInputError(
      `limit must be between 1 and ${PUBLIC_MAP_MAX_PAGE_ROWS}`,
    );
  }
  return parsed;
}

export function parsePublicMapSnapshot(
  value: unknown,
  nowMs = Date.now(),
): string {
  if (value === undefined || value === null || value === '') {
    return new Date(nowMs).toISOString();
  }
  if (typeof value !== 'string' || value.length > 40) {
    throw new PublicMapInputError('snapshot is invalid');
  }
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)
      || new Date(parsed).toISOString() !== value
      || parsed > nowMs + 60_000
      || parsed < nowMs - PUBLIC_MAP_SNAPSHOT_MAX_AGE_MS) {
    throw new PublicMapInputError('snapshot is expired or invalid');
  }
  return value;
}

export function encodePublicMapCursor(nodeId: string): string {
  return Buffer.from(
    JSON.stringify({ version: 1, nodeId }),
    'utf8',
  ).toString('base64url');
}

export function parsePublicMapCursor(value: unknown): string | null {
  if (value === undefined || value === null || value === '') return null;
  if (typeof value !== 'string' || value.length > 512) {
    throw new PublicMapInputError('cursor is invalid');
  }
  try {
    const decoded = JSON.parse(Buffer.from(value, 'base64url').toString('utf8')) as {
      version?: unknown;
      nodeId?: unknown;
    };
    if (
      decoded.version !== 1
      || typeof decoded.nodeId !== 'string'
      || decoded.nodeId.length < 1
      || decoded.nodeId.length > 128
      || /[\u0000-\u001f\u007f]/.test(decoded.nodeId)
    ) {
      throw new Error('invalid cursor payload');
    }
    return decoded.nodeId;
  } catch {
    throw new PublicMapInputError('cursor is invalid');
  }
}

export function fitPublicMapRowsToByteBudget<T>(
  rows: readonly T[],
  maxRows: number,
  maxBytes = PUBLIC_MAP_MAX_PAGE_BYTES,
): { rows: T[]; truncatedByBytes: boolean } {
  const accepted: T[] = [];
  // Account for the response envelope and page metadata with a conservative
  // fixed reserve; every row is then measured as encoded JSON.
  let bytes = 512;
  for (const row of rows.slice(0, maxRows)) {
    const rowBytes = Buffer.byteLength(JSON.stringify(row), 'utf8') + 1;
    if (accepted.length > 0 && bytes + rowBytes > maxBytes) {
      return { rows: accepted, truncatedByBytes: true };
    }
    if (rowBytes + 512 > maxBytes) {
      throw new Error('one public map row exceeds the response byte budget');
    }
    accepted.push(row);
    bytes += rowBytes;
  }
  return { rows: accepted, truncatedByBytes: false };
}
