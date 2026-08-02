import type { QueryResultRow } from 'pg';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type PublicationCursor = {
  publishedAt: string;
  id: string;
};

export type PublicPlannedNode = {
  id: string;
  name: string;
  lat: number;
  lon: number;
  heightM: number | null;
  region: string | null;
  publishedAt: string;
  expiresAt: string;
};

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function encodePlannedNodeCursor(cursor: PublicationCursor): string {
  return Buffer.from(JSON.stringify(cursor), 'utf8').toString('base64url');
}

export function decodePlannedNodeCursor(value: unknown): PublicationCursor | null {
  if (value === undefined || value === null || value === '') return null;
  if (typeof value !== 'string' || value.length > 256 || !/^[A-Za-z0-9_-]+$/.test(value)) {
    throw new Error('INVALID_PLANNED_NODE_CURSOR');
  }
  try {
    const parsed = JSON.parse(Buffer.from(value, 'base64url').toString('utf8')) as {
      publishedAt?: unknown;
      id?: unknown;
    };
    if (
      typeof parsed.publishedAt !== 'string'
      || !Number.isFinite(Date.parse(parsed.publishedAt))
      || typeof parsed.id !== 'string'
      || !UUID_PATTERN.test(parsed.id)
    ) {
      throw new Error('invalid cursor payload');
    }
    return { publishedAt: new Date(parsed.publishedAt).toISOString(), id: parsed.id };
  } catch {
    throw new Error('INVALID_PLANNED_NODE_CURSOR');
  }
}

export async function listPublicPlannedNodes(
  query: QueryFn,
  input: { limit: number; cursor: PublicationCursor | null },
): Promise<{ items: PublicPlannedNode[]; nextCursor: string | null }> {
  const limit = Math.min(100, Math.max(1, Math.floor(input.limit) || 50));
  const result = await query<{
    id: string;
    name: string;
    lat: number;
    lon: number;
    height_m: number | null;
    region: string | null;
    published_at: string;
    expires_at: string;
  }>(
    `SELECT planned_node_id::text AS id,
            public_name AS name,
            public_lat AS lat,
            public_lon AS lon,
            public_height_m AS height_m,
            region,
            published_at,
            expires_at
       FROM planned_node_publications
      WHERE expires_at > NOW()
        AND (
          $1::timestamptz IS NULL
          OR (published_at, planned_node_id) < ($1::timestamptz, $2::uuid)
        )
      ORDER BY published_at DESC, planned_node_id DESC
      LIMIT $3`,
    [input.cursor?.publishedAt ?? null, input.cursor?.id ?? null, limit + 1],
  );
  const pageRows = result.rows.slice(0, limit);
  const items = pageRows.map((row) => ({
    id: row.id,
    name: row.name,
    lat: Number(row.lat),
    lon: Number(row.lon),
    heightM: row.height_m === null ? null : Number(row.height_m),
    region: row.region,
    publishedAt: new Date(row.published_at).toISOString(),
    expiresAt: new Date(row.expires_at).toISOString(),
  }));
  const last = pageRows.at(-1);
  return {
    items,
    nextCursor: result.rows.length > limit && last
      ? encodePlannedNodeCursor({
          publishedAt: new Date(last.published_at).toISOString(),
          id: last.id,
        })
      : null,
  };
}

export async function listOperatorPlannedNodes(query: QueryFn): Promise<QueryResultRow[]> {
  const result = await query(
    `SELECT planned.id::text,
            planned.owner_pubkey,
            planned.name,
            planned.lat,
            planned.lon,
            planned.height_m,
            planned.notes,
            planned.created_at,
            publication.public_name,
            publication.public_lat,
            publication.public_lon,
            publication.public_height_m,
            publication.region,
            publication.published_at,
            publication.expires_at
       FROM planned_nodes planned
       LEFT JOIN planned_node_publications publication
         ON publication.planned_node_id = planned.id
      ORDER BY planned.created_at DESC
      LIMIT 100`,
  );
  return result.rows;
}
