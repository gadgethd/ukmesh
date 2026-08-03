import type { QueryResultRow } from 'pg';

export type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

export type HistoricPathNode = QueryResultRow & {
  node_id: string;
  name: string | null;
  lat: number;
  lon: number;
  iata: string | null;
  role: number | null;
  last_seen: string | Date;
  is_online: true;
  hardware_model: string | null;
  public_key: string | null;
  advert_count: number | null;
  elevation_m: number | null;
  network: string | null;
};

/**
 * Resolve trustworthy multibyte relay hashes against the historic inventory.
 * Only a unique, coordinate-bearing repeater can be reactivated; its stored
 * coordinates are returned unchanged for the live node upsert.
 */
export async function reactivateHistoricPathNodes(
  query: QueryFn,
  input: {
    pathHashes: string[];
    sizeBytes: number;
    seenAt: Date;
    routeType?: number;
    network?: string;
  },
): Promise<HistoricPathNode[]> {
  const { pathHashes, sizeBytes, seenAt, routeType, network } = input;
  // Direct routes contain a future route. Only Flood/TransportFlood paths are
  // evidence that the listed relay actually handled this packet.
  if ((routeType !== 0 && routeType !== 1) || (sizeBytes !== 2 && sizeBytes !== 3)) return [];

  const prefixLength = sizeBytes * 2;
  const hashes = Array.from(new Set(
    pathHashes
      .map((hash) => String(hash).trim().toUpperCase())
      .filter((hash) => hash.length === prefixLength && /^[0-9A-F]+$/.test(hash)),
  ));
  if (hashes.length === 0) return [];

  const networkScope = network === 'test'
    ? "n.network = 'test'"
    : "n.network IS DISTINCT FROM 'test'";
  // prefixLength is restricted above to 4 or 6, allowing the matching
  // functional indexes to be used without interpolating caller input.
  const result = await query<HistoricPathNode>(
    `WITH input(hash) AS (
       SELECT UNNEST($1::text[])
     ),
     candidates AS (
       SELECT n.node_id, i.hash
       FROM nodes n
       JOIN input i ON UPPER(LEFT(n.node_id, ${prefixLength})) = i.hash
       WHERE (n.role = 2 OR n.role IS NULL)
         AND ${networkScope}
         AND n.lat BETWEEN -90 AND 90
         AND n.lon BETWEEN -180 AND 180
         AND NOT (ABS(n.lat) < 1e-9 AND ABS(n.lon) < 1e-9)
     ),
     unique_hashes AS (
       SELECT hash
       FROM candidates
       GROUP BY hash
       HAVING COUNT(*) = 1
     ),
     matched AS (
       SELECT c.node_id
       FROM candidates c
       JOIN unique_hashes u ON u.hash = c.hash
     ),
     updated AS (
       UPDATE nodes n
       SET last_path_evidence_at = $2::timestamptz
       FROM matched m
       WHERE n.node_id = m.node_id
         AND (n.last_path_evidence_at IS NULL OR n.last_path_evidence_at < $2::timestamptz)
       RETURNING
         n.node_id,
         n.name,
         n.lat,
         n.lon,
         COALESCE(n.observer_iata, n.iata) AS iata,
         n.role,
         n.last_path_evidence_at AS last_seen,
         TRUE AS is_online,
         n.hardware_model,
         n.public_key,
         n.advert_count,
         n.elevation_m,
         n.network
     )
     SELECT * FROM updated`,
    [hashes, seenAt.toISOString()],
  );
  return result.rows;
}
