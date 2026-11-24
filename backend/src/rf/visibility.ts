import type { QueryResultRow } from 'pg';
import { UKMESH_NETWORK_SQL_LIST } from '../networks.js';
import { nodeAliasArraySql } from '../platform/query/networkFilters.js';
import { PUBLIC_RESPONSE_GENERATION_SQL } from '../privacy/responseGeneration.js';

export type RfQuery = <T extends QueryResultRow = QueryResultRow>(
  sql: string, params?: unknown[], signal?: AbortSignal,
) => Promise<{ rows: T[] }>;

export const RF_REPEATER_WHERE = `
  n.role = 2
  AND n.lat IS NOT NULL
  AND n.lon IS NOT NULL
  AND n.lat BETWEEN -90 AND 90
  AND n.lon BETWEEN -180 AND 180
  AND NOT (ABS(n.lat) < 1e-9 AND ABS(n.lon) < 1e-9)
  AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
  AND (
    n.network = 'ukmesh'
    OR EXISTS (
      SELECT 1 FROM core.node_identity_sightings sighting
      WHERE sighting.node_id = n.node_id AND sighting.network = 'ukmesh'
    )
  )`;
// An RF artifact can expose physical position. Exact identity and all its aliases
// must be eligible, but a short prefix shared by an unrelated node cannot veto it.
export const PUBLIC_RF_REPEATER_WHERE = `
  ${RF_REPEATER_WHERE}
  AND n.node_id ~ '^[0-9A-Fa-f]{64}$'
  AND NOT EXISTS (
    SELECT 1 FROM core.private_node_prefixes AS blocking
    WHERE blocking.network IN (${UKMESH_NETWORK_SQL_LIST})
      AND blocking.node_id = ANY(${nodeAliasArraySql('n.node_id')})
  )
  AND NOT EXISTS (
    SELECT 1 FROM core.nodes AS member
    WHERE member.node_id = ANY(${nodeAliasArraySql('n.node_id')})
      AND member.name LIKE '%🚫%'
  )`;

export type RfPosition = { lat: number; lon: number };
export type RfVisibility = { generation: number; positions: Map<string, RfPosition> };

export function publicRfGeneration(value: unknown): number | null {
  const generation = typeof value === 'string' && /^[1-9][0-9]*$/.test(value) ? Number(value) : value;
  return typeof generation === 'number' && Number.isSafeInteger(generation) && generation > 0
    ? generation : null;
}

export function createRfVisibilityReader(query: RfQuery) {
  return async (keys: readonly string[]): Promise<RfVisibility> => {
    const signal = AbortSignal.timeout(2000);
    const { rows } = await query<{
      generation: string;
      positions: Array<{ key: string; lat: number; lon: number }>;
    }>(`SELECT policy.generation, COALESCE(
        jsonb_agg(jsonb_build_object('key', lower(repeater.node_id), 'lat', repeater.lat, 'lon', repeater.lon))
          FILTER (WHERE repeater.node_id IS NOT NULL), '[]'::jsonb
      ) AS positions
      FROM (${PUBLIC_RESPONSE_GENERATION_SQL}) AS policy
      LEFT JOIN LATERAL (
        SELECT n.node_id, n.lat, n.lon
        FROM core.node_identity_nodes n
        WHERE n.node_id = ANY($1::text[]) AND ${PUBLIC_RF_REPEATER_WHERE}
      ) AS repeater ON true
      GROUP BY policy.generation`, [keys.map((key) => key.toUpperCase())], signal);
    const state = rows[0];
    const generation = publicRfGeneration(state?.generation);
    if (generation === null || !Array.isArray(state?.positions)) throw new Error('RF_VISIBILITY_UNAVAILABLE');
    return { generation, positions: new Map(state.positions.map(({ key, lat, lon }) => [key, { lat, lon }])) };
  };
}
