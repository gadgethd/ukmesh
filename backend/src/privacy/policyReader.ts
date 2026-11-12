import { PrivatePrefixCache, type PrivatePrefixRow } from './privatePrefixCache.js';
import type { QueryResultRow } from 'pg';

export type PublicPolicyQuery = <T extends QueryResultRow = QueryResultRow>(
  sql: string, params?: unknown[], signal?: AbortSignal,
) => Promise<{ rows: T[] }>;

/** Load one authoritative policy snapshot for an entire public read batch. */
export function createPublicPolicyReader(query: PublicPolicyQuery) {
  return async (): Promise<PrivatePrefixCache | null> => {
    const { rows } = await query<{
      generation: string;
      materialized_generation: string;
      remat_active: boolean;
      prefixes: PrivatePrefixRow[];
    }>(`
      SELECT visibility.generation::text AS generation,
             materialized.visibility_generation::text AS materialized_generation,
             EXISTS (
               SELECT 1 FROM ops.privacy_rematerialization_queue
                WHERE status IN ('pending', 'processing', 'failed')
             ) AS remat_active,
             COALESCE(jsonb_agg(jsonb_build_object(
               'node_id', prefix.node_id,
               'network', prefix.network,
               'prefix_size_bytes', prefix.prefix_size_bytes,
               'prefix', prefix.prefix
             )) FILTER (WHERE prefix.node_id IS NOT NULL), '[]'::jsonb) AS prefixes
        FROM ops.public_visibility_state visibility
        JOIN ops.packet_visibility_materialization_state materialized
          ON materialized.singleton = visibility.singleton
        LEFT JOIN core.private_node_prefixes prefix ON TRUE
       WHERE visibility.singleton = TRUE
       GROUP BY visibility.generation, materialized.visibility_generation
    `, [], AbortSignal.timeout(30_000));
    const state = rows[0];
    const generation = Number(state?.generation);
    if (!state || !Number.isSafeInteger(generation) || generation < 1
      || Number(state.materialized_generation) !== generation
      || state.remat_active !== false || !Array.isArray(state.prefixes)) return null;
    const policy = new PrivatePrefixCache();
    policy.replace(generation, state.prefixes);
    return policy;
  }

}
