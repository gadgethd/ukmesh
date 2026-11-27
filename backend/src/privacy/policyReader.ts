import { PrivatePrefixCache, type PrivatePrefixRow, type PublicPrivacyPolicy } from './privatePrefixCache.js';
import type { QueryResultRow } from 'pg';

export type PublicPolicyQuery = <T extends QueryResultRow = QueryResultRow>(
  sql: string, params?: unknown[], signal?: AbortSignal,
) => Promise<{ rows: T[] }>;

/** Load one authoritative policy snapshot for an entire public read batch. */
export function createPublicPolicyReader(query: PublicPolicyQuery) {
  let cached: PublicPrivacyPolicy | null = null;
  let invalidation = 0;
  return async (): Promise<PublicPrivacyPolicy | null> => {
    const captured = cached;
    const capturedInvalidation = invalidation;
    const forget = () => {
      if (cached !== captured) return;
      cached = null;
      invalidation++;
    };
    const { rows } = await query<{
      generation: string;
      materialized_generation: string;
      remat_active: boolean;
      prefixes: PrivatePrefixRow[] | null;
    }>(`
      WITH authority AS MATERIALIZED (
        SELECT visibility.generation::text AS generation,
               materialized.visibility_generation::text AS materialized_generation,
               EXISTS (
                 SELECT 1 FROM ops.privacy_rematerialization_queue
                  WHERE status IN ('pending', 'processing', 'failed')
               ) AS remat_active
          FROM ops.public_visibility_state visibility
          JOIN ops.packet_visibility_materialization_state materialized
            ON materialized.singleton = visibility.singleton
         WHERE visibility.singleton = TRUE
      )
      SELECT authority.*, CASE
        WHEN generation = materialized_generation AND NOT remat_active
          AND generation IS DISTINCT FROM $1::text
        THEN (SELECT COALESCE(jsonb_agg(jsonb_build_object(
          'node_id', prefix.node_id,
          'network', prefix.network,
          'prefix_size_bytes', prefix.prefix_size_bytes,
          'prefix', prefix.prefix
        )), '[]'::jsonb) FROM core.private_node_prefixes prefix)
      END AS prefixes FROM authority
    `, [captured?.currentGeneration ?? null], AbortSignal.timeout(30_000)).catch((error: unknown) => {
      forget();
      throw error;
    });
    const state = rows[0];
    const generation = Number(state?.generation);
    if (!state || !Number.isSafeInteger(generation) || generation < 1
      || Number(state.materialized_generation) !== generation
      || state.remat_active !== false) {
      forget();
      return null;
    }
    // A concurrent fence may already have seen a newer generation or an
    // uncertified state while this statement was in flight.
    if (capturedInvalidation !== invalidation
      || (cached !== captured && cached && generation < cached.currentGeneration)) return null;
    if (captured?.currentGeneration === generation && state.prefixes === null) return captured;
    if (!Array.isArray(state.prefixes)) {
      forget();
      return null;
    }
    const policy = new PrivatePrefixCache();
    policy.replace(generation, state.prefixes);
    if (cached === captured || (cached && generation > cached.currentGeneration)) cached = policy;
    return policy;
  };
}
