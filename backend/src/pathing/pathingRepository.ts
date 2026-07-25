import type { QueryResultRow } from 'pg';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type PathHistoryCacheRow = {
  window_start: string | null;
  updated_at: string | null;
  packet_count: number;
  resolved_packet_count: number;
  segment_counts: Array<{ count?: number }> | null;
};

type PathingRepositoryDeps = {
  getPathHistoryCache: (scope: string) => Promise<PathHistoryCacheRow | null>;
  query: QueryFn;
};

export type PathingRepository = ReturnType<typeof createPathingRepository>;

export function createPathingRepository(deps: PathingRepositoryDeps) {
  const { getPathHistoryCache, query } = deps;

  async function fetchPathHistory(scope: string): Promise<PathHistoryCacheRow | null> {
    return getPathHistoryCache(scope);
  }

  async function fetchPathLearning(network: string, limit: number) {
    const eligibleNodesCte = `eligible_nodes AS MATERIALIZED (
      SELECT n.node_id
      FROM nodes n
      WHERE n.lat IS NOT NULL
        AND n.lon IS NOT NULL
        AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
        AND (n.role IS NULL OR n.role = 2)
        AND (
          (
            $1 = 'test'
            AND (
              n.network = 'test'
              OR EXISTS (
                SELECT 1 FROM node_network_sightings s
                WHERE s.node_id = n.node_id
                  AND s.network = 'test'
                  AND s.last_seen_at > NOW() - INTERVAL '30 days'
              )
            )
          )
          OR (
            $1 = 'ukmesh'
            AND n.network IS DISTINCT FROM 'test'
            AND (
              n.network IN ('ukmesh', 'northeast', 'teesside')
              OR EXISTS (
                SELECT 1 FROM node_network_sightings s
                WHERE s.node_id = n.node_id
                  AND s.network IN ('ukmesh', 'northeast', 'teesside')
                  AND s.last_seen_at > NOW() - INTERVAL '30 days'
              )
            )
          )
        )
    )`;
    const [prefixRows, transitionRows, edgeRows, motifRows, calibrationRows] = await Promise.all([
      query<{
        prefix: string;
        receiver_region: string;
        prev_prefix: string | null;
        node_id: string;
        probability: number;
        count: number;
      }>(
        `WITH ${eligibleNodesCte}
         SELECT p.prefix, p.receiver_region, p.prev_prefix, p.node_id, p.probability, p.count
         FROM path_prefix_priors p
         JOIN eligible_nodes n ON n.node_id = p.node_id
         WHERE p.network = $1
         ORDER BY p.count DESC
         LIMIT $2`,
        [network, limit],
      ),
      query<{
        from_node_id: string;
        to_node_id: string;
        receiver_region: string;
        probability: number;
        count: number;
      }>(
        `WITH ${eligibleNodesCte}
         SELECT p.from_node_id, p.to_node_id, p.receiver_region, p.probability, p.count
         FROM path_transition_priors p
         JOIN eligible_nodes source ON source.node_id = p.from_node_id
         JOIN eligible_nodes target ON target.node_id = p.to_node_id
         WHERE p.network = $1
         ORDER BY p.count DESC
         LIMIT $2`,
        [network, limit],
      ),
      query<{
        from_node_id: string;
        to_node_id: string;
        receiver_region: string;
        hour_bucket: number;
        observed_count: number;
        expected_count: number;
        missing_count: number;
        directional_support: number;
        recency_score: number;
        reliability: number;
        itm_path_loss_db: number | null;
        score: number;
        consistency_penalty: number;
      }>(
        `WITH ${eligibleNodesCte}
         SELECT p.from_node_id, p.to_node_id, p.receiver_region, p.hour_bucket,
                p.observed_count, p.expected_count, p.missing_count, p.directional_support,
                p.recency_score, p.reliability, p.itm_path_loss_db, p.score, p.consistency_penalty
         FROM path_edge_priors p
         JOIN eligible_nodes source ON source.node_id = p.from_node_id
         JOIN eligible_nodes target ON target.node_id = p.to_node_id
         WHERE p.network = $1
         ORDER BY p.score DESC, p.observed_count DESC
         LIMIT $2`,
        [network, limit],
      ),
      query<{
        receiver_region: string;
        hour_bucket: number;
        motif_len: number;
        node_ids: string;
        probability: number;
        count: number;
      }>(
        `WITH ${eligibleNodesCte}
         SELECT p.receiver_region, p.hour_bucket, p.motif_len, p.node_ids, p.probability, p.count
         FROM path_motif_priors p
         WHERE p.network = $1
           AND NOT EXISTS (
             SELECT 1
             FROM unnest(string_to_array(p.node_ids, '>')) AS motif_node(node_id)
             LEFT JOIN eligible_nodes n ON n.node_id = motif_node.node_id
             WHERE n.node_id IS NULL
           )
         ORDER BY p.count DESC
         LIMIT $2`,
        [network, limit],
      ),
      query<{
        evaluated_packets: number;
        top1_accuracy: number;
        mean_pred_confidence: number;
        confidence_scale: number;
        confidence_bias: number;
        recommended_threshold: number;
      }>(
        `SELECT evaluated_packets, top1_accuracy, mean_pred_confidence, confidence_scale, confidence_bias, recommended_threshold
         FROM path_model_calibration
         WHERE network = $1`,
        [network],
      ),
    ]);

    return {
      prefixRows,
      transitionRows,
      edgeRows,
      motifRows,
      calibrationRows,
    };
  }

  return {
    fetchPathHistory,
    fetchPathLearning,
  };
}
