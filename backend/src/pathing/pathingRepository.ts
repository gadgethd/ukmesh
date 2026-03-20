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
    const [prefixRows, transitionRows, edgeRows, motifRows, calibrationRows] = await Promise.all([
      query<{
        prefix: string;
        receiver_region: string;
        prev_prefix: string | null;
        node_id: string;
        probability: number;
        count: number;
      }>(
        `SELECT prefix, receiver_region, prev_prefix, node_id, probability, count
         FROM path_prefix_priors
         WHERE network = $1
         ORDER BY count DESC
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
        `SELECT from_node_id, to_node_id, receiver_region, probability, count
         FROM path_transition_priors
         WHERE network = $1
         ORDER BY count DESC
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
        `SELECT from_node_id, to_node_id, receiver_region, hour_bucket,
                observed_count, expected_count, missing_count, directional_support,
                recency_score, reliability, itm_path_loss_db, score, consistency_penalty
         FROM path_edge_priors
         WHERE network = $1
         ORDER BY score DESC, observed_count DESC
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
        `SELECT receiver_region, hour_bucket, motif_len, node_ids, probability, count
         FROM path_motif_priors
         WHERE network = $1
         ORDER BY count DESC
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
