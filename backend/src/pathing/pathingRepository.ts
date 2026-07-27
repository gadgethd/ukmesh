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
  visibility_generation: number;
};

type PathingRepositoryDeps = {
  getPathHistoryCache: (
    scope: string,
    visibilityGeneration: number,
  ) => Promise<PathHistoryCacheRow | null>;
  getPublicVisibilityGeneration: () => Promise<number>;
  query: QueryFn;
};

export type PathingRepository = ReturnType<typeof createPathingRepository>;

export function createPathingRepository(deps: PathingRepositoryDeps) {
  const { getPathHistoryCache, getPublicVisibilityGeneration, query } = deps;

  async function fetchVisibilityGeneration(): Promise<number> {
    return getPublicVisibilityGeneration();
  }

  async function fetchPathHistory(
    scope: string,
    visibilityGeneration: number,
  ): Promise<PathHistoryCacheRow | null> {
    return getPathHistoryCache(scope, visibilityGeneration);
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
        `SELECT pp.prefix, pp.receiver_region, pp.prev_prefix, pp.node_id, pp.probability, pp.count
         FROM path_prefix_priors pp
         JOIN nodes n ON n.node_id = pp.node_id
         WHERE pp.network = $1
           AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
         ORDER BY pp.count DESC
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
        `SELECT pt.from_node_id, pt.to_node_id, pt.receiver_region, pt.probability, pt.count
         FROM path_transition_priors pt
         JOIN nodes from_node ON from_node.node_id = pt.from_node_id
         JOIN nodes to_node ON to_node.node_id = pt.to_node_id
         WHERE pt.network = $1
           AND (from_node.name IS NULL OR from_node.name NOT LIKE '%🚫%')
           AND (to_node.name IS NULL OR to_node.name NOT LIKE '%🚫%')
         ORDER BY pt.count DESC
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
        `SELECT pe.from_node_id, pe.to_node_id, pe.receiver_region, pe.hour_bucket,
                pe.observed_count, pe.expected_count, pe.missing_count, pe.directional_support,
                pe.recency_score, pe.reliability, pe.itm_path_loss_db, pe.score, pe.consistency_penalty
         FROM path_edge_priors pe
         JOIN nodes from_node ON from_node.node_id = pe.from_node_id
         JOIN nodes to_node ON to_node.node_id = pe.to_node_id
         WHERE pe.network = $1
           AND (from_node.name IS NULL OR from_node.name NOT LIKE '%🚫%')
           AND (to_node.name IS NULL OR to_node.name NOT LIKE '%🚫%')
         ORDER BY pe.score DESC, pe.observed_count DESC
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
        `SELECT pm.receiver_region, pm.hour_bucket, pm.motif_len, pm.node_ids, pm.probability, pm.count
         FROM path_motif_priors pm
         WHERE pm.network = $1
           AND NOT EXISTS (
             SELECT 1
             FROM nodes private_node
             WHERE private_node.name LIKE '%🚫%'
               AND private_node.node_id = ANY(string_to_array(pm.node_ids, '>'))
           )
         ORDER BY pm.count DESC
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
    fetchVisibilityGeneration,
    fetchPathHistory,
    fetchPathLearning,
  };
}
