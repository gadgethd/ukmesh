import type { QueryResultRow } from 'pg';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

/** Private nodes publish 1..3-byte prefixes of their node id, so a stored path
 * prefix is private when it is a leading substring of any such node id - the
 * same identity rule the packet privacy predicate enforces. An empty prefix
 * (the head of a path) carries no identity and is always allowed through. */
export function privatePrefixPathFilterSql(columnExpr: string, alias = 'private_prefix'): string {
  return `EXISTS (
           SELECT 1
           FROM core.private_node_prefixes ${alias}
           WHERE LENGTH(${columnExpr}) > 0
             AND LEFT(UPPER(${alias}.node_id), LENGTH(${columnExpr}))
                 = UPPER(${columnExpr})
         )`;
}

type PathingRepositoryDeps = {
  getPublicVisibilityGeneration: () => Promise<number>;
  query: QueryFn;
};

export type PathingRepository = ReturnType<typeof createPathingRepository>;

type EdgePriorOrder = {
  score: number;
  observed_count: number;
  from_node_id: string;
  to_node_id: string;
  hour_bucket: number;
  receiver_region: string;
};

/** Compare text the way PostgreSQL's default C collation does for these keys. */
function compareText(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * Total order for `/api/path-learning` edge priors.
 *
 * `(score, observed_count)` alone is not unique: ties were returned in
 * whatever order the planner produced, so two identical reads could emit a
 * different `edgePriors` array (and a different LIMIT boundary set). The
 * repository key `(from_node_id, to_node_id, hour_bucket, receiver_region)` is
 * unique, which makes both the selected set and the emitted order stable.
 */
export function compareEdgePriors(a: EdgePriorOrder, b: EdgePriorOrder): number {
  return (b.score - a.score)
    || (b.observed_count - a.observed_count)
    || compareText(a.from_node_id, b.from_node_id)
    || compareText(a.to_node_id, b.to_node_id)
    || (a.hour_bucket - b.hour_bucket)
    || compareText(a.receiver_region, b.receiver_region);
}

export function createPathingRepository(deps: PathingRepositoryDeps) {
  const { getPublicVisibilityGeneration, query } = deps;

  async function fetchVisibilityGeneration(): Promise<number> {
    return getPublicVisibilityGeneration();
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
         FROM analytics.path_prefix_priors pp
         JOIN core.nodes n ON n.node_id = pp.node_id
         WHERE pp.network = $1
           AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
           AND NOT ${privatePrefixPathFilterSql('pp.prefix')}
           AND NOT ${privatePrefixPathFilterSql('pp.prev_prefix')}
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
         FROM analytics.path_transition_priors pt
         JOIN core.nodes from_node ON from_node.node_id = pt.from_node_id
         JOIN core.nodes to_node ON to_node.node_id = pt.to_node_id
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
         FROM analytics.path_edge_priors pe
         JOIN core.nodes from_node ON from_node.node_id = pe.from_node_id
         JOIN core.nodes to_node ON to_node.node_id = pe.to_node_id
         WHERE pe.network = $1
           AND (from_node.name IS NULL OR from_node.name NOT LIKE '%🚫%')
           AND (to_node.name IS NULL OR to_node.name NOT LIKE '%🚫%')
         ORDER BY pe.score DESC, pe.observed_count DESC,
                  pe.from_node_id, pe.to_node_id, pe.hour_bucket, pe.receiver_region
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
         FROM analytics.path_motif_priors pm
         WHERE pm.network = $1
           AND NOT EXISTS (
             SELECT 1
             FROM core.nodes private_node
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
         FROM analytics.path_model_calibration
         WHERE network = $1`,
        [network],
      ),
    ]);

    // Belt and braces: the SQL tie-break fixes the LIMIT boundary, and this
    // sort makes the emitted array independent of whatever order the driver
    // hands back. A future edit that drops a key from ORDER BY still cannot
    // make two identical reads disagree here.
    edgeRows.rows.sort(compareEdgePriors);

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
    fetchPathLearning,
  };
}
