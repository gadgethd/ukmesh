import type { PoolClient } from 'pg';

const WRITE_BATCH_SIZE = 1_000;

export type PathLearningDeltaDefinition = {
  target: string;
  stage: string;
  columns: string[];
  keys: string[];
  semantic: string[];
  jsonRecord: string;
};

export const PATH_LEARNING_DELTA_DEFINITIONS: PathLearningDeltaDefinition[] = [
  {
    target: 'path_prefix_priors', stage: 'desired_path_prefix_priors',
    columns: ['prefix', 'receiver_region', 'prev_prefix', 'node_id', 'count', 'probability'],
    keys: ['prefix', 'receiver_region', 'prev_prefix', 'node_id'],
    semantic: ['count', 'probability'],
    jsonRecord: 'prefix text, receiver_region text, prev_prefix text, node_id text, count integer, probability double precision',
  },
  {
    target: 'path_position_prefix_priors', stage: 'desired_path_position_prefix_priors',
    columns: ['prefix', 'position', 'node_id', 'count', 'probability'],
    keys: ['prefix', 'position', 'node_id'], semantic: ['count', 'probability'],
    jsonRecord: 'prefix text, position smallint, node_id text, count integer, probability double precision',
  },
  {
    target: 'path_corridor_priors', stage: 'desired_path_corridor_priors',
    columns: ['src_node_id', 'rx_node_id', 'position', 'node_id', 'count', 'probability'],
    keys: ['src_node_id', 'rx_node_id', 'position', 'node_id'], semantic: ['count', 'probability'],
    jsonRecord: 'src_node_id text, rx_node_id text, position smallint, node_id text, count integer, probability double precision',
  },
  {
    target: 'path_transition_priors', stage: 'desired_path_transition_priors',
    columns: ['from_node_id', 'to_node_id', 'receiver_region', 'count', 'probability'],
    keys: ['from_node_id', 'to_node_id', 'receiver_region'], semantic: ['count', 'probability'],
    jsonRecord: 'from_node_id text, to_node_id text, receiver_region text, count integer, probability double precision',
  },
  {
    target: 'path_position_transition_priors', stage: 'desired_path_position_transition_priors',
    columns: ['position', 'from_node_id', 'to_node_id', 'count', 'probability'],
    keys: ['position', 'from_node_id', 'to_node_id'], semantic: ['count', 'probability'],
    jsonRecord: 'position smallint, from_node_id text, to_node_id text, count integer, probability double precision',
  },
  {
    target: 'path_edge_priors', stage: 'desired_path_edge_priors',
    columns: [
      'from_node_id', 'to_node_id', 'receiver_region', 'hour_bucket', 'observed_count',
      'expected_count', 'missing_count', 'directional_support', 'recency_score', 'reliability',
      'itm_path_loss_db', 'score', 'consistency_penalty',
    ],
    keys: ['receiver_region', 'hour_bucket', 'from_node_id', 'to_node_id'],
    semantic: [
      'observed_count', 'expected_count', 'missing_count', 'directional_support',
      'recency_score', 'reliability', 'itm_path_loss_db', 'score', 'consistency_penalty',
    ],
    jsonRecord: [
      'from_node_id text', 'to_node_id text', 'receiver_region text', 'hour_bucket smallint',
      'observed_count integer', 'expected_count integer', 'missing_count integer',
      'directional_support double precision', 'recency_score double precision',
      'reliability double precision', 'itm_path_loss_db double precision', 'score double precision',
      'consistency_penalty double precision',
    ].join(', '),
  },
  {
    target: 'path_motif_priors', stage: 'desired_path_motif_priors',
    columns: ['receiver_region', 'hour_bucket', 'motif_len', 'node_ids', 'count', 'probability'],
    keys: ['receiver_region', 'hour_bucket', 'motif_len', 'node_ids'],
    semantic: ['count', 'probability'],
    jsonRecord: 'receiver_region text, hour_bucket smallint, motif_len smallint, node_ids text, count integer, probability double precision',
  },
];

export function pathLearningDeltaMergeSql(definition: PathLearningDeltaDefinition): string {
  const allColumns = ['network', ...definition.columns];
  const conflictKeys = ['network', ...definition.keys];
  const semanticTarget = definition.semantic.map((column) => `${definition.target}.${column}`).join(', ');
  const semanticExcluded = definition.semantic.map((column) => `EXCLUDED.${column}`).join(', ');
  const updateSet = definition.semantic.map((column) => `${column} = EXCLUDED.${column}`).join(',\n           ');
  const absentKey = definition.keys.map(
    (column) => `desired.${column} IS NOT DISTINCT FROM ${definition.target}.${column}`,
  ).join('\n             AND ');
  return `WITH upserted AS (
    INSERT INTO ${definition.target} (${allColumns.join(', ')}, updated_at)
    SELECT ${allColumns.map((column) => `desired.${column}`).join(', ')}, NOW()
      FROM ${definition.stage} desired
     WHERE desired.network = $1
    ON CONFLICT (${conflictKeys.join(', ')}) DO UPDATE SET
           ${updateSet},
           updated_at = NOW()
     WHERE ROW(${semanticTarget}) IS DISTINCT FROM ROW(${semanticExcluded})
    RETURNING 1
  ), deleted AS (
    DELETE FROM ${definition.target}
     WHERE network = $1
       AND NOT EXISTS (
         SELECT 1 FROM ${definition.stage} desired
          WHERE desired.network = $1
            AND ${absentKey}
       )
    RETURNING 1
  )
  SELECT (SELECT COUNT(*)::integer FROM upserted) AS upserted,
         (SELECT COUNT(*)::integer FROM deleted) AS deleted`;
}

export async function publishPathLearningDelta(
  client: PoolClient,
  network: string,
  datasets: Array<{ definition: PathLearningDeltaDefinition; rows: object[] }>,
  assertOwned: () => void,
): Promise<{ upserted: number; deleted: number }> {
  for (const { definition } of datasets) {
    await client.query(
      `CREATE TEMP TABLE ${definition.stage}
       (LIKE ${definition.target} INCLUDING DEFAULTS)
       ON COMMIT DROP`,
    );
  }
  for (const { definition, rows } of datasets) {
    for (let offset = 0; offset < rows.length; offset += WRITE_BATCH_SIZE) {
      assertOwned();
      await client.query(
        `INSERT INTO ${definition.stage} (network, ${definition.columns.join(', ')})
         SELECT $1, ${definition.columns.map((column) => `row.${column}`).join(', ')}
           FROM jsonb_to_recordset($2::jsonb) AS row(${definition.jsonRecord})`,
        [network, JSON.stringify(rows.slice(offset, offset + WRITE_BATCH_SIZE))],
      );
    }
  }
  let upserted = 0;
  let deleted = 0;
  for (const { definition } of datasets) {
    assertOwned();
    const result = await client.query<{ upserted: number; deleted: number }>(
      pathLearningDeltaMergeSql(definition),
      [network],
    );
    upserted += Number(result.rows[0]?.upserted ?? 0);
    deleted += Number(result.rows[0]?.deleted ?? 0);
  }
  return { upserted, deleted };
}
