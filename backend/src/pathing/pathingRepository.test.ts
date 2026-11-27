import assert from 'node:assert/strict';
import test from 'node:test';
import type { QueryResultRow } from 'pg';
import { createPathingRepository } from './pathingRepository.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type EdgeRow = {
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
};

/**
 * Three rows that tie on both `score` and `observed_count` but differ on the
 * repository key. Without the tie-break PostgreSQL may return them in any
 * order, so the API could emit a different `edgePriors` array for the same
 * read.
 */
function edgeRows(): EdgeRow[] {
  const base = {
    observed_count: 7,
    expected_count: 9,
    missing_count: 2,
    directional_support: 0.8,
    recency_score: 0.6,
    reliability: 0.9,
    itm_path_loss_db: null,
    score: 0.5,
    consistency_penalty: 0,
  };
  return [
    { ...base, from_node_id: 'BB'.repeat(32), to_node_id: '22'.repeat(32), receiver_region: 'LHR', hour_bucket: 9 },
    { ...base, from_node_id: 'AA'.repeat(32), to_node_id: '33'.repeat(32), receiver_region: 'MAN', hour_bucket: 9 },
    { ...base, from_node_id: 'AA'.repeat(32), to_node_id: '11'.repeat(32), receiver_region: 'LHR', hour_bucket: 9 },
  ];
}

function expectedOrder(rows: EdgeRow[]): EdgeRow[] {
  return [...rows].sort((a, b) =>
    (b.score - a.score)
    || (b.observed_count - a.observed_count)
    || (a.from_node_id < b.from_node_id ? -1 : a.from_node_id > b.from_node_id ? 1 : 0)
    || (a.to_node_id < b.to_node_id ? -1 : a.to_node_id > b.to_node_id ? 1 : 0)
    || (a.hour_bucket - b.hour_bucket)
    || (a.receiver_region < b.receiver_region ? -1 : a.receiver_region > b.receiver_region ? 1 : 0));
}

function repositoryWith(edgeOrder: EdgeRow[][], observedSql: string[]): ReturnType<typeof createPathingRepository> {
  let edgeCall = 0;
  const query: QueryFn = async <T extends QueryResultRow>(text: string): Promise<{ rows: T[] }> => {
    if (text.includes('FROM analytics.path_edge_priors')) {
      observedSql.push(text);
      const rows = edgeOrder[Math.min(edgeCall, edgeOrder.length - 1)] ?? [];
      edgeCall += 1;
      return { rows: rows as unknown as T[] };
    }
    if (text.includes('FROM analytics.path_prefix_priors')
      || text.includes('FROM analytics.path_transition_priors')
      || text.includes('FROM analytics.path_motif_priors')
      || text.includes('FROM analytics.path_model_calibration')) {
      return { rows: [] };
    }
    throw new Error(`unexpected query: ${text}`);
  };
  return createPathingRepository({ query });
}

test('identical path-learning reads return identical edgePriors', async () => {
  const rows = edgeRows();
  // Two calls hand the rows back in different orders, which is what a plan
  // change or a VACUUM can do when the ORDER BY is not a total order.
  const sql: string[] = [];
  const repository = repositoryWith([rows, [...rows].reverse()], sql);

  const first = await repository.fetchPathLearning('ukmesh', 100);
  const second = await repository.fetchPathLearning('ukmesh', 100);

  assert.deepEqual(first.edgeRows.rows, expectedOrder(rows));
  assert.deepEqual(
    second.edgeRows.rows,
    first.edgeRows.rows,
    'repeated identical reads must not reorder equal-score edge priors',
  );
  assert.deepEqual(
    first.edgeRows.rows.map((row) => `${row.from_node_id}:${row.to_node_id}:${row.hour_bucket}:${row.receiver_region}`),
    [
      `${'AA'.repeat(32)}:${'11'.repeat(32)}:9:LHR`,
      `${'AA'.repeat(32)}:${'33'.repeat(32)}:9:MAN`,
      `${'BB'.repeat(32)}:${'22'.repeat(32)}:9:LHR`,
    ],
  );
});

test('the edge-prior query orders by the full unique repository key', async () => {
  const sql: string[] = [];
  const repository = repositoryWith([edgeRows()], sql);
  await repository.fetchPathLearning('ukmesh', 100);

  assert.equal(sql.length, 1);
  const order = sql[0]!.replace(/\s+/g, ' ').match(/ORDER BY (.+?) LIMIT/)?.[1];
  assert.equal(
    order,
    'pe.score DESC, pe.observed_count DESC, pe.from_node_id, pe.to_node_id, pe.hour_bucket, pe.receiver_region',
    'a non-unique ORDER BY lets the LIMIT boundary and the emitted order change between identical reads',
  );
});
