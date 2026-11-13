import assert from 'node:assert/strict';
import test from 'node:test';
import { LinkMetricsIndex, type LinkMetricRow } from './linkMetricsIndex.js';
import { undirectedLinkKey } from '../path-shared/priorKeys.js';
import type { LinkMetrics } from './types.js';

const row = (a: string, b: string, observed_count: number | string = 1): LinkMetricRow => ({
  node_a_id: a, node_b_id: b, observed_count, multibyte_observed_count: '2',
  itm_path_loss_db: '107.5', itm_viable: true, count_a_to_b: '3', count_b_to_a: '4',
});

// The former loader's conversion, kept independent of the packed representation.
function reference(rows: LinkMetricRow[]) {
  return new Map(rows.map(r => [undirectedLinkKey(r.node_a_id, r.node_b_id), {
    observed_count: Number(r.observed_count ?? 0),
    multibyte_observed_count: Number(r.multibyte_observed_count ?? 0),
    itm_path_loss_db: r.itm_path_loss_db == null ? null : Number(r.itm_path_loss_db),
    itm_viable: r.itm_viable ?? null,
    count_a_to_b: r.count_a_to_b == null ? null : Number(r.count_a_to_b),
    count_b_to_a: r.count_b_to_a == null ? null : Number(r.count_b_to_a),
    ...(r.neighbor_report_count !== undefined ? { neighbor_report_count: Number(r.neighbor_report_count) } : {}),
    ...(r.neighbor_best_snr_db !== undefined ? { neighbor_best_snr_db: r.neighbor_best_snr_db === null ? null : Number(r.neighbor_best_snr_db) } : {}),
  } satisfies LinkMetrics]));
}

test('packed links preserve all fields, nulls, special numbers and optional evidence', () => {
  const rows: LinkMetricRow[] = [row('A', 'B'),
    { ...row('C', 'D'), observed_count: null, multibyte_observed_count: null, itm_path_loss_db: null,
      itm_viable: null, count_a_to_b: null, count_b_to_a: null },
    { ...row('E', 'F'), observed_count: NaN, multibyte_observed_count: -0, itm_path_loss_db: Infinity,
      itm_viable: false, count_a_to_b: -Infinity, count_b_to_a: '9007199254740993', neighbor_report_count: '9', neighbor_best_snr_db: null },
    { ...row('G', 'H'), neighbor_best_snr_db: '-3.5' },
  ];
  const expected = reference(rows), actual = new LinkMetricsIndex(rows);
  assert.equal(actual.size, expected.size);
  for (const [key, value] of expected) assert.deepEqual(actual.get(key), value);
  assert.equal(actual.get('A:C'), undefined);
  assert.equal(actual.get('B:A'), undefined);
  assert.equal(actual.get('no delimiter'), undefined);
  assert.equal(actual.get('missing:node'), undefined);
});

test('legacy IDs, key collisions, self links and duplicate direction retain Map semantics', () => {
  const rows = [row('', '雪'), row('é', 'a'), row('self', 'self'),
    row('A', 'B', 1), row('B', 'A', 27), row('a:b', 'c', 3), row('a', 'b:c', 4),
    row(':', ':', 5), row('A'.repeat(64), 'B'.repeat(64), 6)];
  const actual = new LinkMetricsIndex(rows), expected = reference(rows);
  assert.equal(actual.size, expected.size);
  for (const [key, value] of expected) assert.deepEqual(actual.get(key), value);
  assert.equal(actual.get('A:B')?.count_a_to_b, 3, 'reverse duplicates do not swap stored directional counts');
  assert.equal(actual.get('a:b:c')?.observed_count, 4);
  rows[0]!.observed_count = 900;
  assert.equal(actual.get(':雪')?.observed_count, 1, 'the snapshot retains values independently of input rows');
});

test('complete deterministic graph matches the former loader', () => {
  const rows = Array.from({ length: 20_000 }, (_, i) => row(String(i % 501).padStart(64, '0'), String((i * 17 + 1) % 997).padStart(64, '0'), i));
  const expected = reference(rows), actual = new LinkMetricsIndex(rows);
  assert.equal(actual.size, expected.size);
  for (const [key, value] of expected) assert.deepEqual(actual.get(key), value);
});
