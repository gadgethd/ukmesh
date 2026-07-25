import assert from 'node:assert/strict';
import { test } from 'node:test';
import type { QueryResultRow } from 'pg';
import { createStatsRepository } from './statsRepository.js';

test('24-hour chart aggregates use the complete scoped window and true signal medians', async () => {
  const calls: Array<{ text: string; params?: unknown[] }> = [];
  const query = async <T extends QueryResultRow = QueryResultRow>(
    text: string,
    params?: unknown[],
  ): Promise<{ rows: T[] }> => {
    calls.push({ text, params });
    return { rows: [] };
  };
  const repository = createStatsRepository({
    query,
    networkFilters: () => ({
      params: ['ukmesh'],
      packets: 'AND network = $1',
      packetsAlias: (alias: string) => `AND ${alias}.network = $1`,
      nodes: 'AND network = $1',
      nodesAlias: (alias: string) => `AND ${alias}.network = $1`,
    }),
  });

  await repository.fetchChartsData('ukmesh', undefined);

  const sqlFor = (fragment: string): string => {
    const sql = calls.find((call) => call.text.includes(fragment))?.text;
    assert.ok(sql, `expected query containing ${fragment}`);
    return sql;
  };
  const fullWindowAggregates = [
    sqlFor('WITH prefix_counts'),
    sqlFor('hash_hex_len'),
    sqlFor('fully_decoded_multibyte_24h'),
    sqlFor('avg_observers'),
    sqlFor('median_rssi'),
  ];

  for (const sql of fullWindowAggregates) {
    assert.doesNotMatch(sql, /\bLIMIT\s+50000\b/i);
    assert.match(sql, /AND p\.network = \$1/);
  }

  const signalSql = sqlFor('median_rssi');
  assert.match(signalSql, /percentile_cont\(0\.5\)\s+WITHIN GROUP\s+\(ORDER BY p\.rssi\)::text\s+AS median_rssi/i);
  assert.match(signalSql, /percentile_cont\(0\.5\)\s+WITHIN GROUP\s+\(ORDER BY p\.snr\)::text\s+AS median_snr/i);
  assert.doesNotMatch(signalSql, /AVG\(p\.rssi\)::text\s+AS median_rssi/i);
  assert.doesNotMatch(signalSql, /AVG\(p\.snr\)::text\s+AS median_snr/i);
  assert.match(sqlFor('WITH prefix_counts'), /LIMIT 10/i);

  const decodedPathSql = sqlFor('fully_decoded_multibyte_24h');
  assert.match(
    decodedPathSql,
    /n\.name IS NULL OR n\.name NOT LIKE '%🚫%'/,
    'private nodes must never participate in public decoded-path resolution',
  );
});
