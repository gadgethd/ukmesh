import assert from 'node:assert/strict';
import { test } from 'node:test';
import type { QueryResultRow } from 'pg';
import {
  compareAggregateShadowRows,
  compareExactMultibyteRows,
  createStatsRepository,
} from './statsRepository.js';

test('multibyte shadow comparison is exact for totals, nested values, nulls, and ordering', () => {
  const raw = [{
    multibyte_packets_24h: '17',
    latest_fully_decoded_nodes: [{ ord: 1, node_id: 'AA', name: null }],
  }];
  assert.equal(compareExactMultibyteRows(raw, structuredClone(raw)).matched, true);
  assert.equal(compareExactMultibyteRows(
    [{ ...raw[0], multibyte_packets_24h: '18' }],
    raw,
  ).matched, false);
  assert.equal(compareExactMultibyteRows(
    [{ ...raw[0], latest_fully_decoded_nodes: [{ ord: 1, node_id: 'AA', name: '' }] }],
    raw,
  ).matched, false);
  assert.equal(compareExactMultibyteRows([...raw, { extra: true }], raw).matched, false);
});

test('multibyte fact cutover removes both raw decode scans and binds the privacy generation', async () => {
  const calls: Array<{ text: string; params?: unknown[] }> = [];
  const query = async <T extends QueryResultRow = QueryResultRow>(
    text: string,
    params?: unknown[],
  ): Promise<{ rows: T[] }> => {
    calls.push({ text, params });
    if (text.includes('FROM multibyte_path_fact_state')) {
      return { rows: [{ ready: true }] as T[] };
    }
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
    multibyteFactsReadsEnabled: true,
    multibyteFactsShadowEnabled: false,
  });

  await repository.fetchChartsData('ukmesh', undefined, 12);
  const factCalls = calls.filter((call) => call.text.includes('FROM multibyte_path_facts f'));
  assert.equal(factCalls.length, 2);
  assert.ok(factCalls.every((call) => call.params?.at(-1) === 12));
  assert.equal(calls.some((call) => call.text.includes('row_number() OVER () AS obs_id')), false);
});

test('aggregate shadow comparison requires exact keys and applies the documented count tolerance', () => {
  assert.deepEqual(
    compareAggregateShadowRows(
      [{ hour: '2026-07-29T12:00:00Z', count: '10,000' }],
      [{ hour: new Date('2026-07-29T12:00:00Z'), count: '10000' }],
    ),
    { matched: false, maxAbsoluteDifference: Number.POSITIVE_INFINITY, reason: 'count' },
  );
  assert.deepEqual(
    compareAggregateShadowRows(
      [{ hour: '2026-07-29T12:00:00Z', count: '10000' }],
      [{ hour: new Date('2026-07-29T12:00:00Z'), count: '9990' }],
    ),
    { matched: true, maxAbsoluteDifference: 10 },
  );
  assert.deepEqual(
    compareAggregateShadowRows(
      [{ packet_type: 4, count: '100' }],
      [{ packet_type: 4, count: '94' }],
    ),
    { matched: false, maxAbsoluteDifference: 6, reason: 'count' },
  );
  assert.equal(
    compareAggregateShadowRows(
      [{ route_type: 'Unknown', count: '5' }],
      [{ route_type: '0', count: '5' }],
    ).reason,
    'keys',
  );
});

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
});

test('canonical charts coalesce six high-volume dimensions into one maintained aggregate read', async () => {
  const calls: string[] = [];
  const query = async <T extends QueryResultRow = QueryResultRow>(
    text: string,
  ): Promise<{ rows: T[] }> => {
    calls.push(text);
    if (text.includes('FROM packet_hourly_stats')) {
      return {
        rows: [{
          packets_per_hour: [{ hour: '2026-07-29T12:00:00.000Z', count: '3' }],
          packets_per_day: [{ day: '2026-07-29T00:00:00.000Z', count: '3' }],
          packet_types: [{ packet_type: 4, count: '3' }],
          hop_distribution: [{ hops: 2, count: '3' }],
          route_types: [{ route_type: '0', count: '3' }],
          transport_codes: [{ transport_code: 'EU', region_scope: 'UK', count: '3' }],
        }] as T[],
      };
    }
    return { rows: [] };
  };
  const filters = () => ({
    params: ['ukmesh'],
    packets: 'AND network = $1',
    packetsAlias: (alias: string) => `AND ${alias}.network = $1`,
    nodes: 'AND network = $1',
    nodesAlias: (alias: string) => `AND ${alias}.network = $1`,
  });
  const repository = createStatsRepository({
    query,
    networkFilters: filters,
    aggregateReadsEnabled: true,
    aggregateShadowEnabled: false,
  });

  const result = await repository.fetchChartsData('ukmesh', undefined);
  assert.equal(calls.filter((sql) => sql.includes('FROM packet_hourly_stats')).length, 1);
  const aggregateSql = calls.find((sql) => sql.includes('FROM packet_hourly_stats'));
  assert.ok(aggregateSql);
  assert.match(aggregateSql, /p\.time > \$\d+::timestamptz/);
  assert.match(aggregateSql, /p\.time <= \$\d+::timestamptz/);
  assert.doesNotMatch(aggregateSql, /p\.time < .* OR p\.time >=/);
  assert.match(aggregateSql, /SELECT \* FROM rollup_24h\s+UNION ALL\s+SELECT \* FROM raw_24h/);
  assert.equal(
    calls.filter((sql) =>
      sql.includes("time_bucket('1 hour', p.time) AS bucket, COUNT(*)::int AS count")).length,
    0,
  );
  assert.deepEqual(result.ptResult.rows, [{ packet_type: 4, count: '3' }]);
  assert.deepEqual(result.hdResult.rows, [{ hops: 2, count: '3' }]);

  calls.length = 0;
  await repository.fetchChartsData('ukmesh', 'A'.repeat(64));
  assert.equal(calls.filter((sql) => sql.includes('FROM packet_hourly_stats')).length, 0);
  assert.ok(calls.some((sql) =>
    sql.includes("time_bucket('1 hour', p.time) AS bucket, COUNT(*)::int AS count")));
});

test('map summary uses the same coordinate, role, and 14-day freshness rules as the map', async () => {
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

  await repository.fetchStatsSummary('ukmesh', undefined);

  const staleSql = calls.find((call) =>
    call.text.includes("<= NOW() - INTERVAL '14 days'"),
  )?.text;
  const mapSql = calls.find((call) =>
    call.text.includes('nodes.last_path_evidence_at')
    && call.text.includes('nodes.last_rx_at')
    && call.text.includes("> NOW() - INTERVAL '28 days'")
    && !call.text.includes("<= NOW() - INTERVAL '14 days'"),
  )?.text;
  assert.ok(staleSql, 'expected stale-node count query');
  assert.ok(mapSql, 'expected on-map node count query');

  for (const sql of [staleSql, mapSql]) {
    assert.match(sql, /nodes\.lat BETWEEN -90 AND 90/);
    assert.match(sql, /nodes\.lon BETWEEN -180 AND 180/);
    assert.match(sql, /NOT \(ABS\(nodes\.lat\) < 5 AND ABS\(nodes\.lon\) < 5\)/);
    assert.match(sql, /\(nodes\.role IS NULL OR nodes\.role NOT IN \(1, 3\)\)/);
    assert.match(sql, /nodes\.last_seen/);
    assert.match(sql, /nodes\.last_rx_at/);
    assert.match(sql, /nodes\.last_status_at/);
    assert.match(sql, /nodes\.last_path_evidence_at/);
    assert.match(sql, /nodes\.name NOT LIKE/);
    assert.doesNotMatch(sql, /INTERVAL '7 days'/);
  }

  assert.match(staleSql, />\s+NOW\(\) - INTERVAL '28 days'/);
});

test('statistics repository bounds concurrent database queries', async () => {
  const previous = process.env['STATS_DB_QUERY_CONCURRENCY'];
  process.env['STATS_DB_QUERY_CONCURRENCY'] = '2';
  let active = 0;
  let maxActive = 0;
  try {
    const query = async <T extends QueryResultRow = QueryResultRow>(): Promise<{ rows: T[] }> => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      await new Promise((resolve) => setImmediate(resolve));
      active -= 1;
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

    await repository.fetchStatsSummary('ukmesh', undefined);
    assert.equal(maxActive, 2);
  } finally {
    if (previous === undefined) delete process.env['STATS_DB_QUERY_CONCURRENCY'];
    else process.env['STATS_DB_QUERY_CONCURRENCY'] = previous;
  }
});
