import assert from 'node:assert/strict';
import { test } from 'node:test';
import type { QueryResultRow } from 'pg';
import {
  bindChartSnapshot,
  compareAggregateShadowRows,
  createStatsRepository,
} from './statsRepository.js';

test('pins repeated chart NOW expressions to one timestamp and parameter', () => {
  const asOf = new Date('2026-09-23T10:15:00.000Z');
  const snapshot = bindChartSnapshot(
    "SELECT * FROM packets WHERE time > NOW() - INTERVAL '24 hours' AND time <= NOW()",
    ['ukmesh'],
    asOf,
  );
  assert.equal(
    snapshot.text,
    "SELECT * FROM packets WHERE time > $2::timestamptz - INTERVAL '24 hours' AND time <= $2::timestamptz",
  );
  assert.deepEqual(snapshot.params, ['ukmesh', asOf.toISOString()]);
});

test('multibyte fact cutover removes both raw decode scans and binds the privacy generation', async () => {
  const calls: Array<{ text: string; params?: unknown[] }> = [];
  const query = async <T extends QueryResultRow = QueryResultRow>(
    text: string,
    params?: unknown[],
  ): Promise<{ rows: T[] }> => {
    calls.push({ text, params });
    if (/FROM (?:mesh\.)?multibyte_path_fact_state/.test(text)) {
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
  });

  await repository.fetchChartsData('ukmesh', undefined, 12);
  const factCalls = calls.filter((call) => /FROM (?:mesh\.)?multibyte_path_facts f/.test(call.text));
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
  const multibyteSql = sqlFor('fully_decoded_multibyte_24h');
  const fullWindowAggregates = [
    sqlFor('prefix_counts'),
    sqlFor('hash_hex_len'),
    sqlFor('avg_observers'),
    sqlFor('median_rssi'),
  ];

  for (const sql of fullWindowAggregates) {
    assert.doesNotMatch(sql, /\bLIMIT\s+50000\b/i);
    assert.match(sql, /AND p\.network = \$1/);
  }
  assert.doesNotMatch(multibyteSql, /\bLIMIT\s+50000\b/i);
  assert.match(multibyteSql, /AND f\.network = \$1/);

  const signalSql = sqlFor('median_rssi');
  assert.match(signalSql, /percentile_cont\(0\.5\)\s+WITHIN GROUP\s+\(ORDER BY p\.rssi\)::text\s+AS median_rssi/i);
  assert.match(signalSql, /percentile_cont\(0\.5\)\s+WITHIN GROUP\s+\(ORDER BY p\.snr\)::text\s+AS median_snr/i);
  assert.doesNotMatch(signalSql, /AVG\(p\.rssi\)::text\s+AS median_rssi/i);
  assert.doesNotMatch(signalSql, /AVG\(p\.snr\)::text\s+AS median_snr/i);
  assert.match(sqlFor('prefix_counts'), /LIMIT 10/i);
  assert.equal(
    calls.some((call) => call.text.includes('meshcore_canonical_node_id')),
    false,
    'chart queries must canonicalize through the identity alias relation',
  );
  assert.ok(calls.some((call) => call.text.includes('LEFT JOIN core.node_identity_aliases')));
});

test('canonical charts coalesce six high-volume dimensions into one maintained aggregate read', async () => {
  const calls: string[] = [];
  const query = async <T extends QueryResultRow = QueryResultRow>(
    text: string,
  ): Promise<{ rows: T[] }> => {
    calls.push(text);
    if (text.includes('FROM mesh.packet_hourly_stats')) {
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
  assert.equal(calls.filter((sql) => sql.includes('FROM mesh.packet_hourly_stats')).length, 1);
  const aggregateSql = calls.find((sql) => sql.includes('FROM mesh.packet_hourly_stats'));
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
  assert.equal(calls.filter((sql) => sql.includes('FROM mesh.packet_hourly_stats')).length, 0);
  assert.ok(calls.some((sql) =>
    sql.includes("time_bucket('1 hour', p.time) AS bucket, COUNT(*)::int AS count")));
});

test('legacy chart refresh consolidates same-snapshot scans and keeps the aggregate switch', async () => {
  const calls: string[] = [];
  const query = async <T extends QueryResultRow = QueryResultRow>(
    text: string,
  ): Promise<{ rows: T[] }> => {
    calls.push(text);
    if (/FROM (?:mesh\.)?multibyte_path_fact_state/.test(text)) {
      return { rows: [{ ready: true }] as T[] };
    }
    if (text.includes('FROM mesh.packet_hourly_stats')) {
      return {
        rows: [{
          packets_per_hour: [], packets_per_day: [], packet_types: [],
          hop_distribution: [], route_types: [], transport_codes: [],
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
  const repository = createStatsRepository({ query, networkFilters: filters });
  await repository.fetchChartsData('ukmesh', undefined);
  // The six switchable parts share one legacy counts scan; radios+totals,
  // path hashes and region/reception each scan once. Four packet scans total
  // replace the original fifteen, plus the two multibyte fact reads.
  const legacyCountsFragment = "time_bucket('1 hour', p.time) AS bucket, COUNT(*)::int AS count";
  assert.equal(calls.filter((sql) => sql.includes(legacyCountsFragment)).length, 1);
  assert.equal(calls.filter((sql) => sql.includes('AS radios_per_hour')).length, 1);
  assert.equal(calls.filter((sql) => sql.includes('AS prefix_collisions')).length, 1);
  assert.equal(calls.filter((sql) => sql.includes('AS region_summary')).length, 1);
  assert.equal(calls.filter((sql) => /FROM mesh\.packets p/.test(sql)).length, 4);
  assert.equal(calls.filter((sql) => /FROM (?:mesh\.)?multibyte_path_facts f/.test(sql)).length, 2);

  calls.length = 0;
  const readRepository = createStatsRepository({
    query, networkFilters: filters, aggregateReadsEnabled: true,
  });
  await readRepository.fetchChartsData('ukmesh', undefined);
  // Aggregate reads must never start the lazy legacy counts scan.
  assert.equal(calls.filter((sql) => sql.includes(legacyCountsFragment)).length, 0);
  assert.equal(calls.filter((sql) => sql.includes('FROM mesh.packet_hourly_stats')).length, 1);
  assert.equal(calls.filter((sql) => sql.includes('AS radios_per_hour')).length, 1);
  assert.equal(calls.filter((sql) => sql.includes('AS region_summary')).length, 1);
});

test('per-network shadowing runs before reads are enabled and stays off for other networks', async () => {
  const calls: Array<{ text: string; params?: unknown[] }> = [];
  const query = async <T extends QueryResultRow = QueryResultRow>(
    text: string,
    params?: unknown[],
  ): Promise<{ rows: T[] }> => {
    calls.push({ text, params });
    return { rows: [] };
  };
  const shadowRepository = createStatsRepository({
    query,
    networkFilters: () => ({
      params: ['ukmesh'],
      packets: 'AND network = $1',
      packetsAlias: (alias: string) => `AND ${alias}.network = $1`,
      nodes: 'AND network = $1',
      nodesAlias: (alias: string) => `AND ${alias}.network = $1`,
    }),
    aggregateShadowNetworks: ['ukmesh'],
  });

  await shadowRepository.fetchChartsData('ukmesh', undefined);
  assert.ok(calls.some((call) => call.text.includes('FROM mesh.packet_hourly_stats')));
  assert.ok(calls.some((call) => call.text.includes('WITH scoped AS MATERIALIZED')));
  assert.ok(calls.every((call) => !/\bNOW\(\)/i.test(call.text)));

  calls.length = 0;
  const readRepository = createStatsRepository({
    query,
    networkFilters: () => ({
      params: ['ukmesh'],
      packets: 'AND network = $1',
      packetsAlias: (alias: string) => `AND ${alias}.network = $1`,
      nodes: 'AND network = $1',
      nodesAlias: (alias: string) => `AND ${alias}.network = $1`,
    }),
    aggregateReadNetworks: ['ukmesh'],
  });
  await readRepository.fetchChartsData('ukmesh', undefined);
  assert.ok(calls.some((call) => call.text.includes('FROM mesh.packet_hourly_stats')));

  calls.length = 0;
  await readRepository.fetchChartsData('test', undefined);
  assert.equal(calls.some((call) => call.text.includes('FROM mesh.packet_hourly_stats')), false);
});

test('Compose aggregate switch accepts a comma-separated network value', async () => {
  const previousReads = process.env['STATS_AGGREGATE_READS_ENABLED'];
  const previousShadow = process.env['STATS_AGGREGATE_SHADOW_ENABLED'];
  process.env['STATS_AGGREGATE_READS_ENABLED'] = 'ukmesh';
  process.env['STATS_AGGREGATE_SHADOW_ENABLED'] = 'false';
  try {
    const calls: string[] = [];
    const query = async <T extends QueryResultRow = QueryResultRow>(
      text: string,
    ): Promise<{ rows: T[] }> => {
      calls.push(text);
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
    assert.ok(calls.some((sql) => sql.includes('FROM mesh.packet_hourly_stats')));

    calls.length = 0;
    await repository.fetchChartsData('test', undefined);
    assert.equal(calls.some((sql) => sql.includes('FROM mesh.packet_hourly_stats')), false);
  } finally {
    if (previousReads === undefined) delete process.env['STATS_AGGREGATE_READS_ENABLED'];
    else process.env['STATS_AGGREGATE_READS_ENABLED'] = previousReads;
    if (previousShadow === undefined) delete process.env['STATS_AGGREGATE_SHADOW_ENABLED'];
    else process.env['STATS_AGGREGATE_SHADOW_ENABLED'] = previousShadow;
  }
});

test('summary shares one scoped identity read and retains its result rows', async () => {
  const calls: Array<{ text: string; params?: unknown[] }> = [];
  const query = async <T extends QueryResultRow = QueryResultRow>(
    text: string, params?: unknown[],
  ): Promise<{ rows: T[] }> => {
    calls.push({ text, params });
    const rows = text.includes('WITH scoped_nodes AS MATERIALIZED') ? [{
      stale_count: '3', map_node_count: '8', total_node_count: '12',
      count_connected: '0', total_adverts: null, last_seen_at: null, last_lat: null, last_lon: null,
    }] : [];
    return { rows: rows as T[] };
  };
  const repository = createStatsRepository({
    query,
    networkFilters: () => ({
      params: ['ukmesh'], packets: 'AND network = $1',
      packetsAlias: (alias: string) => `AND ${alias}.network = $1`,
      nodes: 'AND network = $1', nodesAlias: (alias: string) => `AND ${alias}.network = $1`,
    }),
  });
  const result = await repository.fetchStatsSummary('ukmesh', undefined);
  const identityReads = calls.filter(call => call.text.includes('WITH scoped_nodes AS MATERIALIZED'));
  assert.equal(identityReads.length, 1, 'node categories must share the expensive identity read');
  assert.equal(identityReads[0].text.includes('FROM core.node_identity_nodes'), false,
    'shared counts must avoid the wide canonical view');
  assert.deepEqual(identityReads[0].params, ['ukmesh']);
  assert.deepEqual(result.staleCount.rows, [{ count: '3' }]);
  assert.deepEqual(result.mapNodeCount.rows, [{ count: '8' }]);
  assert.deepEqual(result.totalNodeCount.rows, [{ count: '12' }]);
  assert.deepEqual(result.internationalCount.rows, [{
    count_connected: '0', total_adverts: null, last_seen_at: null, last_lat: null, last_lon: null,
  }]);
});

for (const concurrency of [1, 2]) {
  test(`statistics repository bounds concurrent database queries at ${concurrency}`, async () => {
    const previous = process.env['STATS_DB_QUERY_CONCURRENCY'];
    process.env['STATS_DB_QUERY_CONCURRENCY'] = String(concurrency);
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
      assert.equal(maxActive, concurrency);
    } finally {
      if (previous === undefined) delete process.env['STATS_DB_QUERY_CONCURRENCY'];
      else process.env['STATS_DB_QUERY_CONCURRENCY'] = previous;
    }
  });
}


test('consolidated chart responses enforce the budget on each logical result', async () => {
  const previous = process.env.STATS_CHART_MAX_RESULT_ROWS;
  const previousConcurrency = process.env.STATS_DB_QUERY_CONCURRENCY;
  process.env.STATS_CHART_MAX_RESULT_ROWS = '1';
  process.env.STATS_DB_QUERY_CONCURRENCY = '8';
  try {
    let count = 1;
    let siblingAborted = false;
    const query = async <T extends QueryResultRow>(
      sql: string,
      _params?: unknown[],
      signal?: AbortSignal,
    ): Promise<{ rows: T[] }> => {
      if (count > 1 && sql.includes('WITH hashes AS MATERIALIZED')) {
        assert.ok(signal);
        return new Promise((_, reject) => {
          signal.addEventListener('abort', () => {
            siblingAborted = true;
            reject(signal.reason);
          }, { once: true });
        });
      }
      if (!sql.includes('AS packets_per_hour') || !sql.includes('WITH scoped AS MATERIALIZED')) {
        return { rows: [] };
      }
      const rows = Array.from({ length: count }, (_, hour) => ({ hour, count: 1 }));
      return { rows: [{ packets_per_hour: rows, packets_per_day: rows }] as unknown as T[] };
    };
    const repo = createStatsRepository({
      query,
      aggregateReadsEnabled: false,
      aggregateShadowEnabled: false,
      networkFilters: () => ({
        params: [], packets: '', packetsAlias: () => '', nodes: '', nodesAlias: () => '',
      }),
    });
    const result = await repo.fetchChartsData('ukmesh');
    assert.equal(result.phResult.rows.length, 1);
    assert.equal(result.pdResult.rows.length, 1);
    count = 2;
    await assert.rejects(repo.fetchChartsData('ukmesh'), /STATS_QUERY_BUDGET_EXCEEDED/);
    assert.equal(siblingAborted, true);
  } finally {
    if (previous === undefined) delete process.env.STATS_CHART_MAX_RESULT_ROWS;
    else process.env.STATS_CHART_MAX_RESULT_ROWS = previous;
    if (previousConcurrency === undefined) delete process.env.STATS_DB_QUERY_CONCURRENCY;
    else process.env.STATS_DB_QUERY_CONCURRENCY = previousConcurrency;
  }
});
