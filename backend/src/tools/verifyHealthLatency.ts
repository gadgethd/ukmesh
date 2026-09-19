import { readFileSync, writeFileSync } from 'node:fs';
import { performance } from 'node:perf_hooks';
import { createHash } from 'node:crypto';
import type { QueryResultRow } from 'pg';
import { query, closeDb } from '../db/index.js';
import { MQTT_NODES_SQL } from '../repositories/mqttNodes.js';
import { getAnalysisWorkloadStates } from '../analysis/runState.js';
import { loadSpamMessageConfig } from '../spam/config.js';
import { loadRecentMessages } from '../spam/repository.js';
import { buildIncidentsWithPaths } from '../spam/analyzer.js';
import { clusterMessages } from '../spam/cluster.js';

const evidence = '../docs/evidence/health-latency-20260919/';
const mode = process.argv[2];
try {
  const safety = await query<{ read_only: string }>("SELECT current_setting('default_transaction_read_only') AS read_only");
  if (safety.rows[0]?.read_only !== 'on') throw new Error('Read-only connection required');
  if (mode === 'mqtt') {
    const networks = ['ukmesh', 'northeast', 'teesside'];
    const before = readFileSync(`${evidence}mqtt-nodes-before.sql`, 'utf8').trim().replace(/;$/, '');
    const results: unknown[] = [];
    for (let trial = 1; trial <= 3; trial += 1) {
      for (const [label, sql, params] of [
        ['before', before, []], ['after', MQTT_NODES_SQL, [networks, networks]],
      ] as const) {
        const start = performance.now();
        const result = await query(`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${sql}`, [...params]);
        const plan = result.rows[0]!['QUERY PLAN'];
        writeFileSync(`${evidence}mqtt-${label}-${trial}.plan.json`, JSON.stringify(plan, null, 2) + '\n');
        results.push({ trial, label, wallMs: performance.now() - start, executionMs: plan[0]['Execution Time'], rows: plan[0].Plan['Actual Rows'] });
      }
    }
    const equality = await query(`WITH original AS MATERIALIZED (${before}), fixed AS MATERIALIZED (${MQTT_NODES_SQL})
      SELECT (SELECT COUNT(*) FROM original) AS original_rows,
             (SELECT COUNT(*) FROM fixed) AS fixed_rows,
             (SELECT COUNT(*) FROM ((SELECT to_jsonb(o) FROM original o EXCEPT ALL SELECT to_jsonb(f) FROM fixed f)
               UNION ALL (SELECT to_jsonb(f) FROM fixed f EXCEPT ALL SELECT to_jsonb(o) FROM original o)) differences) AS differences`, [networks, networks]);
    console.log(JSON.stringify({ capturedAt: new Date().toISOString(), results, equality: equality.rows[0] }, null, 2));
    if (equality.rows[0]?.['differences'] !== '0') process.exitCode = 2;
  } else if (mode === 'spam') {
    const cfg = loadSpamMessageConfig();
    const start = performance.now();
    const records = await loadRecentMessages(cfg);
    const loaded = performance.now();
    const queries: Array<{ sql: string; ms: number; rows: number }> = [];
    const probeQuery = async <T extends QueryResultRow>(text: string, params?: unknown[]) => {
      const started = performance.now();
      const result = await query<T>(text, params);
      queries.push({ sql: String(text).trim().split('\n')[0]!, ms: performance.now() - started, rows: result.rows.length });
      return result;
    };
    const items = await buildIncidentsWithPaths(records, Date.now(), probeQuery, cfg);
    const finished = performance.now();
    let comparison: unknown;
    if (process.env['HEALTH_AUDIT_COMPARE_SPAM'] === '1') {
      const baselinePath = '../../../.health-latency-local/spam-before/cluster.ts';
      const baseline = await import(baselinePath) as { clusterMessages: typeof clusterMessages };
      const oldStart = performance.now();
      const oldIncidents = baseline.clusterMessages(records, cfg);
      const oldEnd = performance.now();
      const newIncidents = clusterMessages(records, cfg);
      const newEnd = performance.now();
      const digest = (value: unknown) => createHash('sha256').update(JSON.stringify(value)).digest('hex');
      const equal = digest(oldIncidents) === digest(newIncidents);
      comparison = { oldClusterMs: oldEnd - oldStart, newClusterMs: newEnd - oldEnd, equal, oldDigest: digest(oldIncidents), newDigest: digest(newIncidents) };
      if (!equal) process.exitCode = 2;
    }
    console.log(JSON.stringify({ capturedAt: new Date().toISOString(), messages: records.length, incidents: items.length,
      loadMs: loaded - start, buildMs: finished - loaded, totalMs: finished - start, budgetMs: cfg.analysisBudgetMs,
      withinBudget: finished - start < cfg.analysisBudgetMs, queries, comparison, persisted: false }, null, 2));
  } else if (mode === 'analysis') {
    console.log(JSON.stringify(await getAnalysisWorkloadStates(), null, 2));
  } else throw new Error('Unknown probe');
} catch (error) {
  // Do not echo query parameters, connection URLs or message payloads.
  console.error('Read-only probe failed:', error instanceof Error ? error.name : 'unknown error');
  process.exitCode = 1;
} finally {
  await closeDb();
}
