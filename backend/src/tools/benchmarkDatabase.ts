/** Read-only plans for the actual public summary/observer/topology SQL. */
import { execFileSync } from 'node:child_process';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import pg from 'pg';
import { createStatsRepository } from '../stats/statsRepository.js';
import { createTopologyLoader } from '../api/routes/topology.js';
import { networkFilters } from '../platform/query/networkFilters.js';
import { assertLocalDatabaseUrl, option } from './sampleData.js';

const endpoint = process.env['DATABASE_URL'] ?? '';
assertLocalDatabaseUrl(endpoint);
const output = path.resolve(option(process.argv.slice(2), 'output') ?? `../plans/measurements/database-${Date.now()}`);
const pool = new pg.Pool({ connectionString: endpoint, max: 1 });
const client = await pool.connect();
const plans: { query: string; rows: number; plan: Record<string, unknown>; executionMs: number }[] = [];
let topology: ReturnType<typeof createTopologyLoader> | undefined;
try {
  await client.query('BEGIN READ ONLY');
  await client.query("SET LOCAL statement_timeout = '30s'");
  const counts = (await client.query(`SELECT
    (SELECT COUNT(*)::text FROM mesh.packets) AS packets,
    (SELECT COUNT(*)::text FROM core.nodes) AS nodes,
    (SELECT COUNT(*)::text FROM mesh.packet_paths) AS paths`)).rows[0];
  if (Number(counts.packets) < 1 || Number(counts.nodes) < 1) throw new Error('prepared populated fixture required');
  const query = async <T extends pg.QueryResultRow>(sql: string, params?: unknown[]) => {
    const result = await client.query<T>(sql, params);
    const explained = await client.query(`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${sql}`, params);
    const plan = explained.rows[0]['QUERY PLAN'][0];
    plans.push({ query: sql, rows: result.rows.length, plan, executionMs: Number(plan['Execution Time']) });
    return { rows: result.rows };
  };
  const repository = createStatsRepository({ query, networkFilters });
  await repository.fetchStatsSummary('ukmesh', undefined);
  await repository.fetchObserverActivity('ukmesh');
  topology = createTopologyLoader({ query, networkFilters, getPublicVisibilityGeneration: async () => 1 });
  const graph = await topology.load('ukmesh', 300);
  if (!graph.nodes.length) throw new Error('topology fixture must contain connected nodes');
  if (plans.length < 5) throw new Error('expected at least five actual public query plans');
  plans.sort((a, b) => b.executionMs - a.executionMs);
  await mkdir(output, { recursive: true });
  await writeFile(path.join(output, 'plans.json'), JSON.stringify({
    version: 1, at: new Date().toISOString(), node: process.version,
    commit: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(),
    settings: { readOnly: true, scope: 'ukmesh', concurrency: 1, note: 'EXPLAIN execution time is SQL diagnostic timing, not API latency; SELECTs execute once before their plans' },
    counts, plans,
  }, null, 2) + '\n');
  console.log(`Recorded ${plans.length} actual read-only query plans: ${output}`);
} finally {
  topology?.shutdown();
  await client.query('ROLLBACK').catch(() => undefined);
  client.release();
  await pool.end();
}
