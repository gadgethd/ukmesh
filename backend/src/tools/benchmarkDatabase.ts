/** Read-only plans for the actual public summary/observer/topology SQL. */
import { execFileSync } from 'node:child_process';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import pg from 'pg';
import type { createTopologyLoader as TopologyLoader } from '../api/routes/topology.js';
import { assertLocalDatabaseUrl, option } from './sampleData.js';

const endpoint = process.env['DATABASE_URL'] ?? '';
assertLocalDatabaseUrl(endpoint);
const args = process.argv.slice(2);
const output = path.resolve(option(args, 'output') ?? `../plans/measurements/database-${Date.now()}`);
const source = path.resolve(option(args, 'source-root') ?? 'src');
const { createStatsRepository } = await import(pathToFileURL(path.join(source, 'stats/statsRepository.ts')).href);
const { createTopologyLoader } = await import(pathToFileURL(path.join(source, 'api/routes/topology.ts')).href);
const { networkFilters } = await import(pathToFileURL(path.join(source, 'platform/query/networkFilters.ts')).href);
const selected = option(args, 'cases')?.split(',') ?? ['summary', 'observer', 'topology'];
if (selected.some(value => !['summary', 'observer', 'topology'].includes(value))) throw new Error('unknown SQL workload');
const pool = new pg.Pool({ connectionString: endpoint, max: 1 });
const client = await pool.connect();
const plans: { query: string; rows: number; plan: Record<string, unknown>; executionMs: number }[] = [];
let topology: ReturnType<typeof TopologyLoader> | undefined;
let counts: Record<string, unknown> | undefined;
let failure: unknown;
try {
  await client.query('BEGIN READ ONLY');
  await client.query("SET LOCAL statement_timeout = '30s'");
  counts = (await client.query(`SELECT
    (SELECT COUNT(*)::text FROM mesh.packets) AS packets,
    (SELECT COUNT(*)::text FROM core.nodes) AS nodes,
    (SELECT COUNT(*)::text FROM mesh.packet_paths) AS paths`)).rows[0];
  if (!counts || Number(counts.packets) < 1 || Number(counts.nodes) < 1) throw new Error('prepared populated fixture required');
  const query = async <T extends pg.QueryResultRow>(sql: string, params?: unknown[]) => {
    const result = args.includes('--plan-only') ? { rows: [] as T[] } : await client.query<T>(sql, params);
    if (args.includes('--plan-only')) {
      const explained = await client.query(`EXPLAIN (FORMAT JSON) ${sql}`, params);
      plans.push({ query: sql, rows: 0, plan: explained.rows[0]['QUERY PLAN'][0], executionMs: 0 });
      return result;
    }
    const explained = await client.query(`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${sql}`, params);
    const plan = explained.rows[0]['QUERY PLAN'][0];
    plans.push({ query: sql, rows: result.rows.length, plan, executionMs: Number(plan['Execution Time']) });
    return { rows: result.rows };
  };
  const repository = createStatsRepository({ query, networkFilters });
  if (selected.includes('summary')) await repository.fetchStatsSummary('ukmesh', undefined);
  if (selected.includes('observer')) await repository.fetchObserverActivity('ukmesh');
  if (selected.includes('topology')) {
    const loader: ReturnType<typeof TopologyLoader> = createTopologyLoader({ query, networkFilters, getPublicVisibilityGeneration: async () => 1 });
    topology = loader;
    const graph = await loader.load('ukmesh', 300);
    if (!args.includes('--plan-only') && !graph.nodes.length) throw new Error('topology fixture must contain connected nodes');
  }
} catch (error) {
  failure = error;
} finally {
  topology?.shutdown();
  await client.query('ROLLBACK').catch(() => undefined);
  client.release();
  await pool.end();
  plans.sort((a, b) => b.executionMs - a.executionMs);
  await mkdir(output, { recursive: true });
  await writeFile(path.join(output, 'plans.json'), JSON.stringify({
    version: 1, at: new Date().toISOString(), node: process.version,
    commit: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(),
    settings: { readOnly: true, source, selected, planOnly: args.includes('--plan-only'), scope: 'ukmesh', concurrency: 1, note: 'EXPLAIN execution time is SQL diagnostic timing, not API latency; SELECTs execute once before their plans' },
    counts, plans, error: failure instanceof Error ? failure.message : failure ? String(failure) : null,
  }, null, 2) + '\n');
  console.log(`Recorded ${plans.length} actual read-only query plans: ${output}`);
}
if (failure) throw failure;
