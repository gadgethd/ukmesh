/** Owned-fixture HTTP/SQL qualification using the actual public route handlers.
 * No fixture writes, .env loading, cache-policy changes or remote requests.
 */
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { mkdir, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import pg from 'pg';
import { option } from './sampleData.js';
import { configureLookupReadOnlyEnvironment, lookupBenchmarkOptions, withLookupSource } from './lookupBenchmarkSession.js';
import { LOOKUP_RESPONSE_VALIDATION_LIMITS } from './lookupResponseVerifier.js';
import { lookupSourceIdentity } from './lookupSource.js';
import { LookupTimeline } from './lookupTimeline.js';
import { runLookupSamples } from './lookupHttpTransport.js';
import { createLookupRequestFleet } from './lookupHttpRequests.js';
import { loadLookupHttpWorkload } from './lookupHttpWorkload.js';

const args = process.argv.slice(2);
const { source, output, samples, concurrency, trials, cache, budgetMs } = lookupBenchmarkOptions(args);
const timeline = new LookupTimeline();
let completed = false;
try {
await timeline.span({ phase: 'benchmark' }, async () => {
const sourceIdentity = await lookupSourceIdentity(source, option(args, 'reference-revision') ?? null);
configureLookupReadOnlyEnvironment(process.env['DATABASE_URL'] ?? '', process.env);

await withLookupSource(source, async (load, runtime) => {
  const workloadRoutes = await loadLookupHttpWorkload(load, runtime);
  const pool = runtime.pool as pg.Pool;
  const query = pool.query.bind(pool);
  type CapturedQuery = { workload: string; sql: string; params: unknown[]; calls: number; totalMs: number };
  const captured = new Map<string, CapturedQuery>();
  let workload = 'fixture-probe';
  // Observe checked-out clients as well as pool.query, preserving pg's callback
  // and promise overloads. All statements remain byte-for-byte production SQL.
  pool.on('connect', client => {
    const execute = client.query.bind(client) as (...values: unknown[]) => unknown;
    client.query = ((...values: unknown[]) => {
      const spec = values[0] as string | pg.QueryConfig;
      const text = typeof spec === 'string' ? spec : spec.text;
      if (!/^\s*(?:\/\*[\s\S]*?\*\/\s*)?(SELECT|WITH|SHOW|EXPLAIN)\b/i.test(text)) {
        throw new Error('lookup benchmark refuses non-read SQL');
      }
      const started = performance.now();
      const record = () => {
        if (/^EXPLAIN\b/i.test(text)) return;
        const key = workload + '\0' + text;
        const previous = captured.get(key);
        if (previous) { previous.calls++; previous.totalMs += performance.now() - started; }
        else captured.set(key, { workload, sql: text,
          params: typeof spec !== 'string' ? spec.values ?? [] : Array.isArray(values[1]) ? values[1] : [],
          calls: 1, totalMs: performance.now() - started });
      };
      const callback = values.at(-1);
      if (typeof callback === 'function') {
        values[values.length - 1] = (...result: unknown[]) => { record(); callback(...result); };
        return execute(...values);
      }
      return (execute(...values) as Promise<unknown>).finally(record);
    }) as typeof client.query;
  });

  const percentile = (values: number[], quantile: number) => {
    const sorted = [...values].sort((a, b) => a - b);
    return sorted[Math.ceil(sorted.length * quantile) - 1]!;
  };
  const fleet = createLookupRequestFleet();
  type LookupResult = { trial: number; name: string; samples: number; p50Ms: number; p95Ms: number; p99Ms: number;
    bytes: number; entries: number | null; digests: string[]; dataDigests: string[] };
  const report: LookupResult[] = [];

  try {
    const { counts, snapshot, cases, regionEnabled } = await workloadRoutes.prepare(option(args, 'snapshot'));
    const selected = option(args, 'cases')?.split(',');
    if (selected?.some(name => !cases.some(item => item.name === name))) throw new Error('unknown lookup case');
    const cpuBefore = process.cpuUsage(), resourceStarted = performance.now();
    for (let trial = 0; trial < trials; trial++) {
      await timeline.span({ phase: 'trial', trial: trial + 1 }, () => workloadRoutes.withTrial(async (port, caches) => {
        const { nodeLinksCache, inferredNodesCache } = caches;
        for (const definition of cases.filter(item => !selected || selected.includes(item.name))) {
          await timeline.span({ phase: 'case', trial: trial + 1, case: definition.name }, async () => {
          const testCase = { ...definition };
          // Long baseline runs must not expire a valid pagination snapshot.
          // Record a fresh request snapshot per case; it is fixed within a trial.
          if (!option(args, 'snapshot') && testCase.route.startsWith('/api/nodes/map?')) {
            const url = new URL(testCase.route, 'http://127.0.0.1');
            url.searchParams.set('snapshot', new Date().toISOString());
            testCase.route = url.pathname + url.search;
          }
          workload = testCase.name;
          await timeline.span({ phase: 'warmup', trial: trial + 1, case: testCase.name }, async () => {
            for (let warmup = 0; warmup < 3; warmup++) await fleet.request(port, testCase);
          });
          const results = await timeline.span({ phase: 'samples', trial: trial + 1, case: testCase.name },
            () => runLookupSamples(samples, concurrency, () => {
              if (cache === 'miss') { nodeLinksCache.clear(); inferredNodesCache.clear(); }
              return fleet.request(port, testCase);
            }));
          const result = { trial: trial + 1, name: testCase.name, samples: results.length,
            p50Ms: percentile(results.map(row => row.ms), .5), p95Ms: percentile(results.map(row => row.ms), .95),
            p99Ms: percentile(results.map(row => row.ms), .99), bytes: results[0]!.bytes,
            entries: results[0]!.entries, digests: [...new Set(results.map(row => row.digest))],
            dataDigests: [...new Set(results.map(row => row.dataDigest))] };
          report.push(result); console.log(JSON.stringify(result));
          });
        }
      }));
    }
    const resources = { wallMs: performance.now() - resourceStarted, cpuMicros: process.cpuUsage(cpuBefore),
      memory: process.memoryUsage(), responseValidation: await fleet.readStatistics(),
      note: 'Handler and loopback HTTP timing share this process; one bounded worker validates each completed response. Validation counters are collected after timing. SQL plans exclude this overhead.' };
    const plans = [];
    // Explain outside request timing. These are the real SQL strings and binds.
    if (args.includes('--plans')) for (const item of [...captured.values()].sort((a, b) => b.totalMs - a.totalMs)) {
      const explained = await query(`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${item.sql}`, item.params);
      plans.push({ workload: item.workload, sql: item.sql, calls: item.calls, totalMs: item.totalMs, plan: explained.rows[0]['QUERY PLAN'][0] });
    }
    await mkdir(output, { recursive: true });
    const sourceIdentityAfter = await lookupSourceIdentity(source, sourceIdentity.revision);
    const sourceStable = JSON.stringify(sourceIdentityAfter) === JSON.stringify(sourceIdentity);
    const passed = sourceStable && (budgetMs === undefined || report.every(result => result.p95Ms <= budgetMs));
    await writeFile(path.join(output, 'lookups.json'), JSON.stringify({ version: 1,
      at: new Date().toISOString(), snapshot, source, sourceIdentity, sourceIdentityAfter, sourceStable, passed,
      commit: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(),
      diffSha256: createHash('sha256').update(execFileSync('git', ['diff', 'HEAD'])).digest('hex'),
      hardware: { cpus: os.cpus().length, model: os.cpus()[0]?.model, memory: os.totalmem() },
      database: (await query('SELECT version() version')).rows[0], counts, resources,
      databaseSettings: (await query(`SELECT name,setting,unit FROM pg_settings WHERE name IN
        ('shared_buffers','work_mem','max_connections','max_parallel_workers_per_gather','statement_timeout') ORDER BY name`)).rows,
      settings: { samples, trials, concurrency, cache, clients: fleet.clientCount, productionRateLimits: true, regionEnabled,
        responseValidation: LOOKUP_RESPONSE_VALIDATION_LIMITS,
        scope: 'ukmesh', note: 'Actual public routers, pool size and privacy delivery gates; excludes unrelated background services. Cache-miss bursts may coalesce identical work.' },
      timeline: 'timeline.json', results: report, queries: [...captured.values()].map(({ params: _params, ...rest }) => rest), plans,
    }, null, 2) + '\n');
    if (!sourceStable) throw new Error('lookup source changed during the benchmark; see saved evidence');
    if (budgetMs !== undefined && report.some(result => result.p95Ms > budgetMs)) {
      throw new Error(`lookup p95 budget ${budgetMs}ms exceeded; see saved evidence`);
    }
  } finally {
    await fleet.close();
  }
});
});
completed = true;
} finally {
  await mkdir(output, { recursive: true });
  await writeFile(path.join(output, 'timeline.json'), JSON.stringify({ ...timeline.finish(completed),
    source, settings: { samples, concurrency, trials, cache },
    note: 'Actual native boundaries outside HTTP timers; no per-request events or inferred intervals.' }, null, 2) + '\n', { flag: 'wx' });
}
