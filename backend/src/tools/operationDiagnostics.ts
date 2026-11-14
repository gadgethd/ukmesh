import type pg from 'pg';
import type { LookupSourceLoader } from './lookupBenchmarkSession.js';

type Series = { metric: string; labels: Record<string, string | number>; value: number };
type Histogram = { get: () => Promise<{ values: Array<{ metricName?: string; labels: Record<string, string | number>; value: number }> }> };
const key = (row: Series) => row.metric + JSON.stringify(Object.entries(row.labels).sort(([a], [b]) => a.localeCompare(b)));

export function metricDifferences(before: Series[], after: Series[]): Series[] {
  const old = new Map(before.map(row => [key(row), row.value]));
  const current = new Map(after.map(row => [key(row), row]));
  for (const row of before) if (!current.has(key(row))) throw new Error('operation metric series disappeared');
  return after.map(row => {
    const value = row.value - (old.get(key(row)) ?? 0);
    if (!Number.isFinite(value) || value < 0) throw new Error('operation metric counter regressed');
    return { ...row, value };
  });
}

/** Read the same source runtime's existing counters; never reset or register. */
export async function createOperationDiagnostics(load: LookupSourceLoader, pools: { pool: pg.Pool; analyticsPool: pg.Pool; ingestPool: pg.Pool }) {
  const existing = await load('metrics/database.ts');
  const histograms: Histogram[] = [existing.dbPoolWaitDuration, existing.dbExecutionDuration, existing.dbQueryDuration];
  if (histograms.some(value => typeof value?.get !== 'function')) throw new Error('selected-source database diagnostics unavailable');
  const names = ['pool', 'analyticsPool', 'ingestPool'] as const;
  const profile = Object.fromEntries(names.map(name => [name,
    Object.fromEntries(['max', 'idleTimeoutMillis', 'connectionTimeoutMillis', 'statement_timeout', 'query_timeout']
      .map(option => [option, (pools[name].options as unknown as Record<string, unknown>)[option]]))]));
  return {
    profile,
    pools: () => Object.fromEntries(names.map(name => [name, { total: pools[name].totalCount,
      idle: pools[name].idleCount, waiting: pools[name].waitingCount }])),
    async metrics(): Promise<Series[]> {
      return (await Promise.all(histograms.map(async histogram => (await histogram.get()).values
        .filter(row => row.metricName?.endsWith('_sum') || row.metricName?.endsWith('_count'))
        .map(row => ({ metric: row.metricName!, labels: row.labels, value: row.value }))))).flat();
    },
  };
}
export type OperationDiagnostics = Awaited<ReturnType<typeof createOperationDiagnostics>>;
