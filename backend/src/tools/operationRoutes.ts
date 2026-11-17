import express, { Router } from 'express';
import { withLookupSource, withLookupTrial, readLookupSessionIdentity, collectLookupDatabaseIdentity } from './lookupBenchmarkSession.js';
import type { DatabaseQueryFn } from '../db/contracts.js';
import type { OperationSnapshot } from './operationResponse.js';
import { createOperationDiagnostics } from './operationDiagnostics.js';
import type { OperationDiagnostics } from './operationDiagnostics.js';

/** The selected revision owns all route, privacy, cache and admission policies. */
export async function withOperationServer<T>(source: string, work: (port: number, snapshot: OperationSnapshot, diagnostics?: OperationDiagnostics) => Promise<T>,
  options: { diagnostics?: boolean; onDatabaseIdentity?: (value: unknown) => void } = {}): Promise<T> {
  return withLookupSource(source, async (load, runtime) => {
    const [cacheModule, { registerStatsRoutes }, { registerExportRoutes }, limiters,
      { networkFilters }, { readPublicResponseGeneration }, { readCurrentVisibilityGeneration },
      { maskDecodedPathNodes }, { withHeavyWorkAdmission }, { wrapAsyncHandlers, apiErrorMiddleware }, chartSnapshots] = await Promise.all([
      load('api/bootstrap/caches.ts'), load('api/routes/stats.ts'), load('api/routes/exports.ts'), load('api/bootstrap/limiters.ts'),
      load('platform/query/networkFilters.ts'), load('privacy/responseGeneration.ts'), load('privacy/authority.ts'),
      load('stats/maskDecodedPathNodes.ts'), load('analysis/heavyWorkAdmission.ts'), load('api/errors.ts'), load('stats/chartSnapshot.ts'),
    ]);
    const databaseSetup = { http: await readLookupSessionIdentity(runtime.query, 'http'),
      worker: await readLookupSessionIdentity(runtime.analyticsQuery, 'worker') };
    const generation = await readPublicResponseGeneration(runtime.query);
    const stored = await chartSnapshots.loadStoredChartSnapshot(runtime.analyticsQuery, 'ukmesh', generation);
    const validated = stored && chartSnapshots.validateChartSnapshotPayload(stored.payload, 'ukmesh',
      cacheModule.CHARTS_CACHE_TTL_MS, Date.now(), generation);
    if (!validated || Number(stored.visibility_generation) !== generation
      || Math.abs(Date.parse(stored.generated_at) - validated.generatedAtMs) > 1000) throw new Error('fresh persisted chart snapshot required before read-only comparison');
    const snapshot: OperationSnapshot = { generatedAt: stored.payload.snapshot.generatedAt,
      visibilityGeneration: generation, maxAgeMs: cacheModule.CHARTS_CACHE_TTL_MS };
    const diagnostics = options.diagnostics ? await createOperationDiagnostics(load, runtime) : undefined;
    let attemptedWrite = false;
    const readOnly = (execute: DatabaseQueryFn): DatabaseQueryFn => async (sql, params, signal) => {
      try { return await execute(sql, params, signal); }
      catch (error) { if ((error as { code?: string }).code === '25006') attemptedWrite = true; throw error; }
    };
    const result = await withLookupTrial(cacheModule, async ({ caches, ownRoutes, listen }) => {
      const router = Router(), stopping = new AbortController();
      const service = registerStatsRoutes(router, {
        statsCache: caches.statsCache, statsCacheTtlMs: cacheModule.STATS_CACHE_TTL_MS,
        chartsCache: caches.chartsCache, chartsCacheTtlMs: cacheModule.CHARTS_CACHE_TTL_MS,
        chartsSnapshotStaleTtlMs: cacheModule.CHARTS_CACHE_STALE_TTL_MS, chartsInflight: caches.chartsInflight,
        expensiveLimiter: limiters.EXPENSIVE_LIMITER, statsChartsLimiter: limiters.STATS_CHARTS_LIMITER,
        query: readOnly(runtime.query), analyticsQuery: readOnly(runtime.analyticsQuery), networkFilters, maskDecodedPathNodes,
        getPublicVisibilityGeneration: () => readPublicResponseGeneration(runtime.query),
        runHeavyWork: (workload: string, task: () => Promise<unknown>) => withHeavyWorkAdmission({ pool: runtime.pool, workload, task, signal: stopping.signal }),
      });
      ownRoutes(async () => {
        stopping.abort();
        const pending = [...caches.chartsInflight.values()];
        service.close();
        await Promise.allSettled(pending);
      });
      registerExportRoutes(router, { query: readOnly(runtime.query), networkFilters,
        exportLimiter: limiters.EXPORT_LIMITER, getPublicVisibilityGeneration: readCurrentVisibilityGeneration });
      wrapAsyncHandlers(router);
      const app = express(); app.use('/api', limiters.createGlobalApiLimiter(), router); app.use(apiErrorMiddleware);
      return work(await listen(app), snapshot, diagnostics);
    });
    if (options.onDatabaseIdentity) options.onDatabaseIdentity(await collectLookupDatabaseIdentity(runtime, databaseSetup));
    if (attemptedWrite) throw new Error('read-only chart/export comparison attempted publication; prepare a fresh application snapshot');
    return result;
  });
}
