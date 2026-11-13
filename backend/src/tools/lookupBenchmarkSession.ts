import { once } from 'node:events';
import type { Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import type { Express } from 'express';
import type pg from 'pg';
import type { DatabaseQueryFn } from '../db/contracts.js';
import { assertLocalDatabaseUrl, option } from './sampleData.js';

export type LookupBenchmarkOptions = {
  source: string;
  output: string;
  samples: number;
  concurrency: 1 | 8;
  trials: number;
  cache: 'miss' | 'warm';
  budgetMs: number | undefined;
};

export function lookupBenchmarkOptions(args: string[]): LookupBenchmarkOptions {
  const samples = Number(option(args, 'samples') ?? 200);
  const concurrency = Number(option(args, 'concurrency') ?? 8);
  const trials = Number(option(args, 'trials') ?? 3);
  const cache = option(args, 'cache') ?? 'miss';
  const rawBudget = option(args, 'budget-ms');
  const budgetMs = rawBudget === undefined ? undefined : Number(rawBudget);
  if (!Number.isSafeInteger(samples) || samples < 3 || samples > 1000
    || (concurrency !== 1 && concurrency !== 8)
    || !Number.isSafeInteger(trials) || trials < 1 || trials > 3
    || (cache !== 'miss' && cache !== 'warm')
    || (budgetMs !== undefined && (!Number.isFinite(budgetMs) || budgetMs <= 0))) {
    throw new Error('invalid lookup benchmark options');
  }
  return {
    source: path.resolve(option(args, 'source-root') ?? 'src'),
    output: path.resolve(option(args, 'output') ?? `../plans/database-lookups/${Date.now()}`),
    samples, concurrency, trials, cache, budgetMs,
  };
}

export function configureLookupReadOnlyEnvironment(endpoint: string, env: NodeJS.ProcessEnv): void {
  assertLocalDatabaseUrl(endpoint);
  const readOnlyUrl = new URL(endpoint);
  readOnlyUrl.searchParams.set('options', '-c default_transaction_read_only=on');
  for (const key of ['DATABASE_URL', 'MESHCORE_HTTP_DATABASE_URL', 'MESHCORE_WORKER_DATABASE_URL', 'MESHCORE_INGEST_DATABASE_URL', 'OWNER_DATABASE_URL']) {
    env[key] = readOnlyUrl.href;
  }
  delete env['DATABASE_SCHEMA'];
}

type LookupRuntime = { pool: pg.Pool; query: DatabaseQueryFn; closeDb: () => Promise<void> };
export type LookupSourceLoader = (file: string) => Promise<any>;

/** Archived revisions also import their process-default runtime. Configure the
 * read-only environment before loading either revision, and close it on failure. */
export async function withLookupSource<T>(source: string,
  work: (load: LookupSourceLoader, runtime: LookupRuntime) => Promise<T>): Promise<T> {
  const load: LookupSourceLoader = file => import(pathToFileURL(path.join(source, file)).href);
  const runtime = await load('db/runtime.ts') as LookupRuntime;
  try { return await work(load, runtime); }
  finally { await runtime.closeDb(); }
}

export type LookupCacheModule = Pick<typeof import('../api/bootstrap/caches.js'),
  'createApiCaches' | 'NODE_LINKS_CACHE_TTL_MS' | 'INFERRED_NODES_CACHE_TTL_MS'>;
type LookupCaches = ReturnType<LookupCacheModule['createApiCaches']>;

/** One trial owns its selected revision's caches, routes and loopback server,
 * including resources allocated before setup or listen fails. */
export async function withLookupTrial<T>(cacheModule: LookupCacheModule, work: (trial: {
  caches: LookupCaches;
  ownRoutes: (close: () => void) => void;
  listen: (app: Express) => Promise<number>;
}) => Promise<T>): Promise<T> {
  const caches = cacheModule.createApiCaches();
  let closeRoutes: (() => void) | undefined;
  let server: Server | undefined;
  try {
    return await work({
      caches,
      ownRoutes(close) { closeRoutes = close; },
      async listen(app) {
        server = app.listen(0, '127.0.0.1');
        await once(server, 'listening');
        return (server.address() as AddressInfo).port;
      },
    });
  } finally {
    try { closeRoutes?.(); }
    finally {
      try {
        if (server) {
          server.closeAllConnections();
          await new Promise<void>(resolve => server!.close(() => resolve()));
        }
      } finally { caches.close(); }
    }
  }
}
