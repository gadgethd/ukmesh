import { once } from 'node:events';
import type { Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import type { Express } from 'express';
import type pg from 'pg';
import type { DatabaseQueryFn } from '../db/contracts.js';
import { assertLocalDatabaseUrl, option } from './sampleData.js';
import { databaseUrlForRole, loadDatabaseConfig, type DatabaseRole } from '../platform/config/database.js';

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

export type LookupCredentialScope = 'application-role-readonly' | 'legacy-inspector-readonly';
const ROLE_KEYS = [
  ['http', 'MESHCORE_HTTP_DATABASE_URL'], ['worker', 'MESHCORE_WORKER_DATABASE_URL'],
  ['ingest', 'MESHCORE_INGEST_DATABASE_URL'], ['owner', 'OWNER_DATABASE_URL'],
] as const;

export function lookupCredentialScope(env: NodeJS.ProcessEnv): LookupCredentialScope {
  return ['1', 'true', 'yes', 'on'].includes(String(env['DATABASE_ROLE_ENFORCEMENT'] ?? '').trim().toLowerCase())
    ? 'application-role-readonly' : 'legacy-inspector-readonly';
}

function localReadOnlyUrl(value: string, role: string): URL {
  try {
    assertLocalDatabaseUrl(value);
    const url = new URL(value);
    if (url.hash) throw new Error();
    const options = url.searchParams.get('options');
    url.searchParams.set('options', (options ? options + ' ' : '') + '-c default_transaction_read_only=on');
    return url;
  } catch {
    // URL parsing errors may carry their input. Never attach that exception.
    throw new Error('benchmark requires an explicit loopback PostgreSQL database for ' + role);
  }
}

/** Qualification supplies every actual role. Standalone DATABASE_URL-only use
 * remains an explicitly labelled read-only inspector, never role qualification.
 * Validate the complete proposed environment before changing any caller state. */
export function configureLookupReadOnlyEnvironment(endpoint: string, env: NodeJS.ProcessEnv): LookupCredentialScope {
  const proposed: NodeJS.ProcessEnv = { ...env, DATABASE_URL: endpoint };
  const scope = lookupCredentialScope(proposed);
  const base = localReadOnlyUrl(endpoint, 'default');
  const updates: NodeJS.ProcessEnv = { DATABASE_URL: base.href };
  const urls = new Map<DatabaseRole, URL>();
  for (const [role, key] of ROLE_KEYS) {
    const value = databaseUrlForRole(role, proposed);
    if (!value) throw new Error('explicit benchmark role URL missing for ' + role);
    const url = localReadOnlyUrl(value, role);
    urls.set(role, url); updates[key] = url.href;
  }
  if (scope === 'application-role-readonly') {
    const http = urls.get('http')!;
    if (http.href !== base.href || loadDatabaseConfig(proposed).poolMax !== 8
      || Number(proposed['OWNER_DATABASE_POOL_MAX']) !== 2) throw new Error('qualification role profile differs');
    for (const [role, url] of urls) {
      if (url.host !== http.host || (role === 'owner' ? url.pathname === http.pathname : url.pathname !== http.pathname)) {
        throw new Error('qualification role store or endpoint differs for ' + role);
      }
    }
  }
  Object.assign(env, updates);
  delete env['DATABASE_SCHEMA'];
  return scope;
}

export type LookupSessionIdentity = {
  current_user: string; session_user: string; database: string; backend_pid: number;
  transaction_read_only: string; default_transaction_read_only: string;
  statement_timeout: string; search_path: string; superuser: boolean; bypass_rls: boolean;
};

export const LOOKUP_SESSION_IDENTITY_SQL = `SELECT current_user::text current_user,
  session_user::text session_user, current_database() database, pg_backend_pid() backend_pid,
  current_setting('transaction_read_only') transaction_read_only,
  current_setting('default_transaction_read_only') default_transaction_read_only,
  current_setting('statement_timeout') statement_timeout, current_setting('search_path') search_path,
  r.rolsuper superuser, r.rolbypassrls bypass_rls FROM pg_catalog.pg_roles r WHERE r.rolname=current_user`;

export async function readLookupSessionIdentity(execute: DatabaseQueryFn, role: 'http' | 'worker',
  env: NodeJS.ProcessEnv = process.env): Promise<LookupSessionIdentity> {
  const identity = (await execute(LOOKUP_SESSION_IDENTITY_SQL)).rows[0] as LookupSessionIdentity | undefined;
  const configured = databaseUrlForRole(role, env);
  if (!configured) throw new Error('benchmark identity role URL missing');
  const expected = new URL(configured);
  const configuredUser = decodeURIComponent(expected.username);
  const enforced = lookupCredentialScope(env) === 'application-role-readonly';
  if (!identity || !identity.current_user || identity.session_user !== identity.current_user
    || (configuredUser ? identity.current_user !== configuredUser : enforced)
    || identity.database !== decodeURIComponent(expected.pathname.slice(1))
    || !Number.isSafeInteger(identity.backend_pid) || identity.backend_pid <= 0
    || identity.transaction_read_only !== 'on' || identity.default_transaction_read_only !== 'on'
    || (enforced && (identity.superuser !== false || identity.bypass_rls !== false))) {
    throw new Error('authenticated read-only benchmark session identity differs for ' + role);
  }
  return identity;
}

export async function collectLookupDatabaseIdentity(runtime: Pick<LookupRuntime, 'pool' | 'analyticsPool' | 'ingestPool' | 'query' | 'analyticsQuery'>,
  setup: Partial<Record<'http' | 'worker', LookupSessionIdentity>>, env: NodeJS.ProcessEnv = process.env) {
  if (!setup.http) throw new Error('HTTP setup identity is missing');
  const after: Partial<Record<'http' | 'worker', LookupSessionIdentity>> = {};
  after.http = await readLookupSessionIdentity(runtime.query, 'http', env);
  if (setup.worker) after.worker = await readLookupSessionIdentity(runtime.analyticsQuery, 'worker', env);
  const profiles = Object.fromEntries(([['http', runtime.pool], ['worker', runtime.analyticsPool],
    ['ingest', runtime.ingestPool]] as const).map(([name, pool]) => [name,
    Object.fromEntries(['max', 'idleTimeoutMillis', 'connectionTimeoutMillis', 'statement_timeout', 'query_timeout']
      .map(key => [key, (pool.options as unknown as Record<string, unknown>)[key]]))]));
  const expected = Object.fromEntries(ROLE_KEYS.map(([role]) => {
    const url = new URL(databaseUrlForRole(role, env)!);
    return [role, { user: decodeURIComponent(url.username), database: decodeURIComponent(url.pathname.slice(1)) }];
  }));
  if (lookupCredentialScope(env) === 'application-role-readonly'
    && (profiles.http?.max !== 8 || profiles.worker?.max !== 2 || profiles.ingest?.max !== 2
      || Number(env['OWNER_DATABASE_POOL_MAX']) !== 2)) throw new Error('actual benchmark pool bounds differ');
  return { version: 1, scope: lookupCredentialScope(env), setup, after, profiles, expected,
    ownerPool: { configuredMax: Number(env['OWNER_DATABASE_POOL_MAX'] ?? 3), queriedByBenchmark: false },
    sessionScope: 'Existing setup and post-timing representative used-pool sessions; no additional warmup or claim of every historical physical client' };
}

export type LookupRuntime = { pool: pg.Pool; analyticsPool: pg.Pool; ingestPool: pg.Pool; query: DatabaseQueryFn; analyticsQuery: DatabaseQueryFn; closeDb: () => Promise<void> };
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
  ownRoutes: (close: () => void | Promise<void>) => void;
  listen: (app: Express) => Promise<number>;
}) => Promise<T>): Promise<T> {
  const caches = cacheModule.createApiCaches();
  let closeRoutes: (() => void | Promise<void>) | undefined;
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
    try { await closeRoutes?.(); }
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
