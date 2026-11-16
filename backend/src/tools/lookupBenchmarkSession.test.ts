import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { configureLookupReadOnlyEnvironment, lookupBenchmarkOptions, withLookupSource, withLookupTrial,
  lookupCredentialScope, readLookupSessionIdentity, collectLookupDatabaseIdentity } from './lookupBenchmarkSession.js';
import { createDatabaseRuntime } from '../db/databaseRuntime.js';
import type { DatabaseQueryFn } from '../db/contracts.js';
import type { LookupCacheModule } from './lookupBenchmarkSession.js';

test('benchmark options reject invalid work before loading a database runtime', () => {
  const options = lookupBenchmarkOptions(['--samples', '3', '--concurrency', '1', '--trials', '1', '--cache', 'warm', '--budget-ms', '100']);
  assert.deepEqual([options.samples, options.concurrency, options.trials, options.cache, options.budgetMs], [3, 1, 1, 'warm', 100]);
  for (const [flag, value] of [['budget-ms', 'NaN'], ['budget-ms', '0'], ['budget-ms', 'Infinity'],
    ['samples', '2'], ['samples', '1001'], ['concurrency', '2'], ['trials', '4'], ['cache', 'off']]) {
    assert.throws(() => lookupBenchmarkOptions([`--${flag}`, value!]), /invalid lookup benchmark options/);
  }
  const cli = spawnSync(process.execPath, ['--import', 'tsx', fileURLToPath(new URL('./benchmarkLookups.ts', import.meta.url)), '--budget-ms', 'NaN'], {
    cwd: fileURLToPath(new URL('../..', import.meta.url)),
    env: { PATH: process.env.PATH, DATABASE_URL: 'invalid-database-url' }, encoding: 'utf8', timeout: 10_000,
  });
  assert.equal(cli.status, 1);
  assert.match(cli.stderr, /invalid lookup benchmark options/);
  assert.doesNotMatch(cli.stderr, /Invalid URL/);
});

function roleEnvironment(enforced = true): NodeJS.ProcessEnv {
  const endpoint = (user: string, database = 'lookup_1x') => `postgresql://${user}:do-not-log@127.0.0.1:5434/${database}`;
  return { DATABASE_URL: endpoint('api'), MESHCORE_HTTP_DATABASE_URL: endpoint('api'),
    MESHCORE_WORKER_DATABASE_URL: endpoint('worker'), MESHCORE_INGEST_DATABASE_URL: endpoint('ingest'),
    OWNER_DATABASE_URL: endpoint('owner', 'owner_1x'), OWNER_DATABASE_POOL_MAX: '2',
    DATABASE_ROLE_ENFORCEMENT: enforced ? 'true' : 'false', DATABASE_SCHEMA: 'discard', UNRELATED: 'retained' };
}

test('standalone DATABASE_URL compatibility is explicitly legacy inspector scope', () => {
  const env: NodeJS.ProcessEnv = { DATABASE_SCHEMA: 'untrusted', UNRELATED: 'retained' };
  const scope = configureLookupReadOnlyEnvironment('postgresql://fixture:fixture@127.0.0.1:5434/lookup_10x', env);
  assert.equal(scope, 'legacy-inspector-readonly');
  for (const role of ['DATABASE_URL', 'MESHCORE_HTTP_DATABASE_URL', 'MESHCORE_INGEST_DATABASE_URL', 'MESHCORE_WORKER_DATABASE_URL', 'OWNER_DATABASE_URL']) {
    const url = new URL(env[role]!);
    assert.equal(url.pathname, '/lookup_10x');
    assert.equal(url.searchParams.get('options'), '-c default_transaction_read_only=on');
  }
  assert.equal(env.DATABASE_SCHEMA, undefined);
  assert.equal(env.UNRELATED, 'retained');
});

test('explicit role endpoints remain distinct in both enforced and standalone modes', () => {
  for (const enforced of [true, false]) {
    const env = roleEnvironment(enforced);
    configureLookupReadOnlyEnvironment(env.DATABASE_URL!, env);
    assert.equal(new URL(env.MESHCORE_HTTP_DATABASE_URL!).username, 'api');
    assert.equal(new URL(env.MESHCORE_WORKER_DATABASE_URL!).username, 'worker');
    assert.equal(new URL(env.MESHCORE_INGEST_DATABASE_URL!).username, 'ingest');
    assert.equal(new URL(env.OWNER_DATABASE_URL!).username, 'owner');
    assert.equal(new URL(env.OWNER_DATABASE_URL!).pathname, '/owner_1x');
    assert.equal(lookupCredentialScope(env), enforced ? 'application-role-readonly' : 'legacy-inspector-readonly');
  }
});

test('all endpoints are validated before any environment mutation or secret-bearing error', () => {
  for (const key of ['DATABASE_URL', 'MESHCORE_HTTP_DATABASE_URL', 'MESHCORE_WORKER_DATABASE_URL', 'MESHCORE_INGEST_DATABASE_URL', 'OWNER_DATABASE_URL']) {
    const env = roleEnvironment(); env[key] = 'postgresql://u:do-not-log@remote.invalid/db';
    const before = { ...env };
    assert.throws(() => configureLookupReadOnlyEnvironment(env.DATABASE_URL!, env), error => {
      assert.doesNotMatch(String(error), /do-not-log/); return true;
    });
    assert.deepEqual(env, before);
  }
});

test('enforced qualification refuses missing role, mismatched store and default-admin alias', () => {
  const absent = roleEnvironment(); delete absent.MESHCORE_WORKER_DATABASE_URL;
  const wrongOwner = roleEnvironment(); wrongOwner.OWNER_DATABASE_URL = wrongOwner.MESHCORE_HTTP_DATABASE_URL;
  const wrongWorker = roleEnvironment(); wrongWorker.MESHCORE_WORKER_DATABASE_URL += '_foreign';
  const alias = roleEnvironment(); alias.DATABASE_URL = 'postgresql://admin:do-not-log@127.0.0.1:5434/lookup_1x';
  for (const env of [absent, wrongOwner, wrongWorker, alias]) {
    const before = { ...env };
    assert.throws(() => configureLookupReadOnlyEnvironment(env.DATABASE_URL!, env));
    assert.deepEqual(env, before);
  }
});

test('existing URL options survive while read-only is enforced last', () => {
  const env = roleEnvironment();
  const original = '-c statement_timeout=25000 -c default_transaction_read_only=off';
  for (const key of ['DATABASE_URL', 'MESHCORE_HTTP_DATABASE_URL', 'MESHCORE_WORKER_DATABASE_URL', 'MESHCORE_INGEST_DATABASE_URL', 'OWNER_DATABASE_URL']) {
    const url = new URL(env[key]!); url.searchParams.set('sslmode', 'disable'); url.searchParams.set('options', original); env[key] = url.href;
  }
  configureLookupReadOnlyEnvironment(env.DATABASE_URL!, env);
  for (const key of ['DATABASE_URL', 'MESHCORE_HTTP_DATABASE_URL', 'MESHCORE_WORKER_DATABASE_URL', 'MESHCORE_INGEST_DATABASE_URL', 'OWNER_DATABASE_URL']) {
    const url = new URL(env[key]!);
    assert.equal(url.searchParams.get('sslmode'), 'disable');
    assert.equal(url.searchParams.get('options'), original + ' -c default_transaction_read_only=on');
  }
});

test('remote host overrides and explicit invalid standalone role are refused atomically', () => {
  for (const endpoint of ['postgresql://remote.invalid/db', 'postgresql://localhost/db?host=remote.invalid', 'postgresql://localhost/db?hostaddr=8.8.8.8']) {
    const env: NodeJS.ProcessEnv = { UNRELATED: 'retained' }; const before = { ...env };
    assert.throws(() => configureLookupReadOnlyEnvironment(endpoint, env), /loopback PostgreSQL database/);
    assert.deepEqual(env, before);
  }
  const env = roleEnvironment(false); env.MESHCORE_HTTP_DATABASE_URL = 'remote'; const before = { ...env };
  assert.throws(() => configureLookupReadOnlyEnvironment(env.DATABASE_URL!, env));
  assert.deepEqual(env, before);
});

test('actual runtime factory retains HTTP8 analytics2 ingest2 and explicit owner2 without opening sockets', async () => {
  const env = roleEnvironment(); configureLookupReadOnlyEnvironment(env.DATABASE_URL!, env);
  const runtime = createDatabaseRuntime(env);
  try {
    assert.equal(runtime.pool.options.max, 8);
    assert.equal(runtime.analyticsPool.options.max, 2);
    assert.equal(runtime.ingestPool.options.max, 2);
    assert.equal(env.OWNER_DATABASE_POOL_MAX, '2');
    assert.deepEqual([runtime.pool.totalCount, runtime.analyticsPool.totalCount, runtime.ingestPool.totalCount], [0, 0, 0]);
  } finally { await runtime.close(); }
});

test('session proof checks actual login and read-only values rather than configuration alone', async () => {
  const env = roleEnvironment(); configureLookupReadOnlyEnvironment(env.DATABASE_URL!, env);
  const identity = { current_user: 'api', session_user: 'api', database: 'lookup_1x', backend_pid: 123,
    transaction_read_only: 'on', default_transaction_read_only: 'on', statement_timeout: '30s', search_path: 'public', superuser: false, bypass_rls: false };
  const execute = (row: typeof identity) => (async () => ({ rows: [row] })) as unknown as DatabaseQueryFn;
  assert.deepEqual(await readLookupSessionIdentity(execute(identity), 'http', env), identity);
  for (const changed of [{ current_user: 'admin' }, { session_user: 'admin' }, { database: 'foreign' },
    { transaction_read_only: 'off' }, { superuser: true }, { bypass_rls: true }]) {
    await assert.rejects(readLookupSessionIdentity(execute({ ...identity, ...changed }), 'http', env), /session identity differs/);
  }
});

test('legacy URL without username retains driver-resolved login while qualification refuses it', async () => {
  const env: NodeJS.ProcessEnv = { DATABASE_URL: 'postgresql://127.0.0.1/lookup_1x' };
  configureLookupReadOnlyEnvironment(env.DATABASE_URL!, env);
  const row = { current_user: 'driver_login', session_user: 'driver_login', database: 'lookup_1x', backend_pid: 123,
    transaction_read_only: 'on', default_transaction_read_only: 'on', statement_timeout: '30s', search_path: 'public', superuser: true, bypass_rls: true };
  const execute = (value: typeof row) => (async () => ({ rows: [value] })) as unknown as DatabaseQueryFn;
  assert.deepEqual(await readLookupSessionIdentity(execute(row), 'http', env), row);
  await assert.rejects(readLookupSessionIdentity(execute({ ...row, session_user: 'other' }), 'http', env), /identity differs/);
  await assert.rejects(readLookupSessionIdentity(execute(row), 'http', { ...env, DATABASE_ROLE_ENFORCEMENT: 'true' }), /identity differs/);
});

test('post-timing proof queries only used logical pools and labels representative session scope', async () => {
  const env = roleEnvironment(); configureLookupReadOnlyEnvironment(env.DATABASE_URL!, env);
  const row = { current_user: 'api', session_user: 'api', database: 'lookup_1x', backend_pid: 123,
    transaction_read_only: 'on', default_transaction_read_only: 'on', statement_timeout: '30s', search_path: 'public', superuser: false, bypass_rls: false };
  const runtime = createDatabaseRuntime(env); let calls = 0;
  try {
    const fake = { ...runtime, closeDb: runtime.close,
      query: (async () => { calls++; return { rows: [row] }; }) as unknown as DatabaseQueryFn,
      analyticsQuery: (async () => { throw new Error('unused analytics pool queried'); }) as unknown as DatabaseQueryFn };
    const proof = await collectLookupDatabaseIdentity(fake, { http: row }, env);
    assert.equal(calls, 1); assert.deepEqual(Object.keys(proof.after), ['http']);
    assert.equal(proof.scope, 'application-role-readonly');
    assert.match(proof.sessionScope, /representative/);
    assert.doesNotMatch(JSON.stringify(proof), /do-not-log/);
  } finally { await runtime.close(); }
});

test('each selected source supplies its cache policy and closes after partial trial failure', async t => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'lookup-source-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  for (const [label, ttl] of [['baseline', 1000], ['candidate', 2000]] as const) {
    const source = path.join(directory, label), evidence = path.join(source, 'closed');
    await mkdir(path.join(source, 'db'), { recursive: true });
    await mkdir(path.join(source, 'api/bootstrap'), { recursive: true });
    await writeFile(path.join(source, 'db/runtime.ts'), `
      import {appendFileSync} from 'node:fs';
      export const pool = {};
      export const query = () => { throw new Error('test must not query a database'); };
      export async function closeDb() { appendFileSync(${JSON.stringify(evidence)}, 'runtime\\n'); }
    `);
    await writeFile(path.join(source, 'api/bootstrap/caches.ts'), `
      import {appendFileSync} from 'node:fs';
      export const NODE_LINKS_CACHE_TTL_MS = ${ttl};
      export const INFERRED_NODES_CACHE_TTL_MS = ${ttl * 5};
      export function createApiCaches() {
        return { nodeLinksCache: new Map(), inferredNodesCache: new Map(),
          nodeLinksInflight: new Map(), inferredNodesInflight: new Map(),
          close() { appendFileSync(${JSON.stringify(evidence)}, 'caches\\n'); } };
      }
    `);
    let port = 0;
    await assert.rejects(withLookupSource(source, async load => {
      const selected = await load('api/bootstrap/caches.ts') as LookupCacheModule;
      assert.equal(selected.NODE_LINKS_CACHE_TTL_MS, ttl);
      assert.equal(selected.INFERRED_NODES_CACHE_TTL_MS, ttl * 5);
      await withLookupTrial(selected, async ({ caches, ownRoutes, listen }) => {
        caches.nodeLinksCache.set('warm', { ts: 1, data: label });
        assert.equal(caches.nodeLinksCache.get('warm')?.data, label);
        ownRoutes(() => { caches.nodeLinksCache.clear(); });
        const app = express();
        app.get('/fixture', (_req, res) => res.json({ selected: label }));
        port = await listen(app);
        const response = await fetch(`http://127.0.0.1:${port}/fixture`);
        assert.deepEqual(await response.json(), { selected: label });
        throw new Error('trial failed');
      });
    }), /trial failed/);
    assert.equal(await readFile(evidence, 'utf8'), 'caches\nruntime\n');
    await assert.rejects(fetch(`http://127.0.0.1:${port}/fixture`));
    await assert.rejects(withLookupSource(source, load => load('missing-route-module.ts')), /Cannot find module/);
    assert.equal(await readFile(evidence, 'utf8'), 'caches\nruntime\nruntime\n');
  }
});

test('trial cleanup closes caches when route registration fails, even if its disposer fails too', async () => {
  const events: string[] = [];
  const cacheModule = {
    createApiCaches: () => ({ close() { events.push('caches'); } }),
  } as unknown as LookupCacheModule;
  await assert.rejects(withLookupTrial(cacheModule, async ({ ownRoutes }) => {
    ownRoutes(() => { events.push('routes'); throw new Error('route cleanup failed'); });
    throw new Error('route registration failed');
  }), /route cleanup failed/);
  assert.deepEqual(events, ['routes', 'caches']);
});

test('trial cleanup awaits asynchronous route work before closing its caches', async () => {
  const events: string[] = [];
  const cacheModule = { createApiCaches: () => ({ close() { events.push('caches'); } }) } as unknown as LookupCacheModule;
  await withLookupTrial(cacheModule, async ({ ownRoutes }) => {
    ownRoutes(async () => { await new Promise(resolve => setTimeout(resolve, 10)); events.push('routes'); });
    events.push('work');
  });
  assert.deepEqual(events, ['work', 'routes', 'caches']);
});
