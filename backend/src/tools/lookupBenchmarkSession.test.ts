import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { configureLookupReadOnlyEnvironment, lookupBenchmarkOptions, withLookupSource, withLookupTrial } from './lookupBenchmarkSession.js';
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

test('benchmark role URLs enforce read-only sessions and reject remote host overrides', () => {
  const env: NodeJS.ProcessEnv = { DATABASE_SCHEMA: 'untrusted', MESHCORE_HTTP_DATABASE_URL: 'remote', UNRELATED: 'retained' };
  configureLookupReadOnlyEnvironment('postgresql://fixture:fixture@127.0.0.1:5434/lookup_10x', env);
  for (const role of ['DATABASE_URL', 'MESHCORE_HTTP_DATABASE_URL', 'MESHCORE_INGEST_DATABASE_URL', 'MESHCORE_WORKER_DATABASE_URL', 'OWNER_DATABASE_URL']) {
    const url = new URL(env[role]!);
    assert.equal(url.hostname, '127.0.0.1');
    assert.equal(url.pathname, '/lookup_10x');
    assert.equal(url.searchParams.get('options'), '-c default_transaction_read_only=on');
  }
  assert.equal(env.DATABASE_SCHEMA, undefined);
  assert.equal(env.UNRELATED, 'retained');
  for (const endpoint of ['postgresql://remote.invalid/lookup_10x', 'postgresql://localhost/lookup_10x?host=remote.invalid']) {
    assert.throws(() => configureLookupReadOnlyEnvironment(endpoint, env), /loopback PostgreSQL database/);
  }
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
