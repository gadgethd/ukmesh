import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express from 'express';
import type { DatabaseQueryFn } from '../../db/contracts.js';
import { registerFeedRegionRoute } from './feedRegions.js';

test('feed region options are scoped, normalized and cached only within their visibility generation', async t => {
  let generation = 2;
  let advanceDuringRead = false;
  const scopes: unknown[][] = [];
  const query: DatabaseQueryFn = async <T>(sql: string, params?: unknown[], signal?: AbortSignal) => {
    assert.match(sql, /FROM mesh\.packets p/);
    assert.match(sql, /p\.visibility_ok IS TRUE/);
    assert.ok(signal && !signal.aborted);
    scopes.push(params!);
    if (advanceDuringRead) generation += 1;
    return { rows: [{ iata: ' NCL ' }, { iata: 'LHR' }, { iata: 'lhr' }, { iata: '?' }] as T[] };
  };
  const app = express();
  t.after(registerFeedRegionRoute(app, query, async () => generation));
  const server = app.listen(0, '127.0.0.1');
  t.after(() => { server.closeAllConnections(); server.close(); });
  await once(server, 'listening');
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}/feed/regions`;
  const observer = 'b'.repeat(64);
  const scoped = `${base}?network=test&observer=${observer}`;

  const first = await fetch(scoped);
  assert.equal(first.status, 200);
  assert.deepEqual(await first.json(), { network: 'test', observer: observer.toUpperCase(),
    visibilityGeneration: 2, regions: ['LHR', 'NCL'] });
  assert.deepEqual(scopes, [['test', observer.toUpperCase()]]);

  const cached = await fetch(scoped);
  assert.equal(cached.status, 200);
  assert.equal(scopes.length, 1);

  advanceDuringRead = true;
  const fenced = await fetch(base);
  assert.equal(fenced.status, 503);
  assert.deepEqual(await fenced.json(), { error: 'Observer regions are unavailable', retryable: true });

  advanceDuringRead = false;
  const fresh = await fetch(scoped);
  assert.equal(fresh.status, 200);
  assert.deepEqual(await fresh.json(), { network: 'test', observer: observer.toUpperCase(),
    visibilityGeneration: 3, regions: ['LHR', 'NCL'] });
  assert.equal(scopes.length, 3);
});
