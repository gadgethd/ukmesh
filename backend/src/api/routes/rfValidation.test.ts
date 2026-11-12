import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router, type RequestHandler } from 'express';
import { networkFilters } from '../../platform/query/networkFilters.js';
import { registerRfValidationRoutes, RF_VALIDATION_REQUEST_TIMEOUT_MS } from './rfValidation.js';
import { PUBLIC_REVALIDATE } from '../../http/publicJson.js';

const passThroughLimiter: RequestHandler = (_req, _res, next) => next();
type RfQuery = NonNullable<Parameters<typeof registerRfValidationRoutes>[2]>;
async function start(query: RfQuery, authority: RfQuery = async () => ({ rows: [{ generation: '1' }] as never[] })) {
  const router = Router();
  registerRfValidationRoutes(router, {
    limiter: passThroughLimiter as Parameters<typeof registerRfValidationRoutes>[1]['limiter'],
    networkFilters,
    query: async () => { throw new Error('Must not use the unbounded shared query'); },
  }, (sql, params, signal) => sql.includes('ops.public_visibility_state')
    ? authority(sql, params, signal) : query(sql, params, signal));
  const app = express();
  app.use(router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return {
    url: `http://127.0.0.1:${(server.address() as AddressInfo).port}/rf-validation?network=ukmesh&limit=100`,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

for (const stage of [1, 2]) {
  test(`RF validation propagates an HTTP disconnect during database read ${stage}`, { timeout: 3_000 }, async () => {
    let calls = 0;
    let resolveStarted!: () => void;
    let resolveAborted!: () => void;
    const started = new Promise<void>((resolve) => { resolveStarted = resolve; });
    const aborted = new Promise<void>((resolve) => { resolveAborted = resolve; });
    const server = await start(async (_sql, _params, signal) => {
      assert.ok(signal);
      if (++calls < stage) return { rows: [{ node_id: 'A', name: 'Alpha' }] as never[] };
      resolveStarted();
      return new Promise((_resolve, reject) => {
        signal.addEventListener('abort', () => {
          resolveAborted();
          reject(signal.reason);
        }, { once: true });
      });
    });
    try {
      const controller = new AbortController();
      const response = fetch(server.url, { signal: controller.signal });
      await started;
      controller.abort();
      await assert.rejects(response, /aborted/i);
      await aborted;
      assert.equal(calls, stage);
    } finally { await server.close(); }
  });
}

test('RF deadline aborts the database operation and returns an uncached 503 within five seconds', { timeout: 6_000 }, async () => {
  let aborted = false;
  const server = await start(async (_sql, _params, signal) => new Promise((_resolve, reject) => {
    signal!.addEventListener('abort', () => { aborted = true; reject(signal!.reason); }, { once: true });
  }));
  try {
    const started = performance.now();
    const response = await fetch(server.url);
    assert.equal(response.status, 503);
    assert.ok(aborted);
    assert.ok(performance.now() - started < 5_000);
    assert.equal(RF_VALIDATION_REQUEST_TIMEOUT_MS, 4_500);
    assert.equal(response.headers.get('cache-control'), 'no-store');
    assert.equal(response.headers.get('retry-after'), '30');
    assert.match((await response.json() as { error: string }).error, /temporarily unavailable/);
  } finally { await server.close(); }
});

test('RF server statement cancellation is reported as unavailable, not an empty successful panel', async () => {
  const server = await start(async () => { throw Object.assign(new Error('statement cancelled'), { code: '57014' }); });
  try {
    const response = await fetch(server.url);
    assert.equal(response.status, 503);
    assert.equal(response.headers.get('cache-control'), 'no-store');
  } finally { await server.close(); }
});

test('RF JSON keeps classification summaries and numeric counts, including an honest empty scope', async () => {
  for (const empty of [false, true]) {
    const server = await start(async (sql) => {
      if (sql.includes('FROM core.node_identity_nodes')) {
        return { rows: (empty ? [] : [{ node_id: 'A', name: 'Alpha' }, { node_id: 'B', name: null }]) as never[] };
      }
      return { rows: [{ node_a_id: 'A', node_b_id: 'B', observed_count: '12', multibyte_observed_count: '3',
        itm_path_loss_db: 130, itm_viable: false, force_viable: false,
        last_observed: '2026-09-07T12:00:00Z', classification: 'observed_unexpected' }] as never[] };
    });
    try {
      const response = await fetch(server.url);
      assert.equal(response.status, 200);
      assert.equal(response.headers.get('cache-control'), PUBLIC_REVALIDATE);
      const body = await response.json() as { windowDays: number; summary: { evaluated: number; observedUnexpected: number }; mismatches: Array<{ sourceName: string; observations: number; strongObservations: number }> };
      assert.equal(body.windowDays, 30);
      assert.equal(body.summary.evaluated, empty ? 0 : 1);
      assert.equal(body.summary.observedUnexpected, empty ? 0 : 1);
      if (!empty) {
        assert.equal(body.mismatches[0]!.sourceName, 'Alpha');
        assert.equal(body.mismatches[0]!.observations, 12);
        assert.equal(body.mismatches[0]!.strongObservations, 3);
      }
    } finally { await server.close(); }
  }
});

test('RF authority reads share the request deadline and cancellation signal', { timeout: 6_000 }, async () => {
  let aborted = false;
  const server = await start(async () => { assert.fail('Rows must not load before authority'); },
    async (_sql, _params, signal) => new Promise((_resolve, reject) => {
      signal!.addEventListener('abort', () => { aborted = true; reject(signal!.reason); }, { once: true });
    }));
  try {
    const response = await fetch(server.url);
    assert.equal(response.status, 503); assert.ok(aborted);
    assert.equal(response.headers.get('cache-control'), 'no-store');
  } finally { await server.close(); }
});

test('RF conditional responses check authority twice and withhold a failed or changed generation', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-11-12T12:00:00Z') });
  t.mock.method(console, 'error', () => undefined);
  let reads = 0, mode = 'ready', loads = 0;
  const server = await start(async sql => {
    loads++;
    return { rows: (sql.includes('FROM core.node_identity_nodes') ? [{ node_id: 'A', name: 'Invented RF node' }] : []) as never[] };
  }, async (_sql, _params, signal) => {
    assert.ok(signal); reads++;
    if (mode === 'initial-failure' || mode === 'final-failure' && reads === 2) throw new Error('unavailable');
    return { rows: [{ generation: mode === 'final-change' && reads === 2 ? '2' : '1' }] as never[] };
  });
  try {
    const first = await fetch(server.url); assert.equal(first.status, 200); await first.arrayBuffer();
    const conditional = { cache: 'force-cache' as const, headers: { 'If-None-Match': first.headers.get('etag')! } };
    reads = 0; assert.equal((await fetch(server.url, conditional)).status, 304); assert.equal(reads, 2);
    for (const failure of ['initial-failure','final-failure','final-change']) {
      mode = failure; reads = 0; const before = loads;
      const response = await fetch(server.url, conditional); assert.equal(response.status, 500);
      assert.equal(response.headers.get('cache-control'), 'no-store');
      assert.ok(!JSON.stringify(await response.json()).includes('Invented RF node'));
      if (failure === 'initial-failure') assert.equal(loads, before);
    }
  } finally { await server.close(); }
});
