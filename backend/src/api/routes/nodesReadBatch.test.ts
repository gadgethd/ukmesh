import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router, type RequestHandler } from 'express';
import { encodePublicMapCursor } from '../../nodes/publicMap.js';
import { networkFilters } from '../../platform/query/networkFilters.js';
import { createNodeRepository } from '../../repositories/nodes.js';
import { registerNodeRoutes } from './nodes.js';

const ID = 'A'.repeat(64), OTHER = 'B'.repeat(64);
type Deps = Parameters<typeof registerNodeRoutes>[1];

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}

async function fixture(t: test.TestContext) {
  const previous = process.env['PUBLIC_FEATURE_REGION_SCOPING_ENABLED'];
  process.env['PUBLIC_FEATURE_REGION_SCOPING_ENABLED'] = 'true';
  t.after(() => {
    if (previous === undefined) delete process.env['PUBLIC_FEATURE_REGION_SCOPING_ENABLED'];
    else process.env['PUBLIC_FEATURE_REGION_SCOPING_ENABLED'] = previous;
  });
  t.mock.method(console, 'error', () => undefined);
  const mapRow = { node_id: ID, name: 'Public node', lat: 54, lon: -1, role: 2 };
  const historyRow = { time: new Date('2026-01-01T00:00:00.000Z'), data: { nested: { changes: 0 } } };
  const repository = createNodeRepository(async () => ({ rows: [] }));
  let mapLoad: typeof repository.listPublicMapRows = async () => [mapRow];
  let historyLoad: Deps['getNodeHistory'] = async () => [historyRow];
  const calls = { map: [] as Parameters<typeof mapLoad>[], history: [] as Parameters<typeof historyLoad>[], reads: 0 };
  repository.listPublicMapRows = (...args) => { calls.map.push(args); return mapLoad(...args); };
  let generation = 1;
  let read = async () => generation;
  const app = express(), router = Router();
  const limiter: RequestHandler = (_req, _res, next) => next();
  const close = registerNodeRoutes(router, {
    nodeRepository: repository, getNodes: async () => [],
    getNodeHistory: (...args) => { calls.history.push(args); return historyLoad(...args); },
    getNodeAdverts: async () => [], getTopAdvertingRepeaters: async () => [], requireLocalOnly: () => false,
    networkFilters, getPublicVisibilityGeneration: () => { calls.reads++; return read(); },
    inferredNodesCache: new Map(), inferredNodesInflight: new Map(), inferredNodesCacheTtlMs: 60_000,
    nodeLinksCache: new Map(), nodeLinksInflight: new Map(), nodeLinksCacheTtlMs: 60_000,
    nodesLimiter: limiter as Deps['nodesLimiter'],
  });
  app.use(router);
  const server = app.listen(0, '127.0.0.1');
  t.after(async () => {
    close(); server.closeAllConnections();
    await new Promise<void>(resolve => server.close(() => resolve()));
  });
  await once(server, 'listening');
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  const snapshot = new Date().toISOString();
  return {
    app, close, calls, mapRow, historyRow,
    map: `/nodes/map?network=ukmesh&snapshot=${encodeURIComponent(snapshot)}`,
    history: `/nodes/${ID}/history?network=ukmesh`,
    get: (url: string) => fetch(base + url),
    setGeneration: (value: number) => { generation = value; },
    setMapLoad: (load: typeof mapLoad) => { mapLoad = load; },
    setHistoryLoad: (load: typeof historyLoad) => { historyLoad = load; },
    cohort(count: number, generations?: number[]) {
      const arrivals: Array<() => void> = [];
      read = () => new Promise<number>(resolve => {
        const captured = generations?.[arrivals.length] ?? generation;
        arrivals.push(() => resolve(captured));
        if (arrivals.length === count) {
          read = async () => generation;
          for (const arrival of arrivals) arrival();
        }
      });
    },
  };
}

test('eight simultaneous map and history responses share only their respective body read', { timeout: 10_000 }, async t => {
  const f = await fixture(t);
  const history = [f.historyRow];
  f.setHistoryLoad(async () => history);
  let mapEncodes = 0, historyEncodes = 0, bodyClones = 0;
  const stringify = JSON.stringify, clone = structuredClone;
  t.mock.method(JSON, 'stringify', (value: unknown, ...args: unknown[]) => {
    if (value === f.mapRow) mapEncodes++;
    if (value === history) historyEncodes++;
    return Reflect.apply(stringify, JSON, [value, ...args]);
  });
  t.mock.method(globalThis, 'structuredClone', (value: any, ...args: unknown[]) => {
    if (value?.nodes || Array.isArray(value) && value[0] === f.historyRow) bodyClones++;
    return Reflect.apply(clone, globalThis, [value, ...args]);
  });
  f.cohort(16);
  const responses = await Promise.all(Array.from({ length: 16 }, (_, i) => f.get(i < 8 ? f.map : f.history)));
  assert.equal(f.calls.map.length, 1);
  assert.equal(f.calls.history.length, 1);
  assert.equal(f.calls.reads, 32, 'each caller still makes two independent authority reads');
  for (const [i, response] of responses.entries()) {
    assert.equal(response.status, 200);
    const body = await response.json();
    if (i < 8) assert.deepEqual(body.nodes, [f.mapRow]);
    else assert.equal(body[0].time, f.historyRow.time.toISOString());
  }
  assert.equal(mapEncodes, 1, 'the exact row-budget encoding belongs to the cohort, not each response');
  assert.equal(historyEncodes, 1, 'the history body is encoded once for the cohort');
  assert.equal(bodyClones, 0, 'default immutable-wire delivery never clones the body');
});

test('map cohorts separate every normalized scope, ordered field list and exact page argument', { timeout: 10_000 }, async t => {
  const f = await fixture(t);
  const base = f.map + '&fields=name,lat';
  const urls = [base, base.replace('network=ukmesh', 'network=test'), base + '&observer=' + OTHER,
    base + '&region=North', base.replace('fields=name,lat', 'fields=lat,name'),
    base.replace(/snapshot=[^&]+/, 'snapshot=' + encodeURIComponent(new Date(Date.now() - 1000).toISOString())),
    base + '&cursor=' + encodePublicMapCursor(OTHER), base + '&limit=2', base + '&q=Public'];
  f.cohort(urls.length);
  const responses = await Promise.all(urls.map(url => f.get(url)));
  assert.ok(responses.every(response => response.status === 200));
  assert.equal(f.calls.map.length, urls.length, 'no distinct map argument can share a statement');
  assert.equal(f.calls.reads, 2 * urls.length);
  for (const response of responses) await response.arrayBuffer();
  const count = f.calls.map.length;
  f.cohort(2);
  const equivalent = await Promise.all([
    f.get(base + '&observer=' + OTHER.toLowerCase() + '&q=%20Public%20'),
    f.get(base.replace('fields=name,lat', 'fields=name,%20lat,name') + '&observer=' + OTHER + '&q=Public'),
  ]);
  assert.ok(equivalent.every(response => response.status === 200));
  assert.equal(f.calls.map.length, count + 1, 'the key uses parsed arguments rather than raw query spelling');
  for (const response of equivalent) await response.arrayBuffer();
});

test('history cohorts separate node, hours and network', { timeout: 10_000 }, async t => {
  const f = await fixture(t);
  const urls = [f.history, f.history.replace(ID, OTHER), f.history + '&hours=48', f.history.replace('ukmesh', 'test')];
  f.cohort(urls.length);
  const responses = await Promise.all(urls.map(url => f.get(url)));
  assert.ok(responses.every(response => response.status === 200));
  assert.equal(f.calls.history.length, urls.length);
  assert.equal(f.calls.reads, 2 * urls.length);
  for (const response of responses) await response.arrayBuffer();
});

test('different initial generations never share a body and each caller retains its final fence', { timeout: 10_000 }, async t => {
  for (const feature of ['map', 'history'] as const) {
    const f = await fixture(t);
    f.setGeneration(2); f.cohort(2, [1, 2]);
    const responses = await Promise.all([f.get(f[feature]), f.get(f[feature])]);
    assert.deepEqual(responses.map(response => response.status).sort(), [200, 500]);
    assert.equal(f.calls[feature].length, 2);
    assert.equal(f.calls.reads, 4);
    for (const response of responses) {
      if (response.status === 500) {
        assert.equal(response.headers.get('cache-control'), 'no-store');
        assert.deepEqual(await response.json(), { error: 'Internal server error' });
      } else await response.arrayBuffer();
    }
  }
});

test('a caller arriving during dispatched work and after completion always starts a fresh read', { timeout: 10_000 }, async t => {
  for (const feature of ['map', 'history'] as const) {
    const f = await fixture(t), entered = deferred<void>(), release = deferred<void>();
    t.after(() => release.resolve());
    const load = async () => {
      const first = f.calls[feature].length === 1;
      if (first) { entered.resolve(); await release.promise; }
      return [{ node_id: ID, name: first ? 'first statement' : 'later statement' }];
    };
    if (feature === 'map') f.setMapLoad(load); else f.setHistoryLoad(load);
    const first = f.get(f[feature]);
    await entered.promise;
    const second = await f.get(f[feature]);
    const secondBody = await second.json();
    assert.equal((feature === 'map' ? secondBody.nodes : secondBody)[0].name, 'later statement');
    assert.equal(f.calls[feature].length, 2, 'active work is never a join target');
    release.resolve();
    const firstBody = await (await first).json();
    assert.equal((feature === 'map' ? firstBody.nodes : firstBody)[0].name, 'first statement');
    await (await f.get(f[feature])).arrayBuffer();
    assert.equal(f.calls[feature].length, 3, 'there is no completed-result cache');
  }
});

test('each caller owns copied flat map rows and nested history JSON/Date through custom serialization', { timeout: 10_000 }, async t => {
  const f = await fixture(t);
  const clones = { map: 0, history: 0 }, clone = structuredClone;
  t.mock.method(globalThis, 'structuredClone', (value: any, ...args: unknown[]) => {
    if (value?.nodes) clones.map++;
    if (Array.isArray(value) && value[0] === f.historyRow) clones.history++;
    return Reflect.apply(clone, globalThis, [value, ...args]);
  });
  f.app.set('json replacer', (key: string, value: any) => {
    if (key === '' && value?.nodes) value.nodes[0].name += '!';
    if (key === '' && Array.isArray(value)) {
      value[0].data.nested.changes++;
      assert.ok(value[0].time instanceof Date, 'Date survives the body copy');
      value[0].time.setUTCSeconds(value[0].time.getUTCSeconds() + 1);
    }
    return value;
  });
  f.cohort(16);
  const responses = await Promise.all(Array.from({ length: 16 }, (_, i) => f.get(i < 8 ? f.map : f.history)));
  for (const [i, response] of responses.entries()) {
    const body = await response.json();
    if (i < 8) assert.equal(body.nodes[0].name, 'Public node!');
    else { assert.equal(body[0].data.nested.changes, 1); assert.equal(body[0].time, '2026-01-01T00:00:01.000Z'); }
  }
  assert.equal(f.mapRow.name, 'Public node');
  assert.equal(f.historyRow.data.nested.changes, 0);
  assert.equal(f.historyRow.time.toISOString(), '2026-01-01T00:00:00.000Z');
  assert.deepEqual(clones, { map: 8, history: 8 }, 'only custom delivery clones separately for every caller');
});

test('failed cohorts settle every response and route close settles active work but rejects later reads', { timeout: 10_000 }, async t => {
  const f = await fixture(t);
  f.setMapLoad(async () => { throw new Error('body read failed'); });
  f.cohort(8);
  const failed = await Promise.all(Array.from({ length: 8 }, () => f.get(f.map)));
  assert.equal(f.calls.map.length, 1);
  assert.equal(f.calls.reads, 8, 'failed body loads never reach the final authority read');
  for (const response of failed) {
    assert.equal(response.status, 500); assert.equal(response.headers.get('cache-control'), 'no-store');
    await response.arrayBuffer();
  }
  f.setMapLoad(async () => [f.mapRow]);
  const recovered = await f.get(f.map);
  assert.equal(recovered.status, 200); await recovered.arrayBuffer();
  assert.equal(f.calls.map.length, 2, 'a failed cohort does not poison the next dispatch');
  const entered = deferred<void>(), release = deferred<void>();
  t.after(() => release.resolve());
  f.setHistoryLoad(async () => { entered.resolve(); await release.promise; return [f.historyRow]; });
  const active = f.get(f.history); await entered.promise;
  f.close(); release.resolve();
  const settled = await active;
  assert.equal(settled.status, 200, 'already-dispatched DB work keeps its existing lifetime');
  await settled.arrayBuffer();
  const before = [f.calls.map.length, f.calls.history.length];
  for (const route of [f.map, f.history]) {
    const response = await f.get(route); assert.equal(response.status, 500); await response.arrayBuffer();
  }
  assert.deepEqual([f.calls.map.length, f.calls.history.length], before, 'both route-owned batchers are closed');
});
