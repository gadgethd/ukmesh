import assert from 'node:assert/strict';
import test from 'node:test';
import { frontendCls, frontendInp, frontendLcp } from '../metrics/frontend.js';
import { recordWebVital } from './webVitals.js';
import express, { Router } from 'express';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import { registerTelemetryRoutes } from '../api/routes/telemetry.js';

test('Web Vitals labels and payloads are fixed, unit-correct and never contain attribution', async () => {
  frontendCls.reset(); frontendInp.reset(); frontendLcp.reset();
  for (const body of [null, [], {}, { route: '/docs?token=example', metric: 'LCP', value: 1 },
    { route: '/docs', metric: 'unknown', value: 1 }, { route: '/docs', metric: 'CLS', value: 11 },
    { route: '/docs', metric: 'LCP', value: -1 }, { route: '/docs', metric: 'INP', value: 600_001 },
    { route: '/docs', metric: 'LCP', value: Number.NaN }, { route: '/docs', metric: 'LCP', value: 1, id: 'invented-id' },
  ]) assert.equal(recordWebVital(body), false);
  assert.equal(recordWebVital({ route: '/docs', metric: 'LCP', value: 2500 }), true);
  assert.equal(recordWebVital({ route: '/docs', metric: 'INP', value: 150 }), true);
  assert.equal(recordWebVital({ route: '/docs', metric: 'CLS', value: 0 }), true);
  for (const [histogram, sum] of [[frontendLcp, 2.5], [frontendInp, 0.15], [frontendCls, 0]] as const) {
    const metric = await histogram.get();
    assert.equal(metric.values.find(row => row.metricName?.endsWith('_sum'))?.value, sum);
    assert.equal(metric.values.find(row => row.metricName?.endsWith('_count'))?.value, 1);
    assert.ok(metric.values.every(row => row.labels.route === '/docs'));
  }
});

test('the HTTP receiver accepts a bounded report without database work', async t => {
  const app = express(); app.use(express.json({ limit: '256b' }));
  const router = Router();
  registerTelemetryRoutes(router, { query: async () => { throw new Error('Web Vitals must not access SQL'); } });
  app.use('/api', router);
  const server = app.listen(0, '127.0.0.1');
  t.after(() => { server.closeAllConnections(); server.close(); });
  await once(server, 'listening');
  const endpoint = `http://127.0.0.1:${(server.address() as AddressInfo).port}/api/telemetry/web-vitals`;
  const accepted = await fetch(endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ route: '/regions', metric: 'INP', value: 120 }) });
  assert.equal(accepted.status, 202);
  assert.equal(accepted.headers.get('cache-control'), 'no-store');
  assert.deepEqual(await accepted.json(), { ok: true });
  const rejected = await fetch(endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ route: '/regions', metric: 'INP', value: 120, attribution: 'must not persist' }) });
  assert.equal(rejected.status, 400);
});
