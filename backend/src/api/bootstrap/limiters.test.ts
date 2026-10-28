import assert from 'node:assert/strict';
import test from 'node:test';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import express, { Router } from 'express';
import {
  COVERAGE_LIMITER,
  EXPORT_LIMITER,
  PATH_BETA_LIMITER,
  STATS_CHARTS_LIMITER,
  createGlobalApiLimiter,
  parseApiRateLimitMax,
} from './limiters.js';
import { registerTelemetryRoutes } from '../routes/telemetry.js';

test('global API rate limit keeps a safe production default and bounded overrides', () => {
  assert.equal(parseApiRateLimitMax(undefined), 120);
  assert.equal(parseApiRateLimitMax('1'), 1);
  assert.equal(parseApiRateLimitMax('1000000'), 1_000_000);
  for (const invalid of ['0', '-1', '1.5', 'NaN', '1000001']) {
    assert.throws(() => parseApiRateLimitMax(invalid), /API_RATE_LIMIT_MAX/);
  }
});

async function withLimitedApi(callback: (base: string) => Promise<void>, max = '2') {
  const previous = process.env['API_RATE_LIMIT_MAX'];
  process.env['API_RATE_LIMIT_MAX'] = max;
  let limiter: ReturnType<typeof createGlobalApiLimiter>;
  try {
    limiter = createGlobalApiLimiter();
  } finally {
    if (previous === undefined) delete process.env['API_RATE_LIMIT_MAX'];
    else process.env['API_RATE_LIMIT_MAX'] = previous;
  }
  const app = express();
  app.use(express.json());
  app.use('/api', limiter);
  const router = Router();
  registerTelemetryRoutes(router, {
    query: async () => { throw new Error('Malformed telemetry must not reach the database'); },
  });
  router.get('/stats', (_req, res) => { res.json({ mqttNodes: 21 }); });
  router.get('/stats/charts', STATS_CHARTS_LIMITER, (_req, res) => { res.json({}); });
  router.get('/path-beta/resolve-multi', PATH_BETA_LIMITER, (_req, res) => { res.json({}); });
  router.post('/coverage/planned', COVERAGE_LIMITER, (_req, res) => { res.json({}); });
  router.get('/exports/test', EXPORT_LIMITER, (_req, res) => { res.json({}); });
  router.get('/other', (_req, res) => { res.json({}); });
  app.use('/api', router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    await callback(`http://127.0.0.1:${(server.address() as AddressInfo).port}/api`);
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
}

test('telemetry keeps its strict quota without consuming API or reserved stats quotas', async () => {
  await withLimitedApi(async (base) => {
    for (let i = 0; i < 12; i += 1) {
      const response = await fetch(`${base}/Telemetry/Frontend-Error/`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}',
      });
      assert.equal(response.status, i < 10 ? 400 : 429);
      if (i >= 10) assert.match(await response.text(), /Too many diagnostic reports/);
      else await response.arrayBuffer();
    }
    for (const expected of [200, 200, 429]) {
      const response = await fetch(`${base}/other`);
      assert.equal(response.status, expected);
      await response.arrayBuffer();
    }
    for (const method of ['GET', 'HEAD']) {
      const response = await fetch(`${base}/STATS/?network=ukmesh`, { method });
      assert.equal(response.status, 200);
      assert.equal(response.headers.get('RateLimit-Limit'), '240');
      await response.arrayBuffer();
    }
    const charts = await fetch(`${base}/stats/charts`);
    assert.equal(charts.status, 429);
    assert.match(await charts.text(), /Too many requests, please slow down/);
  });
});

test('reserved stats quota is bounded and cannot consume the shared API allowance', async () => {
  await withLimitedApi(async (base) => {
    for (let i = 0; i < 241; i += 1) {
      const response = await fetch(`${base}/stats?network=ukmesh&_ts=${i}`);
      assert.equal(response.status, i < 240 ? 200 : 429);
      if (i === 240) assert.match(await response.text(), /Too many stats requests/);
      else await response.arrayBuffer();
    }
    const other = await fetch(`${base}/other`);
    assert.equal(other.status, 200);
    await other.arrayBuffer();
  });
});

test('path, coverage, export and chart routes retain their strict independent limits', async () => {
  await withLimitedApi(async (base) => {
    for (const [path, method, max] of [
      ['/path-beta/resolve-multi', 'GET', 60],
      ['/coverage/planned', 'POST', 12],
      ['/exports/test', 'GET', 5],
      ['/stats/charts', 'GET', 12],
    ] as const) {
      for (let i = 0; i <= max; i += 1) {
        const response = await fetch(`${base}${path}`, { method });
        assert.equal(response.status, i < max ? 200 : 429, `${path}: attempt ${i + 1}`);
        await response.arrayBuffer();
      }
    }
  }, '1000');
});
