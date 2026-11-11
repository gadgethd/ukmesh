import assert from 'node:assert/strict';
import test from 'node:test';
import { once } from 'node:events';
import { request as httpRequest } from 'node:http';
import type { AddressInfo } from 'node:net';
import express, { Router, type RequestHandler } from 'express';
import { networkFilters } from '../utils/networkFilters.js';
import { csvCell, registerExportRoutes } from './exports.js';

test('csvCell escapes delimiters, quotes, and newlines', () => {
  assert.equal(csvCell('simple'), 'simple');
  assert.equal(csvCell('hello, world'), '"hello, world"');
  assert.equal(csvCell('node "alpha"'), '"node ""alpha"""');
  assert.equal(csvCell('line one\nline two'), '"line one\nline two"');
  assert.equal(csvCell(null), '');
});

test('node exports use the sightings alias and retain CSV/GeoJSON contracts', async () => {
  const app = express();
  const router = Router();
  const row = {
    node_id: 'A'.repeat(64), name: 'Repeater, "Alpha"', lat: 51.5, lon: -1.2,
    role: 2, iata: 'LHR', last_seen: '2026-09-07T12:00:00Z', hardware_model: null,
  };
  const limiter: RequestHandler = (_req, _res, next) => next();
  registerExportRoutes(router, {
    networkFilters,
    exportLimiter: limiter as Parameters<typeof registerExportRoutes>[1]['exportLimiter'],
    getPublicVisibilityGeneration: async () => 1,
    query: async (sql, params) => {
      assert.match(sql, /FROM node_identity_nodes AS nodes/);
      assert.match(sql, /s.node_id = nodes.node_id/);
      assert.match(sql, /name NOT LIKE '%🚫%'/);
      assert.match(sql, /lat IS NOT NULL AND lon IS NOT NULL/);
      assert.match(sql, /LIMIT \$2/);
      assert.deepEqual(params, [['ukmesh', 'northeast', 'teesside'], 3]);
      return { rows: [row] as never[] };
    },
  });
  app.use('/api', router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    const csv = await fetch(`${base}/api/v1/exports/nodes.csv?network=ukmesh&limit=3`);
    assert.equal(csv.status, 200);
    assert.match(csv.headers.get('content-type')!, /text\/csv/);
    assert.equal(csv.headers.get('x-api-version'), '1');
    assert.equal(await csv.text(), `node_id,name,lat,lon,role,iata,last_seen,hardware_model\n${row.node_id},"Repeater, ""Alpha""",51.5,-1.2,2,LHR,2026-09-07T12:00:00Z,\n`);
    const geo = await fetch(`${base}/api/v1/exports/nodes.geojson?network=ukmesh&limit=3`);
    assert.equal(geo.status, 200);
    assert.match(geo.headers.get('content-type')!, /application\/geo\+json/);
    const body = await geo.json() as { type: string; features: Array<{ geometry: unknown; properties: { node_id: string } }> };
    assert.equal(body.type, 'FeatureCollection');
    assert.equal(body.features.length, 1);
    assert.deepEqual(body.features[0]!.geometry, { type: 'Point', coordinates: [-1.2, 51.5] });
    assert.equal(body.features[0]!.properties.node_id, row.node_id);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});

async function exportServer(options: {
  generation: () => Promise<number>;
  rows: Array<Record<string, unknown>>;
}): Promise<{ base: string; close: () => Promise<void> }> {
  const app = express();
  const router = Router();
  const limiter: RequestHandler = (_req, _res, next) => next();
  registerExportRoutes(router, {
    networkFilters,
    exportLimiter: limiter as Parameters<typeof registerExportRoutes>[1]['exportLimiter'],
    getPublicVisibilityGeneration: options.generation,
    query: async () => ({ rows: options.rows as never[] }),
  });
  app.use('/api', router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return {
    base: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

/** `fetch` marks every request `cache-control: no-cache`, which is deliberately
 * never fresh, so a conditional revalidation has to use the plain HTTP client. */
function httpGet(
  url: string,
  headers: Record<string, string> = {},
): Promise<{ status: number; headers: Record<string, string | string[] | undefined>; body: string }> {
  const target = new URL(url);
  return new Promise((resolve, reject) => {
    const request = httpRequest({
      hostname: target.hostname,
      port: target.port,
      path: `${target.pathname}${target.search}`,
      method: 'GET',
      headers,
    }, (response) => {
      let body = '';
      response.setEncoding('utf8');
      response.on('data', (chunk: string) => { body += chunk; });
      response.on('end', () => resolve({
        status: response.statusCode ?? 0,
        headers: response.headers,
        body,
      }));
    });
    request.on('error', reject);
    request.end();
  });
}

const cachedRow = {
  node_id: 'B'.repeat(64), name: 'Repeater B', lat: 51.5, lon: -1.2,
  role: 2, iata: 'LHR', last_seen: '2026-09-07T12:00:00Z', hardware_model: null,
};

test('node export caches are keyed to the public visibility generation', async () => {
  let generation = 7;
  const server = await exportServer({ rows: [cachedRow], generation: async () => generation });
  try {
    const url = `${server.base}/api/v1/exports/nodes.csv?network=ukmesh&limit=3`;
    const first = await fetch(url);
    assert.equal(first.status, 200);
    assert.equal(first.headers.get('cache-control'), 'public, max-age=60, stale-while-revalidate=60');
    assert.equal(first.headers.get('x-visibility-generation'), '7');
    assert.match(first.headers.get('vary') ?? '', /Accept-Encoding/);
    const etag = first.headers.get('etag');
    assert.match(etag ?? '', /^W\/"nodes-7-/);
    await first.text();

    const revalidated = await httpGet(url, { 'If-None-Match': etag! });
    assert.equal(revalidated.status, 304);
    assert.equal(revalidated.body, '');

    const stable = await fetch(url);
    assert.equal(stable.headers.get('etag'), etag);
    await stable.text();

    // A redaction bumps the generation: the validator changes and a caller that
    // names the superseded generation is refused instead of served stale rows.
    generation = 8;
    const bumped = await fetch(url);
    assert.match(bumped.headers.get('etag') ?? '', /^W\/"nodes-8-/);
    assert.notEqual(bumped.headers.get('etag'), etag);
    assert.equal(bumped.headers.get('x-visibility-generation'), '8');
    await bumped.text();

    const stale = await fetch(`${url}&v=7`);
    assert.equal(stale.status, 412);
    assert.equal(stale.headers.get('cache-control'), 'no-store');
    assert.equal(stale.headers.get('x-visibility-generation'), '8');
    assert.equal(
      (await stale.json() as { visibility_generation: number }).visibility_generation,
      8,
    );

    const current = await fetch(`${url}&v=8`);
    assert.equal(current.status, 200);
    await current.text();
  } finally {
    await server.close();
  }
});

test('node exports fail closed when the visibility generation is unavailable', async () => {
  const server = await exportServer({
    rows: [cachedRow],
    generation: async () => { throw new Error('PUBLIC_VISIBILITY_GENERATION_UNAVAILABLE'); },
  });
  try {
    const response = await fetch(`${server.base}/api/v1/exports/nodes.geojson?network=ukmesh`);
    assert.equal(response.status, 503);
    assert.equal(response.headers.get('cache-control'), 'no-store');
    assert.equal(response.headers.get('x-visibility-generation'), null);
    await response.text();
  } finally {
    await server.close();
  }
});
