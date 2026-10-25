import assert from 'node:assert/strict';
import test from 'node:test';
import { once } from 'node:events';
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
