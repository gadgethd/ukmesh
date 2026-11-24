import assert from 'node:assert/strict';
import http from 'node:http';
import test from 'node:test';
import express from 'express';
import type { QueryResultRow } from 'pg';
import { createHopReachCompatibilityRoutes } from './hopreachCompatibility.js';

const KEY_A = 'a'.repeat(64);
const KEY_B = 'b'.repeat(64);

test('HopReach compatibility API paginates repeaters and returns observed links in bulk', async () => {
  const seen: Array<{ sql: string; params?: unknown[] }> = [];
  const query = async <T extends QueryResultRow>(sql: string, params?: unknown[]) => {
    seen.push({ sql, params });
    if (sql.includes('COUNT(*)')) return { rows: [{ total: '1' }] as T[] };
    if (sql.includes('WITH requested')) {
      return { rows: [{
        source_id: KEY_A,
        pubkey: KEY_B,
        name: 'Observed peer',
        lat: 52,
        lon: -1,
        bottleneck: 4,
        bidir: true,
      }] as T[] };
    }
    return { rows: [{
      public_key: KEY_A,
      name: 'Repeater',
      lat: 51.5,
      lon: -0.1,
      last_heard: new Date('2026-08-01T12:00:00Z'),
      first_seen: new Date('2025-01-01T00:00:00Z'),
      advert_count: 12,
    }] as T[] };
  };

  const app = express();
  app.use(express.json({ limit: '512kb' }));
  app.use(createHopReachCompatibilityRoutes(query));
  const server = http.createServer(app);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  try {
    const address = server.address();
    assert(address && typeof address === 'object');
    const base = `http://127.0.0.1:${address.port}`;
    const nodes = await fetch(`${base}/api/nodes?role=repeater&limit=500&offset=0`).then((res) => res.json()) as Record<string, unknown>;
    assert.equal(nodes['total'], 1);
    assert.equal((nodes['nodes'] as Array<Record<string, unknown>>)[0]?.['role'], 'repeater');

    const reach = await fetch(`${base}/api/reach/bulk`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ public_keys: [KEY_A], days: 14 }),
    }).then((res) => res.json()) as { links_by_public_key: Record<string, unknown[]> };
    assert.equal(reach.links_by_public_key[KEY_A]?.length, 1);
    assert(seen.some(({ sql, params }) => sql.includes('nl.observed_count > 0') && (params?.[1] === 14)));
    assert(seen.every(({ sql }) => !sql.includes('node_coverage')));
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
});

test('HopReach compatibility API rejects forwarded public traffic', async () => {
  const app = express();
  app.use(createHopReachCompatibilityRoutes(async () => ({ rows: [] })));
  const server = http.createServer(app);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  try {
    const address = server.address();
    assert(address && typeof address === 'object');
    const response = await fetch(`http://127.0.0.1:${address.port}/api/nodes`, {
      headers: { 'x-forwarded-for': '203.0.113.8' },
    });
    assert.equal(response.status, 404);
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
});

test('HopReach compatibility API handles UK pagination and coalesces concurrent calibration loads', async () => {
  let countQueries = 0;
  let pageQueries = 0;
  let reachQueries = 0;
  const query = async <T extends QueryResultRow>(sql: string, params?: unknown[]) => {
    if (sql.includes('COUNT(*)')) {
      countQueries += 1;
      await new Promise((resolve) => setTimeout(resolve, 5));
      return { rows: [{ total: '4600' }] as T[] };
    }
    if (sql.includes('WITH requested')) {
      reachQueries += 1;
      // Keep the first database read open long enough for every independently
      // established HTTP connection to enter the adapter's single-flight map.
      await new Promise((resolve) => setTimeout(resolve, 250));
      return { rows: [] as T[] };
    }
    pageQueries += 1;
    const limit = Number(params?.[0] ?? 500);
    const offset = Number(params?.[1] ?? 0);
    const count = Math.min(limit, 4_600 - offset);
    return {
      rows: Array.from({ length: Math.max(0, count) }, (_, index) => ({
        public_key: (offset + index).toString(16).padStart(64, '0'),
        name: `Repeater ${offset + index}`,
        lat: 50 + ((offset + index) % 1_000) / 100,
        lon: -8 + ((offset + index) % 800) / 100,
        last_heard: '2026-08-01T12:00:00Z',
        first_seen: '2025-01-01T00:00:00Z',
        advert_count: 1,
      })) as T[],
    };
  };

  const app = express();
  app.use(express.json({ limit: '512kb' }));
  app.use(createHopReachCompatibilityRoutes(query));
  const server = http.createServer(app);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  try {
    const address = server.address();
    assert(address && typeof address === 'object');
    const base = `http://127.0.0.1:${address.port}`;
    const pageUrls = Array.from(
      { length: 10 },
      (_, page) => `${base}/api/nodes?limit=500&offset=${page * 500}`,
    );
    const coldPages = await Promise.all(pageUrls.map((url) => fetch(url)));
    assert(coldPages.every((response) => response.status === 200));
    assert.equal(countQueries, 1);
    assert.equal(pageQueries, 10);
    assert.equal((await coldPages[9]?.json() as { nodes: unknown[] }).nodes.length, 100);

    const warmPages = await Promise.all(pageUrls.map((url) => fetch(url)));
    assert(warmPages.every((response) => response.headers.get('x-hopreach-cache') === 'HIT'));
    assert.equal(countQueries, 1);
    assert.equal(pageQueries, 10);

    const calibrationRequest = () => fetch(`${base}/api/reach/bulk`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ public_keys: [KEY_A, KEY_B], days: 14 }),
    });
    const concurrent = await Promise.all(Array.from({ length: 32 }, calibrationRequest));
    assert(concurrent.every((response) => response.status === 200));
    assert.equal(reachQueries, 1);
    assert(concurrent.some((response) => response.headers.get('x-hopreach-cache') === 'COALESCED'));
    const warmCalibration = await calibrationRequest();
    assert.equal(warmCalibration.headers.get('x-hopreach-cache'), 'HIT');
    assert.equal(reachQueries, 1);
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
});

test('public RF acquisition fences every page by one converged generation', async () => {
  let generation = 62;
  let changeDuringPage = false;
  let policyReads = 0;
  const query = async <T extends QueryResultRow>(sql: string, params?: unknown[]) => {
    if (sql.includes('ops.public_visibility_state')) {
      policyReads += 1;
      return { rows: [{ generation: String(generation) }] as T[] };
    }
    assert(sql.includes('FROM core.private_node_prefixes') && sql.includes('FROM core.node_identity_aliases'));
    const offset = Number(params?.[1]);
    if (changeDuringPage) generation += 1;
    return { rows: [{
      public_key: offset === 0 ? KEY_A : KEY_B, name: 'Repeater',
      lat: 50, lon: -2, last_heard: null, first_seen: null, advert_count: 1, total: '2',
    }] as T[] };
  };
  const app = express();
  app.use('/hopreach', createHopReachCompatibilityRoutes(query));
  const server = http.createServer(app);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  try {
    const address = server.address();
    assert(address && typeof address === 'object');
    const nodes = `http://127.0.0.1:${address.port}/hopreach/api/nodes?projection=public&limit=1`;
    const first = await fetch(nodes);
    assert.equal(first.status, 200);
    assert.equal(first.headers.get('cache-control'), 'no-store');
    assert.deepEqual((await first.json() as { visibility_generation: number }).visibility_generation, generation);
    assert.equal((await fetch(nodes + '&offset=1')).status, 400);
    assert.equal((await fetch(nodes + '&offset=1&generation=61')).status, 503);
    const second = await fetch(nodes + '&offset=1&generation=62');
    assert.equal(second.status, 200);
    assert.equal((await second.json() as { nodes: Array<{ public_key: string }> }).nodes[0]?.public_key, KEY_B);
    assert.equal(policyReads, 5);

    changeDuringPage = true;
    const raced = await fetch(nodes);
    assert.equal(raced.status, 503);
    assert(!(await raced.text()).includes(KEY_A));
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
});
