import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import { promisify } from 'node:util';
import test from 'node:test';
import express from 'express';
import pg from 'pg';
import { createStatsService } from '../stats/statsService.js';
import type { StatsRepository } from '../stats/statsRepository.js';
import { publicJson } from './publicJson.js';

const source = process.env['TEST_INGEST_DATABASE_URL'];
const local = source && ['127.0.0.1', 'localhost'].includes(new URL(source).hostname);

test('reused initial reads retain fresh HTTP fences against another SQL process and read failure', {
  skip: local ? false : 'owned loopback PostgreSQL required', timeout: 30_000,
}, async t => {
  const admin = new pg.Pool({ connectionString: source, max: 1 });
  const name = `public_fence_${process.pid}_${Date.now()}`;
  await admin.query(`CREATE DATABASE "${name}"`);
  const url = new URL(source!); url.pathname = '/' + name;
  const pool = new pg.Pool({ connectionString: url.href, max: 1 });
  t.after(async () => { await pool.end(); await admin.query(`DROP DATABASE "${name}" WITH (FORCE)`); await admin.end(); });
  await pool.query('CREATE TABLE authority (generation integer NOT NULL); INSERT INTO authority VALUES (1)');
  let reads = 0;
  const read = async () => {
    reads++;
    return Number((await pool.query('SELECT generation FROM authority')).rows[0].generation);
  };
  const statsCache = new Map([
    ['ukmesh:v1', { ts: Date.now(), data: { redacted: false } }],
    ['ukmesh:v2', { ts: Date.now(), data: { redacted: true } }],
  ]);
  const service = createStatsService({ statsCache, statsCacheTtlMs: 60_000,
    chartsCache: new Map(), chartsCacheTtlMs: 60_000, chartsInflight: new Map(),
    repository: {} as StatsRepository, getPublicVisibilityGeneration: read, maskDecodedPathNodes: () => [] });
  const mutateElsewhere = async (sql: string) => promisify(execFile)(process.execPath, ['--input-type=module', '-e', `
    import pg from 'pg';
    const pool = new pg.Pool({connectionString:process.env.TEST_GENERATION_DATABASE_URL,max:1});
    try { await pool.query(process.env.TEST_GENERATION_SQL); } finally { await pool.end(); }
  `], { env: { ...process.env, TEST_GENERATION_DATABASE_URL: url.href, TEST_GENERATION_SQL: sql }, timeout: 10_000 });
  let mutation: string | undefined;
  const app = express();
  app.get('/', async (_req, res) => {
    reads = 0;
    try {
      await publicJson(res, read, async generation => {
        const data = await service.getStatsSummary('ukmesh', undefined, generation);
        if (mutation) await mutateElsewhere(mutation);
        return data;
      });
    } catch { res.status(503).json({ error: 'Unavailable' }); }
  });
  const server = app.listen(0, '127.0.0.1');
  t.after(() => { server.closeAllConnections(); server.close(); });
  await once(server, 'listening');
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  const first = await fetch(base);
  assert.deepEqual(await first.json(), { redacted: false });
  assert.equal(reads, 2, 'one initial SQL read shared with the service and one fresh response read');
  const conditional = { cache: 'force-cache' as const, headers: { 'If-None-Match': first.headers.get('etag')! } };
  assert.equal((await fetch(base, conditional)).status, 304);
  assert.equal(reads, 2);
  mutation = 'UPDATE authority SET generation=2';
  const changed = await fetch(base, conditional);
  assert.equal(changed.status, 503);
  assert.equal(changed.headers.get('cache-control'), 'no-store');
  assert.deepEqual(await changed.json(), { error: 'Unavailable' });
  assert.equal(reads, 2);
  mutation = undefined;
  const current = await fetch(base, conditional);
  assert.equal(current.status, 200);
  assert.deepEqual(await current.json(), { redacted: true });
  mutation = 'DROP TABLE authority';
  const failed = await fetch(base);
  assert.equal(failed.status, 503);
  assert.equal(failed.headers.get('cache-control'), 'no-store');
  assert.deepEqual(await failed.json(), { error: 'Unavailable' });
  const initialFailure = await fetch(base, conditional);
  assert.equal(initialFailure.status, 503);
  assert.equal(initialFailure.headers.get('cache-control'), 'no-store');
  assert.equal(reads, 1, 'an unavailable initial authority cannot reach a cached body');
});
