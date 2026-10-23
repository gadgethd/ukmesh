import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdir, writeFile } from 'node:fs/promises';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import { promisify } from 'node:util';
import pg, { type QueryResultRow } from 'pg';
import express, { Router, type RequestHandler } from 'express';
import { getNodeHistory } from '../../db/index.js';
import { rfValidationRows } from '../../repositories/networkAnalysis.js';
import { networkFilters } from '../utils/networkFilters.js';
import { registerExportRoutes } from './exports.js';

const perfTest = process.env['AUDITWAVE_PERF_TESTS'] === '1' ? test : test.skip;
const execFileAsync = promisify(execFile);
const evidence = '../.fix-backups-auditwave-20260907/evidence';
const summaries: unknown[] = [];
function literal(value: unknown): string {
  if (Array.isArray(value)) return `ARRAY[${value.map(literal).join(',')}]::text[]`;
  if (typeof value === 'number') { assert.ok(Number.isFinite(value)); return String(value); }
  return `'${String(value).replace(/'/g, "''")}'`;
}
async function select(sql: string) {
  const child = execFileAsync('docker', ['exec', '-i', '-e',
    'PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=4500',
    'meshcore-infra-timescaledb-1', 'psql', '-X', '-U', 'meshcore', '-d', 'meshcore',
    '-v', 'ON_ERROR_STOP=1', '-At'], { timeout: 6_000, maxBuffer: 12 * 1024 * 1024 });
  child.child.stdin!.end(sql);
  return JSON.parse((await child).stdout);
}
function measuredQuery(label: string) {
  let calls = 0;
  return async <T extends QueryResultRow = QueryResultRow>(sql: string, params: unknown[] = []): Promise<{ rows: T[] }> => {
    const name = `${label}-${++calls}`;
    const bound = sql.replace(/\$(\d+)\b/g, (_p, index: string) => literal(params[Number(index) - 1]));
    await mkdir(evidence, { recursive: true });
    await writeFile(`${evidence}/${name}.sql`, bound);
    const plan = await select(`EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON) ${bound}`);
    await writeFile(`${evidence}/${name}-explain.json`, JSON.stringify(plan, null, 2));
    const rows = await select(`SELECT COALESCE(jsonb_agg(to_jsonb(result)),'[]'::jsonb) FROM (${bound}) result`);
    const summary = { name, rows: rows.length, planningMs: plan[0]['Planning Time'], executionMs: plan[0]['Execution Time'] };
    summaries.push(summary);
    await writeFile(`${evidence}/performance-summary.json`, JSON.stringify(summaries, null, 2));
    console.log(JSON.stringify(summary));
    assert.ok(plan[0]['Execution Time'] < 4500);
    return { rows };
  };
}

perfTest('EXPLAIN actual RF repository queries on the live read-only database', { timeout: 20_000 }, async () => {
  const { rows } = await rfValidationRows(measuredQuery('rf'), networkFilters('ukmesh'), 250);
  assert.equal(rows.length, 250);
  assert.ok(rows.some(row => row.classification === 'observed_unexpected'));
});

perfTest('EXPLAIN actual history queries for Lyneham, Gadget and an empty id, including maximum window', { timeout: 40_000 }, async (t) => {
  const gadgetRows = await select("SELECT jsonb_agg(n) FROM (SELECT node_id,name FROM node_identity_nodes WHERE name ILIKE 'Gadget-RPT%') n");
  assert.ok(gadgetRows.length > 0);
  for (const [name, id, hours] of [
    ['lyneham', 'C42E3FE2D99234E0E41967C1B70E49FA6F955AD5B8448A06EC2978AB0FF9E79E', 24],
    ['gadget', gadgetRows[0].node_id, 24],
    ['empty', '0'.repeat(64), 24],
    ['lyneham-max-window', 'C42E3FE2D99234E0E41967C1B70E49FA6F955AD5B8448A06EC2978AB0FF9E79E', 672],
  ] as const) {
    const mocked = t.mock.method(pg.Pool.prototype, 'query', measuredQuery(`history-${name}`));
    try {
      const rows = await getNodeHistory(id, hours, 'ukmesh');
      assert.ok(rows.length <= 500);
      if (name === 'empty') assert.equal(rows.length, 0);
      else assert.ok(rows.length > 0);
    } finally { mocked.mock.restore(); }
  }
});

perfTest('EXPLAIN actual node export SQL including its 5000-node maximum', { timeout: 15_000 }, async () => {
  const app = express(), router = Router();
  const limiter: RequestHandler = (_req, _res, next) => next();
  registerExportRoutes(router, { networkFilters,
    exportLimiter: limiter as Parameters<typeof registerExportRoutes>[1]['exportLimiter'],
    getPublicVisibilityGeneration: async () => 1, query: measuredQuery('exports') });
  app.use(router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    const response = await fetch(`http://127.0.0.1:${(server.address() as AddressInfo).port}/v1/exports/nodes.geojson?network=ukmesh&limit=5000`);
    assert.equal(response.status, 200);
    const body = await response.json() as { features: unknown[] };
    assert.ok(body.features.length > 0 && body.features.length <= 5000);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
