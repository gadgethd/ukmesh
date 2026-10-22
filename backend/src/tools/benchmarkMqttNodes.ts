// Read-only measurement adapter for the actual HTTP handler. No application
// listener or configuration is changed on the remote host; psql uses stdin.
// Run from backend: node --import tsx src/tools/benchmarkMqttNodes.ts
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { once } from 'node:events';
import { readFile } from 'node:fs/promises';
import type { AddressInfo } from 'node:net';
import { resolve } from 'node:path';
import { performance } from 'node:perf_hooks';
import { promisify } from 'node:util';
import express, { Router, type RequestHandler } from 'express';
import type { QueryResultRow } from 'pg';
import { registerMiscRoutes } from '../api/routes/misc.js';

const target = process.env['MQTT_PERF_SSH_TARGET'];
const port = process.env['MQTT_PERF_SSH_PORT'] ?? '32720';
const rawControlPath = process.env['MQTT_PERF_SSH_CONTROL_PATH'];
const controlPath = rawControlPath ? resolve(rawControlPath) : undefined;
if (controlPath && Buffer.byteLength(controlPath) > 85) {
  throw new Error('SSH control socket path must be at most 85 bytes, leaving room for its temporary suffix');
}
if (!target || !/^[a-zA-Z0-9_.@-]+$/.test(target) || !/^\d+$/.test(port)) {
  throw new Error('Set MQTT_PERF_SSH_TARGET to the authorized read-only VPS target');
}
const exec = promisify(execFile);
const originalSql = (await readFile(new URL('../api/routes/fixtures/mqtt-nodes-before.sql', import.meta.url), 'utf8')).trim().replace(/;$/, '');
const median = (values: number[]) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]!;

async function query<T extends QueryResultRow>(sql: string, params?: unknown[]): Promise<{ rows: T[] }> {
  const bound = sql.replace(/\$(\d+)::text\[\]/g, (_token, index: string) => {
    const values = params?.[Number(index) - 1];
    assert.ok(Array.isArray(values));
    assert.ok(values.every((value) => ['ukmesh', 'northeast', 'teesside', 'test'].includes(value)));
    return `ARRAY[${values.map((value) => `'${value}'`).join(',')}]::text[]`;
  });
  const child = exec('ssh', ['-p', port, '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', '-o', 'ProxyJump=none',
    ...(controlPath ? ['-o', 'ControlMaster=auto', '-o', 'ControlPersist=60', '-o', `ControlPath=${controlPath}`] : []), target!,
    'docker exec -i -e PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=30000 -c lock_timeout=2000" meshcore-infra-timescaledb-1 psql -X -qAt -U meshcore -d meshcore -v ON_ERROR_STOP=1',
  ], { maxBuffer: 2 * 1024 * 1024, timeout: 40_000 });
  // psql's JSON transport explicitly preserves pg's bigint string behavior.
  child.child.stdin!.end(`BEGIN READ ONLY;
    SELECT COALESCE(jsonb_agg(to_jsonb(q) || jsonb_build_object(
      'packets_24h', q.packets_24h::text, 'uptime_secs', q.uptime_secs::text)), '[]'::jsonb)
    FROM (${bound}) q;
    ROLLBACK;`);
  const { stdout } = await child;
  const rows = JSON.parse(stdout) as T[];
  for (const row of rows) {
    (row as QueryResultRow)['last_seen'] = new Date(row['last_seen']).toISOString();
  }
  return { rows };
}

for (const mode of ['before', 'after', 'cache-prototype'] as const) {
  const app = express();
  let databaseQueries = 0;
  let cachedBody: string | undefined;
  let expiresAt = 0;
  if (mode === 'cache-prototype') {
    app.use('/mqtt-nodes', (_req, res, next) => {
      if (cachedBody !== undefined && performance.now() < expiresAt) {
        res.type('json').send(cachedBody);
        return;
      }
      const send = res.json.bind(res);
      res.json = (body) => {
        if (res.statusCode === 200) {
          cachedBody = JSON.stringify(body);
          expiresAt = performance.now() + 15_000;
        }
        return send(body);
      };
      next();
    });
  }
  const router = Router();
  const passThrough: RequestHandler = (_req, _res, next) => next();
  registerMiscRoutes(router, {
    query: async (sql, params) => {
      databaseQueries++;
      return query(mode === 'after' ? sql : originalSql, params);
    },
    getRecentPackets: async () => [], getRecentPacketEvents: async () => [],
    getPacketDetail: async () => null, getChannelMessageHistory: async () => [],
    getRecentMessageTags: async () => [], getPublicVisibilityGeneration: async () => 1,
    packetDetailLimiter: passThrough as Parameters<typeof registerMiscRoutes>[1]['packetDetailLimiter'],
  });
  app.use(router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address() as AddressInfo;
  const samples: number[] = [];
  try {
    for (let index = 0; index < (mode === 'cache-prototype' ? 6 : 5); index++) {
      const start = performance.now();
      const response = await fetch(`http://127.0.0.1:${address.port}/mqtt-nodes`);
      assert.equal(response.status, 200);
      assert.ok(Array.isArray(await response.json()));
      samples.push(performance.now() - start);
    }
    console.log(JSON.stringify({ mode, scope: ['ukmesh', 'northeast', 'teesside'],
      samples_ms: samples.map((value) => Number(value.toFixed(3))),
      p50_ms: Number(median(samples).toFixed(3)), database_queries: databaseQueries,
      ...(mode === 'cache-prototype' ? { cold_ms: Number(samples[0]!.toFixed(3)),
        hit_p50_ms: Number(median(samples.slice(1)).toFixed(3)), ttl_ms: 15_000 } : {}),
    }));
  } finally { await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve())); }
}
