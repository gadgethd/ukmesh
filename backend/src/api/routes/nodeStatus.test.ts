import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router } from 'express';
import {
  NODE_STATUS_LATEST_MAX_ROWS,
  registerNodeStatusRoutes,
  type NodeStatusRouteDeps,
} from './nodeStatus.js';

async function withNodeStatusServer(
  deps: NodeStatusRouteDeps,
  callback: (baseUrl: string) => Promise<void>,
): Promise<void> {
  const router = Router();
  registerNodeStatusRoutes(router, deps);
  const app = express();
  app.use(router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    const { port } = server.address() as AddressInfo;
    await callback(`http://127.0.0.1:${port}`);
  } finally {
    server.close();
  }
}

test('latest node status query is bounded and preserves the response shape', async () => {
  const statements: string[] = [];
  const signals: Array<AbortSignal | undefined> = [];
  const rows = [{ node_id: 'A'.repeat(64), time: '2026-09-12T00:00:00.000Z' }];
  await withNodeStatusServer({
    query: async (text, _params, signal) => {
      statements.push(text);
      signals.push(signal);
      return { rows };
    },
  }, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/node-status/latest`);
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), rows);
  });

  assert.equal(statements.length, 1);
  assert.match(statements[0] ?? '', /ORDER BY time DESC\s+LIMIT \d+/);
  assert.ok(
    (statements[0] ?? '').includes(`LIMIT ${NODE_STATUS_LATEST_MAX_ROWS}`),
    'latest query must cap the result set with the explicit row limit',
  );
  assert.ok(NODE_STATUS_LATEST_MAX_ROWS > 0 && NODE_STATUS_LATEST_MAX_ROWS <= 10_000);
  assert.ok(signals[0] instanceof AbortSignal);
  assert.equal(signals[0]?.aborted, false);
});

test('latest node status query aborts and returns a retryable error at its deadline', async () => {
  let aborted = false;
  const startedAt = Date.now();
  await withNodeStatusServer({
    latestQueryTimeoutMs: 30,
    query: (_text, _params, signal) => new Promise((_resolve, reject) => {
      assert.ok(signal instanceof AbortSignal);
      signal.addEventListener('abort', () => {
        aborted = true;
        reject(signal.reason);
      }, { once: true });
    }),
  }, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/node-status/latest`);
    assert.equal(response.status, 503);
    assert.deepEqual(await response.json(), {
      error: 'Node status query timed out',
      retryable: true,
    });
  });

  assert.equal(aborted, true);
  assert.ok(Date.now() - startedAt < 2_000, 'handler must respect the statement deadline');
});
