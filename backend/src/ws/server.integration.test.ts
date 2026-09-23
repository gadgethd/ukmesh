import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import test from 'node:test';
import { WebSocket } from 'ws';

// Use an EMPTY disposable database so initial-state reads deliberately fail.
// Dedicated variable names prevent accidentally using an application database.
const databaseUrl = process.env['WS_LIFECYCLE_TEST_DATABASE_URL'];
const redisUrl = process.env['WS_LIFECYCLE_TEST_REDIS_URL'];
test('failed WebSocket initial state still releases client bookkeeping', {
  skip: !databaseUrl || !redisUrl,
  timeout: 15_000,
}, async () => {
  process.env['DATABASE_URL'] = databaseUrl;
  process.env['REDIS_URL'] = redisUrl;
  process.env['WS_INITIAL_STATE_ENABLED'] = '1';
  process.env['WARMUP_NETWORKS'] = '';
  const { initWebSocketServer, closeWebSocketServer } = await import('./server.js');
  const { closeDb } = await import('../db/index.js');
  const { websocketClients } = await import('../metrics.js');
  const server = http.createServer();
  const wss = initWebSocketServer(server);
  let client: WebSocket | undefined;
  try {
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    const address = server.address();
    assert.ok(address && typeof address !== 'string');
    client = new WebSocket(`ws://127.0.0.1:${address.port}/ws?network=ukmesh`);
    const [code] = await once(client, 'close');
    assert.equal(code, 1013);
    // The client can observe its close frame before the server's close event.
    for (let attempt = 0; attempt < 50 && wss.clients.size > 0; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.equal(wss.clients.size, 0);
    assert.equal((await websocketClients.get()).values[0]?.value, 0);
  } finally {
    client?.terminate();
    await closeWebSocketServer(wss);
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await closeDb();
  }
});
