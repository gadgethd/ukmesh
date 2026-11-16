import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import test from 'node:test';
import { WebSocketServer } from 'ws';
import { websocketPingCheck, websocketInitialStateCheck } from './syntheticWebSocketChecks.js';
import { encodeInitialStateColumns, INITIAL_STATE_COLUMNS_FORMAT, INITIAL_STATE_FORMAT_PARAMETER } from '../contracts/initialStateColumns.generated.js';

test('synthetic probes exercise real global sockets and validate bootstrap delivery', { timeout: 10_000 }, async t => {
  const server = http.createServer(), wss = new WebSocketServer({ server });
  let reply: unknown = encodeInitialStateColumns({ type: 'initial_state', ts: 1,
    data: { nodes: [{ node_id: 'one' }], packets: [], messages: [], viable_links: [] } });
  let mode: 'send' | 'close' | 'wait' = 'send';
  const requests: URL[] = [];
  wss.on('connection', (socket, request) => {
    const url = new URL(request.url!, 'http://localhost'); requests.push(url);
    if (url.searchParams.get('initial_state') === '0') return;
    if (mode === 'close') socket.close();
    else if (mode === 'send') socket.send(JSON.stringify(reply));
  });
  t.after(async () => {
    wss.clients.forEach(client => client.terminate());
    await new Promise<void>(resolve => wss.close(() => resolve()));
    await new Promise<void>(resolve => server.close(() => resolve()));
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const address = server.address(); assert.ok(address && typeof address !== 'string');
  const base = `ws://127.0.0.1:${address.port}`;
  await t.test('ping opts out and remains a real bidirectional control-frame probe', async () => {
    assert.equal((await websocketPingCheck(base, 1000)).status, 'ok');
    const url = requests.at(-1)!;
    assert.equal(url.searchParams.get('network'), 'ukmesh');
    assert.equal(url.searchParams.get('initial_state'), '0');
    assert.equal(url.searchParams.get(INITIAL_STATE_FORMAT_PARAMETER), null);
  });
  await t.test('full probe requests and decodes the complete global compact format', async () => {
    assert.equal((await websocketInitialStateCheck(base, 1000)).status, 'ok');
    const url = requests.at(-1)!;
    assert.equal(url.searchParams.get('network'), 'ukmesh');
    assert.equal(url.searchParams.get('observer'), null);
    assert.equal(url.searchParams.get(INITIAL_STATE_FORMAT_PARAMETER), INITIAL_STATE_COLUMNS_FORMAT);
  });
  await t.test('a declared malformed compact frame fails despite its initial_state type', async () => {
    reply = { type: 'initial_state', format: INITIAL_STATE_COLUMNS_FORMAT, data: {}, ts: 1 };
    assert.equal((await websocketInitialStateCheck(base, 1000)).status, 'failed');
  });
  await t.test('close before bootstrap is a failure', async () => {
    mode = 'close'; assert.equal((await websocketInitialStateCheck(base, 1000)).status, 'failed');
  });
  await t.test('a connected but stalled bootstrap meets the original timeout failure path', async () => {
    mode = 'wait';
    const result = await websocketInitialStateCheck(base, 100);
    assert.equal(result.status, 'failed'); assert.match(result.detail, /no initial_state within 100ms/);
  });
});
