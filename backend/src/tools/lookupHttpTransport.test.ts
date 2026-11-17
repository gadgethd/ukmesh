import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import type { Socket } from 'node:net';
import test from 'node:test';
import { createLookupAgents, runLookupSamples, setLookupRequestDeadline } from './lookupHttpTransport.js';

const turn = () => new Promise<void>(resolve => setImmediate(resolve));

test('slow active responses retain30s while free sockets honor the server hint and reconnect without retry', { timeout: 10_000 }, async () => {
  const agents = createLookupAgents(), finite = agents[0]!;
  const zero = new http.Agent({ keepAlive: true, maxSockets: 1, localAddress: '127.0.1.1' });
  const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' }); res.flushHeaders();
    if (req.url === '/slow') setTimeout(() => res.end('{"ok":true}'), 1200);
    else res.end('{"ok":true}');
  });
  server.keepAliveTimeout = 2000; server.keepAliveTimeoutBuffer = 0;
  let dispatched = 0;
  async function request(agent: http.Agent, route: string) {
    dispatched++;
    return new Promise<{ socket: Socket; activeTimeout: number | undefined; reused: boolean; ms: number }>((resolve, reject) => {
      const started = performance.now();
      const req = http.get({ hostname: '127.0.0.1', port: (server.address() as AddressInfo).port, path: route, agent }, res => {
        const socket = req.socket!, activeTimeout = socket.timeout;
        res.resume(); res.on('error', reject); res.on('end', () => resolve({ socket, activeTimeout,
          reused: req.reusedSocket, ms: performance.now() - started }));
      });
      setLookupRequestDeadline(req); req.on('error', reject);
    });
  }
  try {
    assert.equal(agents.length, 128);
    assert.deepEqual(agents.map(agent => agent.options.localAddress), Array.from({ length: 128 }, (_, i) => `127.0.1.${i + 1}`));
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    const [control, positive] = await Promise.all([request(zero, '/slow'), request(finite, '/slow')]);
    assert.equal(control.activeTimeout, 30_000); assert.equal(positive.activeTimeout, 30_000);
    assert.ok(positive.ms >= 1100, 'active response must survive the shorter eventual idle lease');
    await turn(); assert.equal(control.socket.timeout, 0); assert.equal(positive.socket.timeout, 1000);
    const deadline = setTimeout(() => positive.socket.destroy(new Error('idle eviction did not occur')), 1600);
    try { await once(positive.socket, 'close'); } finally { clearTimeout(deadline); }
    assert.equal(control.socket.destroyed, false);
    const [old, fresh] = await Promise.all([request(zero, '/fast'), request(finite, '/fast')]);
    assert.equal(old.reused, true); assert.equal(fresh.reused, false);
    assert.equal(dispatched, 4, 'socket replacement must not retry an application request');
  } finally {
    zero.destroy(); agents.forEach(agent => agent.destroy()); server.closeAllConnections();
    await new Promise<void>(resolve => server.close(() => resolve()));
  }
});

test('successful batches preserve every sample exactly once and the active slot bound', async () => {
  for (const concurrency of [1, 8] as const) {
    let active = 0, maximum = 0; const seen: number[] = [];
    const results = await runLookupSamples(200, concurrency, async index => {
      active++; maximum = Math.max(maximum, active); seen.push(index);
      try { await turn(); return index; } finally { active--; }
    });
    assert.equal(results.length, 200); assert.equal(new Set(seen).size, 200);
    assert.deepEqual([...results].sort((a,b) => a-b), Array.from({ length: 200 }, (_, i) => i));
    assert.equal(maximum, concurrency); assert.equal(active, 0);
  }
});

test('first failure stops new admission and cleanup waits for all active work including a later rejection', async t => {
  const first = new Error('primary socket reset'), later = new Error('later failure');
  const slots: { resolve: (value: number) => void; reject: (error: Error) => void }[] = [];
  const unhandled: unknown[] = []; const onUnhandled = (error: unknown) => unhandled.push(error);
  process.on('unhandledRejection', onUnhandled); t.after(() => process.removeListener('unhandledRejection', onUnhandled));
  let cleanup = false, observed: unknown;
  const batch = (async () => {
    try { await runLookupSamples(200, 8, () => new Promise<number>((resolve, reject) => slots.push({ resolve, reject }))); }
    finally { cleanup = true; }
  })().catch(error => { observed = error; });
  assert.equal(slots.length, 8); slots[2]!.reject(first); await turn();
  assert.equal(cleanup, false); assert.equal(slots.length, 8);
  slots[5]!.reject(later); await turn(); assert.equal(cleanup, false);
  slots.forEach((slot, i) => { if (i !== 2 && i !== 5) slot.resolve(i); });
  await batch; await turn();
  assert.equal(observed, first); assert.equal(cleanup, true); assert.equal(slots.length, 8);
  assert.deepEqual(unhandled, []);
});
