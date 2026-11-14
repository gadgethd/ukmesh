import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import { readOperation, validateOperationResponse as validate } from './operationResponse.js';
import { LOOKUP_RESPONSE_MAX_BYTES } from './lookupResponseBody.js';

const id = 'A'.repeat(64);
const geo = (generatedAt: string, ids = [id]) => Buffer.from(JSON.stringify({ type: 'FeatureCollection', generatedAt,
  features: ids.map(node_id => ({ type: 'Feature', geometry: { type: 'Point', coordinates: [-1, 52] }, properties: { node_id } })) }));

test('export evidence retains bytes and data, normalizes only response time and rejects duplicates', () => {
  const first = validate(geo('first'), 'geojson', 200, 'application/geo+json');
  const second = validate(geo('second'), 'geojson', 200, 'application/geo+json');
  assert.notEqual(first.digest, second.digest); assert.equal(first.dataDigest, second.dataDigest);
  const csv = validate(Buffer.from(`node_id,name\n${id},"Relay, with\nnewline"\n`), 'csv', 200, 'text/csv');
  assert.equal(csv.identityDigest, first.identityDigest); assert.equal(csv.entries, 1);
  assert.throws(() => validate(geo('now', [id, id.toLowerCase()]), 'geojson', 200, 'application/geo+json'), /duplicate/);
  assert.throws(() => validate(Buffer.from(`node_id,name\n${id}\n`), 'csv', 200, 'text/csv'), /CSV/);
  const changed = geo('first', ['B'.repeat(64)]);
  assert.notEqual(validate(changed, 'geojson', 200, 'application/geo+json').dataDigest, first.dataDigest);
});

test('successful HTTP status alone cannot satisfy populated statistics and bounded export contracts', () => {
  for (const [body, format, status, type] of [
    ['{}', 'summary', 200, 'application/json'], ['{}', 'charts', 200, 'application/json'],
    ['{"totalNodes":1,"mapNodes":1,"packetsDay":1}', 'summary', 503, 'application/json'],
    ['{"totalNodes":1,"mapNodes":1,"packetsDay":1}', 'summary', 200, 'text/plain'],
    ['bad JSON', 'charts', 200, 'application/json'],
  ] as const) assert.throws(() => validate(Buffer.from(body), format, status, type));
  assert.throws(() => validate(Buffer.alloc(LOOKUP_RESPONSE_MAX_BYTES + 1), 'csv', 200, 'text/csv'), /budget/);
  assert.equal(validate(Buffer.from('{"totalNodes":2,"mapNodes":1,"packetsDay":3}'), 'summary', 200, 'application/json').entries, null);
});

test('chart evidence keeps exact persisted timestamp and checks freshness and privacy generation', () => {
  const snapshot = { generatedAt: new Date(Date.now() - 1000).toISOString(), visibilityGeneration: 7, maxAgeMs: 30000 };
  const body = (metadata = snapshot) => Buffer.from(JSON.stringify({ snapshot: { status: 'complete', scope: 'ukmesh',
    generatedAt: metadata.generatedAt, visibilityGeneration: metadata.visibilityGeneration }, summary: {}, packetsPerHour: [], packetTypes: [] }));
  const first = validate(body(), 'charts', 200, 'application/json', snapshot);
  const next = { ...snapshot, generatedAt: new Date(Date.now() - 500).toISOString() };
  assert.notEqual(first.dataDigest, validate(body(next), 'charts', 200, 'application/json', next).dataDigest);
  assert.equal(first.snapshot?.['generatedAt'], snapshot.generatedAt);
  assert.ok(Number(first.snapshot?.['ageMs']) >= 1000);
  assert.throws(() => validate(body(), 'charts', 200, 'application/json', { ...snapshot, visibilityGeneration: 8 }), /generation/);
  const old = { ...snapshot, generatedAt: new Date(Date.now() - 60000).toISOString() };
  assert.throws(() => validate(body(old), 'charts', 200, 'application/json', old), /freshness/);
});

test('real operation transport bounds stalled/oversize bodies, releases sockets and recovers', async t => {
  let mode = 'stall';
  const server = http.createServer((_req, response) => {
    response.setHeader('Content-Type', 'application/json');
    if (mode === 'stall') { response.write('{'); return; }
    if (mode === 'oversize') { response.end(Buffer.alloc(LOOKUP_RESPONSE_MAX_BYTES + 1)); return; }
    response.end('{"totalNodes":2,"mapNodes":1,"packetsDay":3}');
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const port = (server.address() as AddressInfo).port;
  const agent = new http.Agent({ keepAlive: true, maxSockets: 1 });
  t.after(async () => { agent.destroy(); server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())); });
  await assert.rejects(readOperation(port, '/api/stats?network=ukmesh', agent, 20), /deadline/);
  mode = 'oversize';
  await assert.rejects(readOperation(port, '/api/stats?network=ukmesh', agent), /budget/);
  mode = 'valid';
  const result = await readOperation(port, '/api/stats?network=ukmesh', agent);
  assert.equal(result.status, 200); assert.ok(result.ms >= 0);
  assert.equal(validate(result.body, 'summary', result.status, result.type).entries, null);
});

test('eight real actors remain serial, reuse their own sockets and retain body assembly in HTTP timing', async t => {
  const body = '{"totalNodes":2,"mapNodes":1,"packetsDay":3}';
  const observed = new Set<string>(); let active = 0, peak = 0;
  const server = http.createServer((request, response) => {
    observed.add(request.socket.remoteAddress!); peak = Math.max(peak, ++active);
    setTimeout(() => { active--; response.setHeader('Content-Type', 'application/json'); response.end(body); }, 2);
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const port = (server.address() as AddressInfo).port;
  const actors = Array.from({ length: 8 }, (_, index) => `127.0.8.${index + 1}`);
  const agents = actors.map(localAddress => new http.Agent({ keepAlive: true, maxSockets: 1, localAddress }));
  t.after(async () => { for (const agent of agents) agent.destroy(); server.closeAllConnections();
    await new Promise<void>(resolve => server.close(() => resolve())); });
  for (let request = 0; request < 16; request++) {
    const result = await readOperation(port, '/api/stats?network=ukmesh', agents[request % 8]);
    assert.equal(result.transport.localAddress, actors[request % 8]);
    assert.equal(result.transport.endMs, result.ms);
    if (request >= 8) assert.equal(result.transport.reusedSocket, true);
  }
  assert.equal(peak, 1); assert.deepEqual([...observed].sort(), actors);
  const concatenate = Buffer.concat; let assembledAt = 0;
  t.mock.method(Buffer, 'concat', (parts, ...args) => {
    const result = concatenate(parts, ...args);
    if (result.toString() === body) {
      const until = performance.now() + 20;
      while (performance.now() < until) { /* deterministic bounded body-assembly work */ }
      assembledAt = performance.now();
    }
    return result;
  });
  const started = performance.now();
  const result = await readOperation(port, '/api/stats?network=ukmesh', agents[0]);
  assert.ok(assembledAt > started);
  assert.ok(result.ms >= assembledAt - started - 1, 'HTTP timing must include the original body assembly boundary');
});
