import assert from 'node:assert/strict';
import http from 'node:http';
import test from 'node:test';
import express from 'express';
import { createPublicRamSource } from './publicSource.js';
import { PrivatePrefixCache } from '../privacy/privatePrefixCache.js';
import { buildPacketBatchInput } from '../db/packetInput.js';

test('F01/F16 credentialed public source filters current policy, pages raw cursors and fails closed', async () => {
  process.env['MQTT_INGEST_MODE'] = 'ram';
  const policy = new PrivatePrefixCache();
  const privateId = 'AA'.repeat(32);
  policy.replace(2, [{ node_id: privateId, network: 'teesside', prefix_size_bytes: 1, prefix: 'AA' }]);
  let unavailable = false;
  const input = buildPacketBatchInput({ topic: 'meshcore/ZZZ/observer/packets', packetHash: 'allowed', rawHex: '00', network: 'northeast' });
  const entries = [{ id: '10-0', entry: { observationId: '11111111-1111-4111-8111-111111111111', admittedAtMs: 10,
    isPrivate: false, visibilityOk: true, input: { ...input, packetHash: 'private', rxNodeId: privateId } } },
  { id: '11-0', entry: { observationId: '22222222-2222-4222-8222-222222222222', admittedAtMs: 11,
    isPrivate: true, visibilityOk: false, input } }];
  const app = express();
  const token = 'fixture'.repeat(8);
  app.use('/public', createPublicRamSource({ token: () => token, read: async () => entries,
    loadPolicy: async () => unavailable ? null : policy }));
  const server = http.createServer(app);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address(); assert.ok(address && typeof address !== 'string');
  const root = `http://127.0.0.1:${address.port}/public`;
  const get = (path: string, authorized = true) => fetch(root + path, { headers: authorized ? { authorization: `Bearer ${token}` } : {} });
  try {
    assert.equal((await get('/entries', false)).status, 401);
    assert.deepEqual(await (await get('/entries?limit=1')).json(), { generation: 2, entries: [], nextCursor: '10-0' });
    const page = await (await get('/entries?after=10-0')).json();
    assert.equal(page.entries.length, 1); assert.equal(page.nextCursor, '11-0');
    assert.equal(JSON.parse(page.entries[0].value).vo, 1);
    assert.equal(JSON.parse(page.entries[0].value).d.packetHash, 'allowed');
    assert.equal((await get('/entries?limit=501')).status, 400);
    unavailable = true;
    assert.equal((await get('/policy')).status, 503);
    assert.equal((await get('/entries')).status, 503);
  } finally { await new Promise<void>((resolve) => server.close(() => resolve())); }
});
