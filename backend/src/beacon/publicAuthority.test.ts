import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import express from 'express';
import { createBeaconPublicAuthority } from './publicAuthority.js';

test('Beacon authority serves only a credentialed ready UKMesh generation and approved keys', async t => {
  const app = express();
  const token = 'beacon-fixture-credential'.repeat(2);
  const streamToken = 'stream-fixture-credential'.repeat(2);
  const allowedKey = 'ab'.repeat(16);
  const privateKey = 'cd'.repeat(16);
  let generation = 4;
  let reads = 0;
  app.use('/closed', createBeaconPublicAuthority({ token: streamToken, streamToken }));
  app.use('/beacon', createBeaconPublicAuthority({ token, streamToken,
    secrets: `fixture:${allowedKey},private:${privateKey}`, allowlist: 'fixture',
    readGeneration: async () => { reads++; return generation; },
  }));
  const server = app.listen(0, '127.0.0.1');
  t.after(() => { server.close(); });
  await new Promise<void>((resolve, reject) => {
    server.once('listening', resolve);
    server.once('error', reject);
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const url = `http://127.0.0.1:${address.port}`;
  const request = (path: string, bearer: string, method = 'GET') =>
    fetch(url + path, { method, headers: { authorization: `Bearer ${bearer}` } });

  assert.equal((await request('/closed/authority', streamToken)).status, 404);
  assert.equal((await request('/beacon/authority', streamToken)).status, 401);
  assert.equal((await request('/beacon/authority', token, 'POST')).status, 404);
  assert.equal(reads, 0);
  const ready = await request('/beacon/authority', token);
  assert.equal(ready.status, 200);
  assert.equal(ready.headers.get('cache-control'), 'no-store');
  const state = await ready.json();
  assert.equal(state.version, 1);
  assert.equal(state.network, 'ukmesh');
  assert.equal(state.generation, 4);
  assert.match(state.registryVersion, /^[a-f0-9]{64}$/);
  assert.deepEqual(state.channels.filter((entry: { name: string }) => entry.name === 'fixture'), [{
    id: createHash('sha256').update('fixture').digest('hex'), name: 'fixture',
    keyFingerprint: createHash('sha256').update(Buffer.from(allowedKey, 'hex')).digest('hex'),
    secretHex: allowedKey,
  }]);
  assert.ok(!JSON.stringify(state).includes(privateKey));

  generation = 0;
  assert.equal((await request('/beacon/authority', token)).status, 503);
  generation = 5;
  assert.equal((await (await request('/beacon/authority', token)).json()).generation, 5);
  assert.equal(reads, 3);
});
