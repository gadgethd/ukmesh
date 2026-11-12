import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express from 'express';
import { publicJson, PUBLIC_REVALIDATE } from './publicJson.js';

test('conditional public reads fence cached bodies, each scope and in-flight changes', async (t) => {
  const app = express();
  let generation = 1;
  let unavailable = false;
  let changeDuringLoad = false;
  let failFinalRead = false;
  let reads = 0;
  let loads = 0;
  app.get('/', async (req, res) => {
    reads = 0;
    try {
      await publicJson(res, async () => {
        reads++;
        if (unavailable || (failFinalRead && reads === 2)) throw new Error('unavailable');
        return generation;
      }, async () => {
        loads++;
        if (changeDuringLoad) generation++;
        return { network: req.query.network, observer: req.query.observer,
          region: req.query.region, sections: req.query.sections, redacted: generation > 1 };
      });
    } catch {
      res.status(503).json({ error: 'Unavailable' });
    }
  });
  const server = app.listen(0, '127.0.0.1');
  t.after(() => { server.closeAllConnections(); server.close(); });
  await once(server, 'listening');
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  const first = await fetch(`${base}/?network=ukmesh`);
  assert.equal(first.status, 200);
  assert.equal(first.headers.get('cache-control'), PUBLIC_REVALIDATE);
  const etag = first.headers.get('etag')!;
  assert.ok(etag, 'use the existing Express validator');
  // Undici adds request Cache-Control: no-cache for conditional default-cache
  // requests; force-cache lets the test exercise native HTTP validation.
  const conditional = () => ({ cache: 'force-cache' as const, headers: { 'If-None-Match': etag } });
  const unchanged = await fetch(`${base}/?network=ukmesh`, conditional());
  assert.equal(unchanged.status, 304);
  assert.equal(reads, 2);
  assert.equal(loads, 2, '304 still validates current origin authority and payload');
  for (const query of ['network=test', 'network=ukmesh&observer=fixture',
    'network=ukmesh&region=fixture', 'network=ukmesh&sections=overview']) {
    assert.equal((await fetch(`${base}/?${query}`, conditional())).status, 200);
  }
  generation++;
  const redacted = await fetch(`${base}/?network=ukmesh`, conditional());
  assert.equal(redacted.status, 200);
  assert.equal((await redacted.json()).redacted, true);
  for (const mode of ['initial failure', 'final failure', 'in-flight change']) {
    unavailable = mode === 'initial failure';
    failFinalRead = mode === 'final failure';
    changeDuringLoad = mode === 'in-flight change';
    const result = await fetch(`${base}/?network=ukmesh`, conditional());
    assert.equal(result.status, 503, mode);
    assert.equal(result.headers.get('cache-control'), 'no-store', mode);
    assert.deepEqual(await result.json(), { error: 'Unavailable' });
  }
});
