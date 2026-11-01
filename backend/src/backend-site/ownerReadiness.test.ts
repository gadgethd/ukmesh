import assert from 'node:assert/strict';
import test from 'node:test';
import type { Request, Response, NextFunction } from 'express';
import { createBackendSiteRoutes } from './routes.js';

test('owner diagnostics require both local access and operator authentication without opening a server', async (t) => {
  const previousToken = process.env['OPERATOR_SITE_TOKEN'];
  const token = 'synthetic-operator-token-for-readiness-test';
  process.env['OPERATOR_SITE_TOKEN'] = token;
  t.after(() => {
    if (previousToken === undefined) delete process.env['OPERATOR_SITE_TOKEN'];
    else process.env['OPERATOR_SITE_TOKEN'] = previousToken;
  });
  let reads = 0;
  const diagnostics = { format: 'meshcore-owner-readiness-v1', ownerAuthorization: { fixture: true } };
  const router = createBackendSiteRoutes({
    query: async () => ({ rows: [] }),
    getOwnerAuthorizationReadiness: async () => { reads++; return diagnostics; },
  });
  const layer = router.stack.find((entry) => entry.route?.path === '/local-api/owner-authorization');
  assert.ok(layer);
  const handler = layer.route!.stack[0].handle;
  async function invoke(peer: string, headers: Record<string, string>) {
    let status = 200;
    let body: unknown;
    const responseHeaders: Record<string, string> = {};
    const res = {
      locals: {},
      status(value: number) { status = value; return this; },
      type() { return this; },
      send(value: unknown) { body = value; return this; },
      json(value: unknown) { body = value; return this; },
      setHeader(name: string, value: string) { responseHeaders[name] = value; },
    };
    await handler({ socket: { remoteAddress: peer }, headers } as unknown as Request,
      res as unknown as Response, (() => { throw new Error('unexpected next'); }) as NextFunction);
    return { status, body, responseHeaders };
  }
  assert.equal((await invoke('203.0.113.5', { authorization: `Bearer ${token}` })).status, 403);
  assert.equal((await invoke('127.0.0.1', {})).status, 401);
  assert.equal(reads, 0);
  const accepted = await invoke('127.0.0.1', { authorization: `Bearer ${token}` });
  assert.equal(accepted.status, 200);
  assert.deepEqual(accepted.body, diagnostics);
  assert.equal(accepted.responseHeaders['Cache-Control'], 'no-store');
  assert.equal(reads, 1);
});
