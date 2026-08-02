import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express from 'express';
import { createBackendSiteRoutes } from './routes.js';

test('operator site exchanges the token for a CSRF-protected bounded browser session', async (t) => {
  const previousToken = process.env['OPERATOR_SITE_TOKEN'];
  process.env['OPERATOR_SITE_TOKEN'] = 'correct-horse-battery-staple-and-operator';
  t.after(() => {
    if (previousToken === undefined) delete process.env['OPERATOR_SITE_TOKEN'];
    else process.env['OPERATOR_SITE_TOKEN'] = previousToken;
  });

  let queryCount = 0;
  const app = express();
  app.use(express.json({ limit: '8kb' }));
  app.use(createBackendSiteRoutes({
    query: (async () => {
      queryCount += 1;
      return { rows: [] };
    }) as Parameters<typeof createBackendSiteRoutes>[0]['query'],
    getHealthOverview: async () => ({
      database: { size_bytes: 123 },
      system: { disk: { volumes: { database: { used_bytes: 100 } } } },
    }),
  }));
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => server.close());
  const { port } = server.address() as AddressInfo;
  const base = `http://127.0.0.1:${port}`;

  const loginPage = await fetch(`${base}/`);
  assert.equal(loginPage.status, 200);
  assert.match(await loginPage.text(), /Operator login/);
  assert.match(loginPage.headers.get('content-security-policy') ?? '', /nonce-/);

  const redirect = await fetch(`${base}/backend`, { redirect: 'manual' });
  assert.equal(redirect.status, 302);
  assert.equal(redirect.headers.get('location'), '/');

  const invalid = await fetch(`${base}/local-api/operator/login`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ token: 'wrong' }),
  });
  assert.equal(invalid.status, 403);

  const login = await fetch(`${base}/local-api/operator/login`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ token: process.env['OPERATOR_SITE_TOKEN'] }),
  });
  assert.equal(login.status, 200);
  const setCookie = login.headers.get('set-cookie') ?? '';
  assert.match(setCookie, /meshcore_operator_session=/);
  assert.match(setCookie, /HttpOnly/i);
  assert.match(setCookie, /Secure/i);
  assert.match(setCookie, /SameSite=Strict/i);
  const cookie = setCookie.split(';', 1)[0] ?? '';

  const sessionResponse = await fetch(`${base}/local-api/operator/session`, {
    headers: { cookie },
  });
  assert.equal(sessionResponse.status, 200);
  const session = await sessionResponse.json() as {
    mode: string;
    csrfToken: string;
    expiresAt: number;
  };
  assert.equal(session.mode, 'session');
  assert.ok(session.csrfToken);
  assert.ok(session.expiresAt > Date.now());

  const dashboard = await fetch(`${base}/local-api/ml-path-learner`, {
    headers: { cookie },
  });
  assert.equal(dashboard.status, 200);
  assert.ok(queryCount > 0);
  const cachedAt = queryCount;
  assert.equal((await fetch(`${base}/local-api/ml-path-learner`, {
    headers: { cookie },
  })).status, 200);
  assert.equal(queryCount, cachedAt);

  const rejectedLogout = await fetch(`${base}/local-api/operator/logout`, {
    method: 'POST',
    headers: { cookie, 'x-csrf-token': 'wrong' },
  });
  assert.equal(rejectedLogout.status, 403);

  const logout = await fetch(`${base}/local-api/operator/logout`, {
    method: 'POST',
    headers: { cookie, 'x-csrf-token': session.csrfToken },
  });
  assert.equal(logout.status, 200);
  assert.match(
    logout.headers.get('set-cookie') ?? '',
    /Expires=Thu, 01 Jan 1970 00:00:00 GMT/,
  );

  assert.equal((await fetch(`${base}/local-api/operator/session`, {
    headers: { cookie },
  })).status, 401);

  const automation = await fetch(`${base}/local-api/operator/session`, {
    headers: { authorization: `Bearer ${process.env['OPERATOR_SITE_TOKEN']}` },
  });
  assert.equal(automation.status, 200);
  assert.deepEqual(await automation.json(), {
    mode: 'automation',
    expiresAt: null,
    csrfToken: null,
  });

  const operatorHealth = await fetch(`${base}/local-api/health`, {
    headers: { authorization: `Bearer ${process.env['OPERATOR_SITE_TOKEN']}` },
  });
  assert.equal(operatorHealth.status, 200);
  assert.deepEqual(await operatorHealth.json(), {
    database: { size_bytes: 123 },
    system: { disk: { volumes: { database: { used_bytes: 100 } } } },
  });
});
