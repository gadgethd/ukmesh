import assert from 'node:assert/strict';
import test from 'node:test';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import type { AddressInfo } from 'node:net';
import { buildAlertForwardPayload, summarizeAlertPayload } from './alert-receiver.js';

test('summarizes Alertmanager notifications without persisting annotations or labels', () => {
  const receipt = summarizeAlertPayload({
    receiver: 'operations-receiver',
    alerts: [
      {
        status: 'firing',
        labels: { alertname: 'BackendDown', secret: 'must-not-be-stored' },
        annotations: { description: 'potentially sensitive detail' },
      },
      {
        status: 'resolved',
        labels: { alertname: 'QueueCapacityHigh' },
      },
    ],
  }, new Date('2026-07-29T12:00:00.000Z'));

  assert.deepEqual(receipt, {
    received_at: '2026-07-29T12:00:00.000Z',
    source: 'alertmanager',
    status: 'firing',
    alert_names: ['BackendDown', 'QueueCapacityHigh'],
    firing: 1,
    resolved: 1,
  });
  assert.equal(JSON.stringify(receipt).includes('must-not-be-stored'), false);
});

async function waitFor<T>(read: () => Promise<T>, accept: (value: T) => boolean): Promise<T> {
  const end = Date.now() + 5_000;
  do {
    const value = await read();
    if (accept(value)) return value;
    await new Promise((resolve) => setTimeout(resolve, 20));
  } while (Date.now() < end);
  throw new Error('test condition did not arrive within five seconds');
}

const ALERT_TOKEN = 'fixture-alert-receiver-token';

async function receiver(forwardUrl: string, token = ALERT_TOKEN) {
  const dir = await mkdtemp(path.join(tmpdir(), 'auditwave-alert-test-'));
  const reservation = http.createServer();
  reservation.listen(0, '127.0.0.1');
  await once(reservation, 'listening');
  const port = (reservation.address() as AddressInfo).port;
  await new Promise<void>((resolve) => reservation.close(() => resolve()));
  const receipt = path.join(dir, 'alerts.jsonl');
  // Only a local throwaway process receives this environment. Never load the
  // real .env or use the live receiver for notification tests.
  const child = spawn(process.execPath, ['--import', 'tsx', 'src/workers/alert-receiver.ts'], {
    env: { PATH: process.env['PATH'], ALERT_RECEIVER_PORT: String(port), ALERT_RECEIVER_PATH: receipt,
      ALERT_RECEIVER_TOKEN: token, ALERT_FORWARD_URL: forwardUrl, ALERT_FORWARD_MAX_ATTEMPTS: '2',
      ALERT_FORWARD_BACKOFF_BASE_MS: '1', ALERT_FORWARD_BACKOFF_CAP_MS: '1' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stdout.on('data', (data) => { output += String(data); });
  child.stderr.on('data', (data) => { output += String(data); });
  const exited = once(child, 'exit');
  const close = async () => {
    if (child.exitCode === null) child.kill('SIGTERM');
    await exited;
    await rm(dir, { recursive: true, force: true });
  };
  try {
    await waitFor(async () => output, (value) => value.includes('listening on internal port'));
  } catch (error) { await close(); throw error; }
  return { base: `http://127.0.0.1:${port}`, receipt, logs: () => output, close };
}

test('unconfigured or invalid forwarding logs a startup warning and readiness is 503 while liveness is 200', { timeout: 15_000 }, async () => {
  for (const url of ['', 'file:///invalid']) {
    const child = await receiver(url);
    try {
      assert.match(child.logs(), /DEGRADED: ALERT_FORWARD_URL is unset or invalid; forwardConfigured=false/);
      assert.equal((await fetch(`${child.base}/healthz`)).status, 200);
      const ready = await fetch(`${child.base}/readyz`);
      assert.equal(ready.status, 503);
      assert.match((await ready.json() as { detail: string }).detail, /archive-only/);
      assert.equal((await fetch(`${child.base}/alerts`, { method: 'POST', headers: { Authorization: `Bearer ${ALERT_TOKEN}` }, body: JSON.stringify({
        service: 'meshcore-analytics', check: 'readiness', kind: 'alert',
      }) })).status, 202);
      assert.match(await readFile(child.receipt, 'utf8'), /"source":"synthetic"/);
      assert.equal((await fetch(`${child.base}/readyz`)).status, 503);
      assert.doesNotMatch(child.logs(), /forward succeeded/);
    } finally { await child.close(); }
  }
});

test('Alertmanager and synthetic forwarding both require a successful stub delivery before readiness becomes healthy', { timeout: 10_000 }, async () => {
  const received: Array<{ content: string; allowed_mentions: { parse: string[] } }> = [];
  const stub = http.createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(Buffer.from(chunk));
    received.push(JSON.parse(Buffer.concat(chunks).toString('utf8')));
    res.writeHead(204).end();
  });
  stub.listen(0, '127.0.0.1');
  await once(stub, 'listening');
  const forwardUrl = `http://127.0.0.1:${(stub.address() as AddressInfo).port}/secret-stub-token`;
  const child = await receiver(forwardUrl);
  try {
    assert.match(child.logs(), /forwardConfigured=true/);
    assert.doesNotMatch(child.logs(), /DEGRADED/);
    assert.equal((await fetch(`${child.base}/readyz`)).status, 503);
    for (const payload of [
      { alerts: [{ status: 'firing', labels: { alertname: 'BackendDown', secret: 'hidden' } }] },
      { service: 'meshcore-analytics', kind: 'recovery', check: 'dependency_readiness', detail: 'hidden' },
    ]) {
      assert.equal((await fetch(`${child.base}/alerts`, { method: 'POST', headers: { Authorization: `Bearer ${ALERT_TOKEN}` }, body: JSON.stringify(payload) })).status, 202);
    }
    await waitFor(async () => received.length, (count) => count === 2);
    await waitFor(async () => (await fetch(`${child.base}/readyz`)).status, (status) => status === 200);
    const health = await (await fetch(`${child.base}/readyz`)).text();
    assert.doesNotMatch(health + child.logs(), /secret-stub-token/);
    assert.match(child.logs(), /forward succeeded: HTTP 204/);
    assert.match(received[0]!.content, /Source: alertmanager/);
    assert.match(received[1]!.content, /Source: synthetic/);
    for (const payload of received) {
      assert.deepEqual(payload.allowed_mentions, { parse: [] });
      assert.doesNotMatch(JSON.stringify(payload), /hidden/);
    }
  } finally {
    await child.close();
    stub.closeAllConnections();
    await new Promise<void>((resolve) => stub.close(() => resolve()));
  }
});

test('failed stub deliveries remain unready and reach bounded retries', { timeout: 10_000 }, async () => {
  let calls = 0;
  const stub = http.createServer((_req, res) => { calls += 1; res.writeHead(503).end(); });
  stub.listen(0, '127.0.0.1');
  await once(stub, 'listening');
  const child = await receiver(`http://127.0.0.1:${(stub.address() as AddressInfo).port}/failed`);
  try {
    await fetch(`${child.base}/alerts`, { method: 'POST', headers: { Authorization: `Bearer ${ALERT_TOKEN}` }, body: JSON.stringify({
      service: 'meshcore-analytics', kind: 'alert', check: 'test-only',
    }) });
    await waitFor(async () => (await readdir(`${child.receipt}.queue`)).filter((name) => name.endsWith('.dead.json')), (files) => files.length === 1);
    const dead = (await readdir(`${child.receipt}.queue`)).find((name) => name.endsWith('.dead.json'))!;
    assert.equal(JSON.parse(await readFile(path.join(`${child.receipt}.queue`, dead), 'utf8')).attempts, 2);
    assert.equal(calls, 2);
    assert.equal((await fetch(`${child.base}/readyz`)).status, 503);
  } finally {
    await child.close();
    stub.closeAllConnections();
    await new Promise<void>((resolve) => stub.close(() => resolve()));
  }
});

test('Compose checks receiver delivery readiness', async () => {
  const compose = await readFile('../docker-compose.yml', 'utf8');
  const service = compose.split('\n  alert-receiver:')[1]!.split('\n  alertmanager:')[0]!;
  assert.match(service, /wget -qO- http:\/\/127.0.0.1:8080\/readyz/);
  assert.doesNotMatch(service, /\/healthz/);
});

test('summarizes synthetic alert and recovery events', () => {
  const alert = summarizeAlertPayload({
    kind: 'alert',
    service: 'meshcore-analytics',
    check: 'dependency_readiness',
  });
  const recovery = summarizeAlertPayload({
    kind: 'recovery',
    service: 'meshcore-analytics',
    check: 'dependency_readiness',
  });

  assert.equal(alert.source, 'synthetic');
  assert.equal(alert.status, 'firing');
  assert.equal(alert.firing, 1);
  assert.equal(recovery.status, 'recovery');
  assert.equal(recovery.resolved, 1);
});

test('builds a bounded Discord-compatible payload without mentions or alert details', () => {
  const receipt = summarizeAlertPayload({
    receiver: 'operations-receiver',
    alerts: [{
      status: 'firing',
      labels: { alertname: '@everyone BackendDown', secret: 'must-not-be-forwarded' },
      annotations: { description: 'potentially sensitive detail' },
    }],
  }, new Date('2026-07-29T12:00:00.000Z'));

  const payload = buildAlertForwardPayload(receipt);

  assert.match(payload.content, /UKMesh alert firing/);
  assert.match(payload.content, /@everyone BackendDown/);
  assert.match(payload.content, /Firing: 1 · Resolved: 0/);
  assert.ok(payload.content.length <= 2_000);
  assert.deepEqual(payload.allowed_mentions, { parse: [] });
  assert.equal(JSON.stringify(payload).includes('must-not-be-forwarded'), false);
  assert.equal(JSON.stringify(payload).includes('potentially sensitive detail'), false);
});

test('bounds Discord content when an Alertmanager notification has many long names', () => {
  const receipt = summarizeAlertPayload({
    alerts: Array.from({ length: 100 }, (_, index) => ({
      status: 'firing',
      labels: { alertname: `${index}-${'x'.repeat(200)}` },
    })),
  });

  assert.equal(buildAlertForwardPayload(receipt).content.length, 2_000);
});


test('alert ingress rejects missing or incorrect bearer credentials before archiving', async () => {
  const child = await receiver('');
  try {
    for (const headers of [{}, { Authorization: 'Bearer incorrect' }]) {
      const response = await fetch(`${child.base}/alerts`, {
        method: 'POST', headers, body: JSON.stringify({ kind: 'alert', check: 'forged' }),
      });
      assert.equal(response.status, 401);
    }
    await assert.rejects(readFile(child.receipt), { code: 'ENOENT' });
  } finally { await child.close(); }
});

test('alert ingress fails closed when the receiver token is unconfigured', async () => {
  const child = await receiver('', '');
  try {
    const response = await fetch(`${child.base}/alerts`, {
      method: 'POST', headers: { Authorization: `Bearer ${ALERT_TOKEN}` },
      body: JSON.stringify({ kind: 'alert', check: 'forged' }),
    });
    assert.equal(response.status, 503);
    await assert.rejects(readFile(child.receipt), { code: 'ENOENT' });
  } finally { await child.close(); }
});
