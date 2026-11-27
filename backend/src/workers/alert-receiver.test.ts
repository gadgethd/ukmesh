import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdir, mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
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

const ALERT_TOKEN = 'owned-audit-test-token';

async function listenStub(t: TestContext, stub: http.Server): Promise<number> {
  stub.listen(0, '127.0.0.1');
  await once(stub, 'listening');
  t.after(() => {
    stub.closeAllConnections();
    return new Promise<void>((resolve) => stub.close(() => resolve()));
  });
  return (stub.address() as AddressInfo).port;
}

async function receiver(forwardUrl: string, options: { directory?: string; keepFiles?: boolean; queueMax?: number; token?: string } = {}) {
  const dir = options.directory ?? await mkdtemp(path.join(tmpdir(), 'auditwave-alert-test-'));
  const receipt = path.join(dir, 'alerts.jsonl');
  // Only a local throwaway process receives this environment. Never load the
  // real .env or use the live receiver for notification tests.
  const child = spawn(process.execPath, ['--import', 'tsx', 'src/workers/alert-receiver.ts'], {
    env: { PATH: process.env['PATH'], ALERT_RECEIVER_PORT: '0', ALERT_RECEIVER_PATH: receipt,
      ALERT_RECEIVER_TOKEN: options.token ?? ALERT_TOKEN, ALERT_FORWARD_URL: forwardUrl, ALERT_FORWARD_MAX_ATTEMPTS: '2',
      ALERT_FORWARD_BACKOFF_BASE_MS: '1', ALERT_FORWARD_BACKOFF_CAP_MS: '1',
      ...(options.queueMax ? { ALERT_FORWARD_QUEUE_MAX: String(options.queueMax) } : {}) },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stdout.on('data', (data) => { output += String(data); });
  child.stderr.on('data', (data) => { output += String(data); });
  const exited = once(child, 'exit');
  const terminate = async (signal: NodeJS.Signals = 'SIGTERM') => {
    if (child.exitCode === null && child.signalCode === null) child.kill(signal);
    await exited;
  };
  const close = async () => {
    await terminate();
    if (!options.keepFiles) await rm(dir, { recursive: true, force: true });
  };
  const listening = /listening on internal port (\d+);/;
  let port: number;
  try {
    const started = await waitFor(async () => {
      assert.equal(child.exitCode, null, output);
      assert.equal(child.signalCode, null, output);
      return listening.exec(output);
    }, (match) => match !== null);
    assert.ok(started);
    port = Number(started[1]);
  } catch (error) {
    await terminate();
    if (!options.directory) await rm(dir, { recursive: true, force: true });
    throw error;
  }
  return { base: `http://127.0.0.1:${port}`, receipt, logs: () => output, close, terminate, directory: dir };
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
      const response = await fetch(`${child.base}/alerts`, { method: 'POST', headers: { Authorization: `Bearer ${ALERT_TOKEN}` }, body: JSON.stringify({
        service: 'meshcore-analytics', check: 'readiness', kind: 'alert',
      }) });
      assert.equal(response.status, 503);
      assert.equal(response.headers.get('retry-after'), '5');
      assert.equal((await response.json() as { archived: boolean }).archived, true);
      assert.match(await readFile(child.receipt, 'utf8'), /"source":"synthetic"/);
      assert.equal((await fetch(`${child.base}/readyz`)).status, 503);
      assert.doesNotMatch(child.logs(), /forward succeeded/);
    } finally { await child.close(); }
  }
});

test('Alertmanager and synthetic forwarding both require a successful stub delivery before readiness becomes healthy', { timeout: 10_000 }, async (t) => {
  const received: Array<{ content: string; allowed_mentions: { parse: string[] } }> = [];
  const stub = http.createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(Buffer.from(chunk));
    received.push(JSON.parse(Buffer.concat(chunks).toString('utf8')));
    res.writeHead(204).end();
  });
  const forwardUrl = `http://127.0.0.1:${await listenStub(t, stub)}/secret-stub-token`;
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
  }
});

test('failed stub deliveries remain unready and reach bounded retries', { timeout: 10_000 }, async (t) => {
  let calls = 0;
  const stub = http.createServer((_req, res) => { calls += 1; res.writeHead(503).end(); });
  const child = await receiver(`http://127.0.0.1:${await listenStub(t, stub)}/failed`);
  try {
    await fetch(`${child.base}/alerts`, { method: 'POST', headers: { Authorization: `Bearer ${ALERT_TOKEN}` }, body: JSON.stringify({
      service: 'meshcore-analytics', kind: 'alert', check: 'test-only',
    }) });
    await waitFor(async () => (await readdir(`${child.receipt}.queue`)).filter((name) => name.endsWith('.dead.json')), (files) => files.length === 1);
    const dead = (await readdir(`${child.receipt}.queue`)).find((name) => name.endsWith('.dead.json'))!;
    assert.equal(JSON.parse(await readFile(path.join(`${child.receipt}.queue`, dead), 'utf8')).attempts, 2);
    assert.equal(calls, 2);
    const metrics = await waitFor(
      async () => (await fetch(`${child.base}/metrics`)).text(),
      (body) => /^meshcore_alert_receiver_dead_lettered_total 1$/m.test(body),
    );
    assert.match(metrics, /^meshcore_alert_receiver_failed_total 2$/m);
    assert.match(metrics, /^meshcore_alert_receiver_dead_lettered_total 1$/m);
    assert.match(metrics, /^meshcore_alert_receiver_unresolved_dead_letter_count 1$/m);
    assert.equal((await fetch(`${child.base}/readyz`)).status, 503);
  } finally {
    await child.close();
  }
});

test('Compose lets Alertmanager start before the first successful delivery', async () => {
  const compose = await readFile('../docker-compose.yml', 'utf8');
  const service = compose.split('\n  alert-receiver:')[1]!.split('\n  alertmanager:')[0]!;
  assert.match(service, /wget -qO- http:\/\/127.0.0.1:8080\/healthz/);
  const alertmanager = compose.split('\n  alertmanager:')[1]!.split('\n  postgres-exporter:')[0]!;
  assert.doesNotMatch(alertmanager, /alert-receiver:/);
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
  const child = await receiver('', { token: '' });
  try {
    const response = await fetch(`${child.base}/alerts`, {
      method: 'POST', headers: { Authorization: `Bearer ${ALERT_TOKEN}` },
      body: JSON.stringify({ kind: 'alert', check: 'forged' }),
    });
    assert.equal(response.status, 503);
    await assert.rejects(readFile(child.receipt), { code: 'ENOENT' });
  } finally { await child.close(); }
});

test('accepted alert survives receiver SIGKILL and a full queue refuses new work', { timeout: 15_000 }, async (t) => {
  const replies: http.ServerResponse[] = [];
  let calls = 0;
  let hold = true;
  const stub = http.createServer((_req, res) => {
    calls += 1;
    if (hold) replies.push(res);
    else res.writeHead(204).end();
  });
  const url = `http://127.0.0.1:${await listenStub(t, stub)}/restart-stub`;
  const first = await receiver(url, { keepFiles: true, queueMax: 1 });
  let second: Awaited<ReturnType<typeof receiver>> | undefined;
  try {
    const send = () => fetch(`${first.base}/alerts`, { method: 'POST', headers: { authorization: 'Bearer owned-audit-test-token' }, body: JSON.stringify({
      service: 'meshcore-analytics', kind: 'alert', check: 'restart-proof',
    }) });
    assert.equal((await send()).status, 202);
    await waitFor(async () => calls, (n) => n === 1);
    assert.equal((await send()).status, 503);
    assert.equal((await readdir(`${first.receipt}.queue`)).filter((file) => file.endsWith('.pending.json')).length, 1);
    await first.terminate('SIGKILL');
    hold = false;
    second = await receiver(url, { directory: first.directory, keepFiles: true });
    await waitFor(async () => (await fetch(`${second!.base}/readyz`)).status, (status) => status === 200);
    assert.equal(calls, 2, 'restart retries the fsynced forwarding intent');
    assert.equal((await readdir(`${first.receipt}.queue`)).filter((file) => file.endsWith('.pending.json')).length, 0);
  } finally {
    for (const reply of replies) reply.destroy();
    await first.close();
    await second?.close();
    await rm(first.directory, { recursive: true, force: true });
  }
});


test('receipt persistence recovers after a transient filesystem failure', { timeout: 10_000 }, async () => {
  const child = await receiver('');
  const send = () => fetch(`${child.base}/alerts`, { method: 'POST', headers: { authorization: 'Bearer owned-audit-test-token' }, body: JSON.stringify({
    service: 'meshcore-analytics', kind: 'alert', check: 'filesystem-recovery',
  }) });
  try {
    await mkdir(child.receipt);
    assert.equal((await send()).status, 503, 'storage failures must remain retryable');
    await rm(child.receipt, { recursive: true });
    const response = await send();
    assert.equal(response.status, 503);
    assert.equal((await response.json() as { archived: boolean }).archived, true);
    assert.match(await readFile(child.receipt, 'utf8'), /filesystem-recovery/);
  } finally { await child.close(); }
});

test('deployed receiver bearer authentication survives the durability changes', async () => {
  const child = await receiver('');
  const body = JSON.stringify({ service: 'meshcore-analytics', kind: 'alert', check: 'auth-fixture' });
  try {
    assert.equal((await fetch(`${child.base}/alerts`, { method: 'POST', body })).status, 401);
    assert.equal((await fetch(`${child.base}/alerts`, {
      method: 'POST', headers: { authorization: 'Bearer wrong-test-token' }, body,
    })).status, 401);
    await assert.rejects(readFile(child.receipt), { code: 'ENOENT' });
    assert.equal((await fetch(`${child.base}/alerts`, {
      method: 'POST', headers: { authorization: 'Bearer owned-audit-test-token' }, body,
    })).status, 503);
    assert.equal((await readFile(child.receipt, 'utf8')).trim().split('\n').length, 1);
  } finally { await child.close(); }
});

test('authenticated redrive retains dead letters and restores readiness only after delivery, including restart', { timeout: 15_000 }, async (t) => {
  let mode: 'fail' | 'hold' | 'success' = 'fail';
  let calls = 0;
  const held: http.ServerResponse[] = [];
  const stub = http.createServer((_req, res) => {
    calls += 1;
    if (mode === 'hold') held.push(res);
    else res.writeHead(mode === 'fail' ? 503 : 204).end();
  });
  const url = `http://127.0.0.1:${await listenStub(t, stub)}/owned-redrive`;
  const child = await receiver(url, { keepFiles: true });
  let restarted: Awaited<ReturnType<typeof receiver>> | undefined;
  try {
    assert.equal((await fetch(`${child.base}/alerts`, {
      method: 'POST', headers: { authorization: `Bearer ${ALERT_TOKEN}` },
      body: JSON.stringify({ service: 'meshcore-analytics', kind: 'alert', check: 'redrive-owned' }),
    })).status, 202);
    const directory = `${child.receipt}.queue`;
    const [name] = await waitFor(async () => (await readdir(directory)).filter((file) => file.endsWith('.dead.json')), (names) => names.length === 1);
    const original = await readFile(path.join(directory, name!));
    const id = name!.slice(0, -'.dead.json'.length);
    const requestId = randomUUID();
    const endpoint = `${child.base}/alerts/dead-letters/${id}/redrive`;
    const body = JSON.stringify({ requestId, reason: 'owned endpoint recovered' });
    const post = (target = endpoint, token = ALERT_TOKEN, content = body) => fetch(target, {
      method: 'POST', headers: { authorization: `Bearer ${token}` }, body: content,
    });
    assert.equal((await fetch(endpoint, { method: 'POST', body: 'not-json' })).status, 401);
    for (const target of [endpoint, `${child.base}/alerts/dead-letters/${randomUUID()}/redrive`]) {
      assert.equal((await post(target, 'incorrect', 'not-json')).status, 401, 'auth precedes lookup and body parsing');
    }
    assert.equal((await post(`${child.base}/alerts/dead-letters/${randomUUID()}/redrive`)).status, 404);
    assert.equal((await post(`${child.base}/alerts/dead-letters/%2e%2e%2foutside/redrive`)).status, 400);
    assert.equal((await post(endpoint, ALERT_TOKEN, '{}')).status, 400);
    await waitFor(async () => (await (await fetch(`${child.base}/healthz`)).json() as { pending_count: number }).pending_count, (count) => count === 0);
    mode = 'hold';
    const admitted = await post();
    assert.equal(admitted.status, 202, (await admitted.text()) + child.logs());
    await waitFor(async () => held.length, (count) => count === 1);
    assert.equal((await post()).status, 202, 'same request remains idempotent during delivery');
    assert.equal((await post(endpoint, ALERT_TOKEN, JSON.stringify({ requestId: randomUUID(), reason: 'competing' }))).status, 409);
    const pendingResponse = await fetch(`${child.base}/readyz`);
    assert.equal(pendingResponse.status, 503);
    const pending = await pendingResponse.json() as Record<string, unknown>;
    assert.equal(pending['dead_letter_count'], 1);
    assert.equal(pending['unresolved_dead_letter_count'], 1);
    assert.equal(pending['resolved_dead_letter_count'], 0);
    held[0]!.writeHead(204).end();
    mode = 'success';
    await waitFor(async () => (await fetch(`${child.base}/readyz`)).status, (status) => status === 200);
    assert.deepEqual(await readFile(path.join(directory, name!)), original);
    assert.equal((await post()).status, 200);
    await child.terminate();
    restarted = await receiver(url, { directory: child.directory, keepFiles: true });
    const health = await (await fetch(`${restarted.base}/readyz`)).json() as Record<string, unknown>;
    assert.equal(health['status'], 'ok');
    assert.equal(health['dead_letter_count'], 1);
    assert.equal(health['unresolved_dead_letter_count'], 0);
    assert.equal(health['resolved_dead_letter_count'], 1);
    assert.equal(calls, 3, 'two failed initial sends and one successful redrive; restart does not resend');
    assert.deepEqual(await readFile(path.join(directory, name!)), original);
  } finally {
    for (const res of held) res.destroy();
    await child.close();
    await restarted?.close();
    await rm(child.directory, { recursive: true, force: true });
  }
});
