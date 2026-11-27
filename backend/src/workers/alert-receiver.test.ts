import assert from 'node:assert/strict';
import test from 'node:test';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import type { AddressInfo } from 'node:net';
import {
  buildAlertForwardPayload,
  renderAlertReceiverMetrics,
  summarizeAlertPayload,
} from './alert-receiver.js';
import type { AlertDeliveryQueueMetrics } from './alertDeliveryQueue.js';

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

function alertHeaders(token = ALERT_TOKEN): Record<string, string> {
  return { 'content-type': 'application/json', authorization: `Bearer ${token}` };
}

function postAlert(base: string, payload: unknown, token = ALERT_TOKEN): Promise<Response> {
  return fetch(`${base}/alerts`, {
    method: 'POST',
    headers: alertHeaders(token),
    body: JSON.stringify(payload),
  });
}

async function receiver(forwardUrl: string, options: { directory?: string; keepFiles?: boolean; queueMax?: number; token?: string } = {}) {
  const dir = options.directory ?? await mkdtemp(path.join(tmpdir(), 'auditwave-alert-test-'));
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
  try {
    await waitFor(async () => output, (value) => value.includes('listening on internal port'));
  } catch (error) { await close(); throw error; }
  return { base: `http://127.0.0.1:${port}`, receipt, logs: () => output, close, terminate, directory: dir };
}

test('unconfigured or invalid forwarding is archive-only: liveness 200, readiness 503, POST retryable 503', { timeout: 15_000 }, async () => {
  for (const url of ['', 'file:///invalid']) {
    const child = await receiver(url);
    try {
      assert.match(child.logs(), /DEGRADED: ALERT_FORWARD_URL is unset or invalid; forwardConfigured=false/);
      assert.equal((await fetch(`${child.base}/healthz`)).status, 200);
      const ready = await fetch(`${child.base}/readyz`);
      assert.equal(ready.status, 503);
      assert.match((await ready.json() as { detail: string }).detail, /archive-only/);

      const archived = await postAlert(child.base, {
        service: 'meshcore-analytics', check: 'readiness', kind: 'alert',
      });
      assert.equal(archived.status, 503, 'archive-only mode must not acknowledge delivery');
      assert.equal(archived.headers.get('retry-after'), '5');
      assert.deepEqual(await archived.json(), {
        error: 'archive-only mode: ALERT_FORWARD_URL is not configured',
        archived: true,
      });
      assert.match(await readFile(child.receipt, 'utf8'), /"source":"synthetic"/);
      assert.equal((await fetch(`${child.base}/readyz`)).status, 503);
      assert.doesNotMatch(child.logs(), /forward succeeded/);
    } finally { await child.close(); }
  }
});

test('forwarding is ready on a fresh volume before the first successful delivery', { timeout: 10_000 }, async () => {
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

    // Fresh volume: no successful forward has ever happened, yet readiness and
    // Alertmanager bootstrap must succeed.
    const bootstrap = await fetch(`${child.base}/readyz`);
    assert.equal(bootstrap.status, 200);
    const bootstrapBody = await bootstrap.json() as { status: string; last_success_at: string | null };
    assert.equal(bootstrapBody.status, 'ok');
    assert.equal(bootstrapBody.last_success_at, null);

    for (const payload of [
      { alerts: [{ status: 'firing', labels: { alertname: 'BackendDown', secret: 'hidden' } }] },
      { service: 'meshcore-analytics', kind: 'recovery', check: 'dependency_readiness', detail: 'hidden' },
    ]) {
      assert.equal((await postAlert(child.base, payload)).status, 202);
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

    const metrics = await waitFor(async () => {
      const response = await fetch(`${child.base}/metrics`);
      assert.equal(response.status, 200);
      assert.match(response.headers.get('content-type') ?? '', /^text\/plain; version=0\.0\.4/);
      return response.text();
    }, (body) => body.includes('meshcore_alert_receiver_forwarded_total 2'));
    assert.match(metrics, /^meshcore_alert_receiver_enqueued_total 2$/m);
    assert.match(metrics, /^meshcore_alert_receiver_pending_count 0$/m);
    const lastSuccess = Number(metrics.match(/^meshcore_alert_receiver_last_success_timestamp_seconds (\d+(?:\.\d+)?)$/m)?.[1]);
    assert.ok(Number.isFinite(lastSuccess) && lastSuccess > 0);
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
    await postAlert(child.base, {
      service: 'meshcore-analytics', kind: 'alert', check: 'test-only',
    });
    await waitFor(async () => (await readdir(`${child.receipt}.queue`)).filter((name) => name.endsWith('.dead.json')), (files) => files.length === 1);
    const dead = (await readdir(`${child.receipt}.queue`)).find((name) => name.endsWith('.dead.json'))!;
    assert.equal(JSON.parse(await readFile(path.join(`${child.receipt}.queue`, dead), 'utf8')).attempts, 2);
    assert.equal(calls, 2);
    assert.equal((await fetch(`${child.base}/readyz`)).status, 503);
    const metrics = await (await fetch(`${child.base}/metrics`)).text();
    assert.match(metrics, /^meshcore_alert_receiver_dead_letter_count 1$/m);
    assert.match(metrics, /^meshcore_alert_receiver_dead_lettered_total 1$/m);
    assert.match(metrics, /^meshcore_alert_receiver_failed_total 2$/m);
  } finally {
    await child.close();
    stub.closeAllConnections();
    await new Promise<void>((resolve) => stub.close(() => resolve()));
  }
});

test('a dead letter on an existing volume makes readiness fail after restart', { timeout: 15_000 }, async () => {
  const stub = http.createServer((_req, res) => { res.writeHead(204).end(); });
  stub.listen(0, '127.0.0.1');
  await once(stub, 'listening');
  const url = `http://127.0.0.1:${(stub.address() as AddressInfo).port}/dead-letter-stub`;
  const first = await receiver(url, { keepFiles: true });
  let second: Awaited<ReturnType<typeof receiver>> | undefined;
  try {
    assert.equal((await fetch(`${first.base}/readyz`)).status, 200);
    await first.terminate();
    const queueDirectory = `${first.receipt}.queue`;
    await writeFile(path.join(queueDirectory, 'stale-alert.dead.json'), JSON.stringify({
      id: 'stale-alert',
      receipt: {
        received_at: '2026-09-23T10:00:00.000Z', source: 'synthetic', status: 'firing',
        alert_names: ['stale'], firing: 1, resolved: 0,
      },
      createdAt: '2026-09-23T10:00:00.000Z', attempts: 5, lastAttemptAt: null,
      nextAttemptAt: '2026-09-23T10:00:00.000Z', lastError: 'exhausted',
    }));

    second = await receiver(url, { directory: first.directory, keepFiles: true });
    const ready = await fetch(`${second.base}/readyz`);
    assert.equal(ready.status, 503);
    const body = await ready.json() as { detail: string; dead_letter_count: number };
    assert.match(body.detail, /dead-lettered/);
    assert.equal(body.dead_letter_count, 1);
    assert.equal((await fetch(`${second.base}/healthz`)).status, 200);
  } finally {
    await first.close();
    await second?.close();
    stub.closeAllConnections();
    await new Promise<void>((resolve) => stub.close(() => resolve()));
    await rm(first.directory, { recursive: true, force: true });
  }
});

test('metrics exposition follows Prometheus text format with required gauges and counters', () => {
  const metrics: AlertDeliveryQueueMetrics = {
    pendingCount: 3,
    oldestQueueAgeSeconds: 42,
    deadLetterCount: 1,
    lastSuccessAt: '2026-09-23T10:00:00.000Z',
    lastError: null,
    enqueuedTotal: 7,
    forwardedTotal: 4,
    failedTotal: 2,
    deadLetteredTotal: 1,
  };
  const body = renderAlertReceiverMetrics(metrics, true, true);

  assert.match(body, /^# HELP meshcore_alert_receiver_pending_count /m);
  assert.match(body, /^# TYPE meshcore_alert_receiver_pending_count gauge$/m);
  assert.match(body, /^meshcore_alert_receiver_pending_count 3$/m);
  assert.match(body, /^meshcore_alert_receiver_dead_letter_count 1$/m);
  assert.match(body, /^meshcore_alert_receiver_queue_oldest_age_seconds 42$/m);
  assert.match(body, /^meshcore_alert_receiver_last_success_timestamp_seconds 1790157600$/m);
  assert.match(body, /^# TYPE meshcore_alert_receiver_enqueued_total counter$/m);
  assert.match(body, /^meshcore_alert_receiver_enqueued_total 7$/m);
  assert.match(body, /^meshcore_alert_receiver_forwarded_total 4$/m);
  assert.match(body, /^meshcore_alert_receiver_failed_total 2$/m);
  assert.match(body, /^meshcore_alert_receiver_dead_lettered_total 1$/m);

  // Every non-comment line must be "name value" so a scraper can parse it.
  for (const line of body.trimEnd().split('\n')) {
    if (line.startsWith('#')) continue;
    assert.match(line, /^[a-z][a-z0-9_]* -?\d+(?:\.\d+)?$/, `unparseable exposition line: ${line}`);
  }
  assert.doesNotMatch(body, /undefined|NaN/);

  const never = renderAlertReceiverMetrics(
    { ...metrics, lastSuccessAt: null, pendingCount: 0, deadLetterCount: 0 },
    false,
    false,
  );
  assert.match(never, /^meshcore_alert_receiver_last_success_timestamp_seconds 0$/m);
  assert.match(never, /^meshcore_alert_receiver_forward_configured 0$/m);
  assert.match(never, /^meshcore_alert_receiver_queue_ready 0$/m);
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
  const child = await receiver('', { token: '' });
  try {
    const response = await postAlert(child.base, { kind: 'alert', check: 'forged' });
    assert.equal(response.status, 503);
    await assert.rejects(readFile(child.receipt), { code: 'ENOENT' });
  } finally { await child.close(); }
});

test('accepted alert survives receiver SIGKILL and a full queue refuses new work', { timeout: 15_000 }, async () => {
  const replies: http.ServerResponse[] = [];
  let calls = 0;
  let hold = true;
  const stub = http.createServer((_req, res) => {
    calls += 1;
    if (hold) replies.push(res);
    else res.writeHead(204).end();
  });
  stub.listen(0, '127.0.0.1');
  await once(stub, 'listening');
  const url = `http://127.0.0.1:${(stub.address() as AddressInfo).port}/restart-stub`;
  const first = await receiver(url, { keepFiles: true, queueMax: 1 });
  let second: Awaited<ReturnType<typeof receiver>> | undefined;
  try {
    const send = () => postAlert(first.base, {
      service: 'meshcore-analytics', kind: 'alert', check: 'restart-proof',
    });
    assert.equal((await send()).status, 202);
    await waitFor(async () => calls, (n) => n === 1);
    assert.equal((await send()).status, 503);
    assert.equal((await readdir(`${first.receipt}.queue`)).filter((file) => file.endsWith('.pending.json')).length, 1);
    await first.terminate('SIGKILL');
    hold = false;
    second = await receiver(url, { directory: first.directory, keepFiles: true });
    await waitFor(async () => calls, (count) => count === 2);
    await waitFor(
      async () => (await readdir(`${first.receipt}.queue`)).filter((file: string) => file.endsWith('.pending.json')).length,
      (count) => count === 0,
    );
    assert.equal((await fetch(`${second.base}/readyz`)).status, 200);
    assert.equal((await readdir(`${first.receipt}.queue`)).filter((file) => file.endsWith('.pending.json')).length, 0);
  } finally {
    for (const reply of replies) reply.destroy();
    await first.close();
    await second?.close();
    stub.closeAllConnections();
    await new Promise<void>((resolve) => stub.close(() => resolve()));
    await rm(first.directory, { recursive: true, force: true });
  }
});


test('receipt persistence recovers after a transient filesystem failure', { timeout: 10_000 }, async () => {
  const child = await receiver('');
  const send = () => postAlert(child.base, {
    service: 'meshcore-analytics', kind: 'alert', check: 'filesystem-recovery',
  });
  try {
    await mkdir(child.receipt);
    const blocked = await send();
    assert.equal(blocked.status, 503, 'storage failures must remain retryable');
    assert.match((await blocked.json() as { error: string }).error, /could not persist/);
    await rm(child.receipt, { recursive: true });
    const archived = await send();
    assert.equal(archived.status, 503, 'archive-only mode is retryable by design');
    assert.equal((await archived.json() as { archived: boolean }).archived, true);
    assert.match(await readFile(child.receipt, 'utf8'), /filesystem-recovery/);
  } finally { await child.close(); }
});

test('deployed receiver bearer authentication survives the durability changes', async () => {
  const child = await receiver('');
  const body = JSON.stringify({ service: 'meshcore-analytics', kind: 'alert', check: 'auth-fixture' });
  try {
    assert.equal((await fetch(`${child.base}/alerts`, { method: 'POST', body })).status, 401);
    assert.equal((await postAlert(child.base, { service: 'meshcore-analytics' }, 'wrong-test-token')).status, 401);
    await assert.rejects(readFile(child.receipt), { code: 'ENOENT' });
    const accepted = await fetch(`${child.base}/alerts`, {
      method: 'POST', headers: alertHeaders(), body,
    });
    assert.equal(accepted.status, 503);
    assert.equal(accepted.headers.get('retry-after'), '5');
    assert.equal((await readFile(child.receipt, 'utf8')).trim().split('\n').length, 1);
  } finally { await child.close(); }
});
