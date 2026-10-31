import 'node:process';
import { WebSocket } from 'ws';
import { pathToFileURL } from 'node:url';
import path from 'node:path';
import { DurableAlertQueue, type AlertReceipt } from './alertDeliveryQueue.js';
import { createSyntheticAlertEvaluator } from './syntheticAlerts.js';
import { observeSyntheticCheck } from '../metrics.js';
import { startWorkerMetrics } from './workerMetrics.js';
import {
  persistSyntheticCheckResults,
  type SyntheticCheckResult as CheckResult,
} from './syntheticPersistence.js';


const BASE_URL = (process.env['SYNTHETIC_BASE_URL'] ?? 'http://backend:3000').replace(/\/$/, '');
const WS_URL = process.env['SYNTHETIC_WS_URL'] ?? BASE_URL.replace(/^http/, 'ws');
function boundedNumber(raw: string | undefined, fallback: number, minimum: number): number {
  const parsed = Number(raw ?? fallback);
  return Number.isFinite(parsed) ? Math.max(minimum, parsed) : fallback;
}

const INTERVAL_MS = boundedNumber(process.env['SYNTHETIC_INTERVAL_MS'], 60_000, 15_000);
const FULL_INTERVAL_MS = Math.min(
  15 * 60_000,
  boundedNumber(process.env['SYNTHETIC_FULL_INTERVAL_MS'], 12 * 60_000, 10 * 60_000),
);
const TIMEOUT_MS = boundedNumber(process.env['SYNTHETIC_TIMEOUT_MS'], 10_000, 1_000);
const INITIAL_STATE_TIMEOUT_MS = boundedNumber(
  process.env['SYNTHETIC_INITIAL_STATE_TIMEOUT_MS'],
  35_000,
  10_000,
);
const FAILURE_THRESHOLD = boundedNumber(process.env['SYNTHETIC_FAILURE_THRESHOLD'], 3, 1);
const ALERT_WEBHOOK_URL = String(process.env['ALERT_WEBHOOK_URL'] ?? '').trim();
const ALERT_RECEIVER_TOKEN = String(process.env['ALERT_RECEIVER_TOKEN'] ?? '').trim();
const ALERT_DIRECTORY = process.env['SYNTHETIC_ALERT_DIRECTORY'] ?? '/var/lib/meshcore-synthetic';
let evaluateAlerts: (results: CheckResult[]) => Promise<void>;
let archival: Promise<void> | null = null;

function elapsedMs(started: number): number {
  return Math.max(0, Math.round(performance.now() - started));
}

async function httpCheck(name: string, path: string, validate?: (body: unknown) => boolean): Promise<CheckResult> {
  const started = performance.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const response = await fetch(`${BASE_URL}${path}`, { signal: controller.signal });
    const body = await response.json().catch(() => null) as unknown;
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    if (validate && !validate(body)) throw new Error('unexpected response payload');
    return { name, status: 'ok', latencyMs: elapsedMs(started), detail: `HTTP ${response.status}` };
  } catch (err) {
    return { name, status: 'failed', latencyMs: elapsedMs(started), detail: (err as Error).message.slice(0, 300) };
  } finally {
    clearTimeout(timeout);
  }
}

function websocketUrl(skipInitialState: boolean): string {
  const url = new URL(`${WS_URL}/ws`);
  url.searchParams.set('network', 'ukmesh');
  if (skipInitialState) url.searchParams.set('initial_state', '0');
  return url.toString();
}

async function websocketPingCheck(): Promise<CheckResult> {
  const started = performance.now();
  return new Promise((resolve) => {
    const socket = new WebSocket(websocketUrl(true), { handshakeTimeout: TIMEOUT_MS });
    let settled = false;
    const finish = (status: CheckResult['status'], detail: string) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      socket.close();
      resolve({ name: 'websocket_ping', status, latencyMs: elapsedMs(started), detail: detail.slice(0, 300) });
    };
    const timeout = setTimeout(() => finish('failed', `no pong within ${TIMEOUT_MS}ms`), TIMEOUT_MS);
    socket.on('open', () => socket.ping());
    socket.on('pong', () => finish('ok', 'handshake and pong received'));
    socket.on('error', (err) => finish('failed', err.message));
    socket.on('close', () => finish('failed', 'socket closed before pong'));
  });
}

async function websocketInitialStateCheck(): Promise<CheckResult> {
  const started = performance.now();
  return new Promise((resolve) => {
    const socket = new WebSocket(websocketUrl(false), { handshakeTimeout: INITIAL_STATE_TIMEOUT_MS });
    let settled = false;
    const finish = (status: CheckResult['status'], detail: string) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      socket.close();
      resolve({ name: 'websocket_initial_state', status, latencyMs: elapsedMs(started), detail: detail.slice(0, 300) });
    };
    const timeout = setTimeout(
      () => finish('failed', `no initial_state within ${INITIAL_STATE_TIMEOUT_MS}ms`),
      INITIAL_STATE_TIMEOUT_MS,
    );
    socket.on('message', (data) => {
      try {
        const lines = String(data).split('\n').filter(Boolean);
        if (lines.some((line) => (JSON.parse(line) as { type?: string }).type === 'initial_state')) {
          finish('ok', 'received initial_state');
        }
      } catch {
        finish('failed', 'malformed WebSocket payload');
      }
    });
    socket.on('error', (err) => finish('failed', err.message));
    socket.on('close', () => finish('failed', 'socket closed before initial_state'));
  });
}

async function persistResults(results: CheckResult[]): Promise<void> {
  await persistSyntheticCheckResults(results);
}

async function notify(receipt: AlertReceipt): Promise<void> {
  if (!ALERT_WEBHOOK_URL) throw new Error('ALERT_WEBHOOK_URL is not configured');
  const response = await fetch(ALERT_WEBHOOK_URL, {
    method: 'POST', headers: {
      'Content-Type': 'application/json',
      ...(ALERT_RECEIVER_TOKEN ? { Authorization: `Bearer ${ALERT_RECEIVER_TOKEN}` } : {}),
    },
    body: JSON.stringify({ service: 'meshcore-analytics', check: receipt.alert_names[0],
      kind: receipt.status === 'firing' ? 'alert' : 'recovery', timestamp: receipt.received_at }),
    signal: AbortSignal.timeout(TIMEOUT_MS),
  });
  await response.body?.cancel();
  if (!response.ok) throw new Error(`alert webhook HTTP ${response.status}`);
}

const notifications = new DurableAlertQueue({
  directory: path.join(ALERT_DIRECTORY, 'outbox'), maxItems: 1_000, maxAttempts: Number.MAX_SAFE_INTEGER,
  backoffBaseMs: 1_000, backoffCapMs: 60_000, send: notify,
});

export async function handleCheckResults(results: CheckResult[], deps: {
  evaluate: (results: CheckResult[]) => Promise<void>;
  persist: (results: CheckResult[]) => Promise<void>;
}): Promise<void> {
  // Alert evaluation cannot be gated on a dependency that is being monitored.
  await deps.evaluate(results);
  await deps.persist(results).catch((error: unknown) => {
    console.error('[synthetic] result archive failed:', error instanceof Error ? error.message : error);
  });
}

async function runChecks(fullCanary: boolean): Promise<void> {
  const checks: Array<{ name: string; promise: Promise<CheckResult> }> = [
    { name: 'websocket_ping', promise: websocketPingCheck() },
  ];
  if (fullCanary) {
    checks.push(
      {
        name: 'http_liveness',
        promise: httpCheck('http_liveness', '/healthz', (body) => (body as { status?: string } | null)?.status === 'ok'),
      },
      {
        name: 'dependency_readiness',
        promise: httpCheck('dependency_readiness', '/readyz', (body) => (body as { status?: string } | null)?.status === 'ready'),
      },
      {
        name: 'stats_api',
        promise: httpCheck('stats_api', '/api/stats?network=ukmesh', (body) => Number.isFinite(Number((body as { totalNodes?: number } | null)?.totalNodes))),
      },
      { name: 'websocket_initial_state', promise: websocketInitialStateCheck() },
    );
  }
  const settled = await Promise.allSettled(checks.map((check) => check.promise));
  const results = settled.map((result, index): CheckResult => {
    if (result.status === 'fulfilled') return result.value;
    return {
      name: checks[index]?.name ?? `check_${index}`,
      status: 'failed',
      latencyMs: TIMEOUT_MS,
      detail: (result.reason instanceof Error ? result.reason.message : String(result.reason)).slice(0, 300),
    };
  });
  for (const result of results) observeSyntheticCheck(result.name, result.status === 'ok');
  await handleCheckResults(results, { evaluate: evaluateAlerts, persist: async (items) => {
    if (archival) return;
    archival = persistResults(items).finally(() => { archival = null; });
    let timer: ReturnType<typeof setTimeout> | undefined;
    try { await Promise.race([archival, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error('result archive timed out')), 2_000);
    })]); } finally { if (timer) clearTimeout(timer); }
  } });
  console.log(
    `[synthetic] mode=${fullCanary ? 'full' : 'ping'} `
      + results.map((result) => `${result.name}=${result.status}:${result.latencyMs}ms`).join(' '),
  );
}

async function main(): Promise<void> {
  startWorkerMetrics();
  await notifications.initialize();
  evaluateAlerts = await createSyntheticAlertEvaluator({ queue: notifications,
    statePath: path.join(ALERT_DIRECTORY, 'incidents.json'), threshold: FAILURE_THRESHOLD });
  notifications.start();
  for (const signal of ['SIGTERM', 'SIGINT'] as const) process.once(signal, () => {
    void notifications.close().finally(() => process.exit(0));
  });
  let running = false;
  // Compose's backend healthcheck is deliberately liveness-only. Give MQTT
  // readiness one lightweight cycle to settle before the first full canary.
  let lastFullCanaryAt = Date.now() - FULL_INTERVAL_MS + INTERVAL_MS;
  const runCycle = async () => {
    if (running) {
      console.warn('[synthetic] check cycle skipped; previous cycle is still active');
      return;
    }
    running = true;
    const now = Date.now();
    const fullCanary = lastFullCanaryAt === 0 || now - lastFullCanaryAt >= FULL_INTERVAL_MS;
    if (fullCanary) lastFullCanaryAt = now;
    await runChecks(fullCanary).catch((err) => {
      console.error('[synthetic] check cycle failed', (err as Error).message);
    }).finally(() => {
      running = false;
    });
  };
  await runCycle();
  setInterval(() => void runCycle(), INTERVAL_MS);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main().catch((err) => {
  console.error('[synthetic] fatal startup error', err);
  process.exit(1);
});
