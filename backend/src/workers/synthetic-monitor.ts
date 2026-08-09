import 'node:process';
import { WebSocket } from 'ws';
import { initDb } from '../db/index.js';
import { observeSyntheticCheck } from '../metrics.js';
import { startWorkerMetrics } from './workerMetrics.js';
import {
  persistSyntheticCheckResults,
  type SyntheticCheckResult as CheckResult,
} from './syntheticPersistence.js';

type AlertState = { failures: number; alerting: boolean };

const BASE_URL = (process.env['SYNTHETIC_BASE_URL'] ?? 'http://backend:3000').replace(/\/$/, '');
const WS_URL = process.env['SYNTHETIC_WS_URL'] ?? BASE_URL.replace(/^http/, 'ws');
function boundedNumber(raw: string | undefined, fallback: number, minimum: number): number {
  const parsed = Number(raw ?? fallback);
  return Number.isFinite(parsed) ? Math.max(minimum, parsed) : fallback;
}

const INTERVAL_MS = boundedNumber(process.env['SYNTHETIC_INTERVAL_MS'], 60_000, 15_000);
const TIMEOUT_MS = boundedNumber(process.env['SYNTHETIC_TIMEOUT_MS'], 10_000, 1_000);
const FAILURE_THRESHOLD = boundedNumber(process.env['SYNTHETIC_FAILURE_THRESHOLD'], 3, 1);
const ALERT_WEBHOOK_URL = String(process.env['ALERT_WEBHOOK_URL'] ?? '').trim();
const states = new Map<string, AlertState>();

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

async function websocketCheck(): Promise<CheckResult> {
  const started = performance.now();
  return new Promise((resolve) => {
    const socket = new WebSocket(`${WS_URL}/ws?network=ukmesh`, { handshakeTimeout: TIMEOUT_MS });
    let settled = false;
    const finish = (status: CheckResult['status'], detail: string) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      socket.close();
      resolve({ name: 'websocket_initial_state', status, latencyMs: elapsedMs(started), detail: detail.slice(0, 300) });
    };
    const timeout = setTimeout(() => finish('failed', `no initial_state within ${TIMEOUT_MS}ms`), TIMEOUT_MS);
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

async function notify(kind: 'alert' | 'recovery', result: CheckResult): Promise<void> {
  const payload = {
    kind,
    service: 'meshcore-analytics',
    check: result.name,
    status: result.status,
    latency_ms: result.latencyMs,
    detail: result.detail,
    timestamp: new Date().toISOString(),
  };
  const log = kind === 'alert' ? console.error : console.warn;
  log(`[synthetic][${kind}] ${result.name}: ${result.detail} (${result.latencyMs}ms)`);
  if (!ALERT_WEBHOOK_URL) return;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    await fetch(ALERT_WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
  } catch (err) {
    console.error('[synthetic] alert webhook failed', (err as Error).message);
  } finally {
    clearTimeout(timeout);
  }
}

async function evaluateAlerts(results: CheckResult[]): Promise<void> {
  for (const result of results) {
    const state = states.get(result.name) ?? { failures: 0, alerting: false };
    if (result.status === 'ok') {
      if (state.alerting) await notify('recovery', result);
      states.set(result.name, { failures: 0, alerting: false });
      continue;
    }
    state.failures += 1;
    if (!state.alerting && state.failures >= FAILURE_THRESHOLD) {
      state.alerting = true;
      await notify('alert', result);
    }
    states.set(result.name, state);
  }
}

async function runChecks(): Promise<void> {
  const checks = [
    httpCheck('http_liveness', '/healthz', (body) => (body as { status?: string } | null)?.status === 'ok'),
    httpCheck('dependency_readiness', '/readyz', (body) => (body as { status?: string } | null)?.status === 'ready'),
    httpCheck('stats_api', '/api/stats?network=ukmesh', (body) => Number.isFinite(Number((body as { totalNodes?: number } | null)?.totalNodes))),
    websocketCheck(),
  ];
  const settled = await Promise.allSettled(checks);
  const names = ['http_liveness', 'dependency_readiness', 'stats_api', 'websocket_initial_state'];
  const results = settled.map((result, index): CheckResult => {
    if (result.status === 'fulfilled') return result.value;
    return {
      name: names[index] ?? `check_${index}`,
      status: 'failed',
      latencyMs: TIMEOUT_MS,
      detail: (result.reason instanceof Error ? result.reason.message : String(result.reason)).slice(0, 300),
    };
  });
  for (const result of results) observeSyntheticCheck(result.name, result.status === 'ok');
  await persistResults(results);
  await evaluateAlerts(results);
  console.log(`[synthetic] ${results.map((result) => `${result.name}=${result.status}:${result.latencyMs}ms`).join(' ')}`);
}

async function main(): Promise<void> {
  startWorkerMetrics();
  await initDb();
  let running = false;
  const runCycle = async () => {
    if (running) {
      console.warn('[synthetic] check cycle skipped; previous cycle is still active');
      return;
    }
    running = true;
    await runChecks().catch((err) => {
      console.error('[synthetic] check cycle failed', (err as Error).message);
    }).finally(() => {
      running = false;
    });
  };
  await runCycle();
  setInterval(() => void runCycle(), INTERVAL_MS);
}

main().catch((err) => {
  console.error('[synthetic] fatal startup error', err);
  process.exit(1);
});
