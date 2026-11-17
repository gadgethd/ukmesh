import { WebSocket } from 'ws';
import { INITIAL_STATE_COLUMNS_FORMAT, INITIAL_STATE_FORMAT_PARAMETER, decodeInitialStateMessage } from '../contracts/initialStateColumns.generated.js';
import type { SyntheticCheckResult as CheckResult } from './syntheticPersistence.js';

function elapsedMs(started: number): number {
  return Math.max(0, Math.round(performance.now() - started));
}

function websocketUrl(baseUrl: string, skipInitialState: boolean): string {
  const url = new URL(`${baseUrl}/ws`);
  url.searchParams.set('network', 'ukmesh');
  if (skipInitialState) url.searchParams.set('initial_state', '0');
  else url.searchParams.set(INITIAL_STATE_FORMAT_PARAMETER, INITIAL_STATE_COLUMNS_FORMAT);
  return url.toString();
}

export async function websocketPingCheck(baseUrl: string, timeoutMs: number): Promise<CheckResult> {
  const started = performance.now();
  return new Promise((resolve) => {
    const socket = new WebSocket(websocketUrl(baseUrl, true), { handshakeTimeout: timeoutMs });
    let settled = false;
    const finish = (status: CheckResult['status'], detail: string) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      socket.close();
      resolve({ name: 'websocket_ping', status, latencyMs: elapsedMs(started), detail: detail.slice(0, 300) });
    };
    const timeout = setTimeout(() => finish('failed', `no pong within ${timeoutMs}ms`), timeoutMs);
    socket.on('open', () => socket.ping());
    socket.on('pong', () => finish('ok', 'handshake and pong received'));
    socket.on('error', (err) => finish('failed', err.message));
    socket.on('close', () => finish('failed', 'socket closed before pong'));
  });
}

export async function websocketInitialStateCheck(baseUrl: string, timeoutMs: number): Promise<CheckResult> {
  const started = performance.now();
  return new Promise((resolve) => {
    const socket = new WebSocket(websocketUrl(baseUrl, false), { handshakeTimeout: timeoutMs });
    let settled = false;
    const finish = (status: CheckResult['status'], detail: string) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      socket.close();
      resolve({ name: 'websocket_initial_state', status, latencyMs: elapsedMs(started), detail: detail.slice(0, 300) });
    };
    const timeout = setTimeout(
      () => finish('failed', `no initial_state within ${timeoutMs}ms`),
      timeoutMs,
    );
    socket.on('message', (data) => {
      try {
        const lines = String(data).split('\n').filter(Boolean);
        if (lines.some((line) => (decodeInitialStateMessage(JSON.parse(line)) as { type?: string }).type === 'initial_state')) {
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

