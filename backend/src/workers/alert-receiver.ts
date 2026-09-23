import http, { type IncomingMessage, type ServerResponse } from 'node:http';
import { appendFile, mkdir, rename, stat } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import {
  AlertQueueFullError,
  DurableAlertQueue,
  type AlertDeliveryQueueMetrics,
  type AlertReceipt,
} from './alertDeliveryQueue.js';

export type { AlertReceipt } from './alertDeliveryQueue.js';

const PORT = boundedInteger(process.env['ALERT_RECEIVER_PORT'], 8080, 1, 65_535);
const MAX_BODY_BYTES = boundedInteger(process.env['ALERT_RECEIVER_MAX_BODY_BYTES'], 262_144, 1_024, 1_048_576);
const MAX_LOG_BYTES = boundedInteger(process.env['ALERT_RECEIVER_MAX_LOG_BYTES'], 1_048_576, 65_536, 100 * 1_048_576);
const RECEIPT_PATH = process.env['ALERT_RECEIVER_PATH'] ?? '/var/lib/meshcore-alerts/alerts.jsonl';
const FORWARD_URL = validForwardUrl(process.env['ALERT_FORWARD_URL']);
const FORWARD_TIMEOUT_MS = boundedInteger(process.env['ALERT_FORWARD_TIMEOUT_MS'], 10_000, 1_000, 60_000);
const FORWARD_QUEUE_MAX_ITEMS = boundedInteger(process.env['ALERT_FORWARD_QUEUE_MAX_ITEMS'], 10_000, 1, 100_000);
const MAX_FORWARD_ATTEMPTS = boundedInteger(process.env['ALERT_FORWARD_MAX_ATTEMPTS'], 5, 1, 20);
const FORWARD_BACKOFF_BASE_MS = boundedInteger(process.env['ALERT_FORWARD_BACKOFF_BASE_MS'], 1_000, 100, 60_000);
const FORWARD_BACKOFF_CAP_MS = boundedInteger(process.env['ALERT_FORWARD_BACKOFF_CAP_MS'], 60_000, 1_000, 300_000);
const FORWARD_QUEUE_DIR = `${RECEIPT_PATH}.queue`;

export type AlertForwardPayload = {
  content: string;
  allowed_mentions: { parse: string[] };
};

function boundedInteger(
  raw: string | undefined,
  fallback: number,
  minimum: number,
  maximum: number,
): number {
  const parsed = Number(raw ?? fallback);
  if (!Number.isSafeInteger(parsed)) return fallback;
  return Math.min(maximum, Math.max(minimum, parsed));
}

function validForwardUrl(raw: string | undefined): string | null {
  const candidate = String(raw ?? '').trim();
  if (!candidate) return null;
  try {
    const parsed = new URL(candidate);
    return parsed.protocol === 'https:' || parsed.protocol === 'http:' ? parsed.href : null;
  } catch {
    return null;
  }
}

function object(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function boundedText(value: unknown, maximum = 120): string {
  return typeof value === 'string' ? value.slice(0, maximum) : '';
}

export function summarizeAlertPayload(payload: unknown, now = new Date()): AlertReceipt {
  const root = object(payload);
  const alerts = Array.isArray(root?.['alerts']) ? root['alerts'] : [];
  if (alerts.length > 0) {
    const summaries = alerts
      .map((entry) => object(entry))
      .filter((entry): entry is Record<string, unknown> => entry !== null)
      .slice(0, 100);
    const names = summaries
      .map((entry) => boundedText(object(entry['labels'])?.['alertname']))
      .filter(Boolean);
    const firing = summaries.filter((entry) => entry['status'] === 'firing').length;
    const resolved = summaries.filter((entry) => entry['status'] === 'resolved').length;
    return {
      received_at: now.toISOString(),
      source: 'alertmanager',
      status: firing > 0 ? 'firing' : resolved > 0 ? 'resolved' : 'unknown',
      alert_names: [...new Set(names)].slice(0, 50),
      firing,
      resolved,
    };
  }

  if (root?.['service'] === 'meshcore-analytics' && typeof root['check'] === 'string') {
    const kind = boundedText(root['kind']);
    return {
      received_at: now.toISOString(),
      source: 'synthetic',
      status: kind === 'alert' ? 'firing' : kind === 'recovery' ? 'recovery' : 'unknown',
      alert_names: [boundedText(root['check'])].filter(Boolean),
      firing: kind === 'alert' ? 1 : 0,
      resolved: kind === 'recovery' ? 1 : 0,
    };
  }

  return {
    received_at: now.toISOString(),
    source: 'unknown',
    status: 'unknown',
    alert_names: [],
    firing: 0,
    resolved: 0,
  };
}

export function buildAlertForwardPayload(receipt: AlertReceipt): AlertForwardPayload {
  const state = receipt.status === 'firing'
    ? { icon: '🚨', label: 'firing' }
    : receipt.status === 'resolved' || receipt.status === 'recovery'
      ? { icon: '✅', label: receipt.status }
      : { icon: '⚠️', label: 'unknown' };
  const names = receipt.alert_names.length > 0
    ? receipt.alert_names.join(', ')
    : 'unnamed alert';
  const content = [
    `${state.icon} **UKMesh alert ${state.label}**`,
    `Source: ${receipt.source}`,
    `Alerts: ${names}`,
    `Firing: ${receipt.firing} · Resolved: ${receipt.resolved}`,
    `Received: ${receipt.received_at}`,
  ].join('\n').slice(0, 2_000);

  return {
    content,
    allowed_mentions: { parse: [] },
  };
}

async function readBody(req: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;
    if (size > MAX_BODY_BYTES) throw new Error('request body too large');
    chunks.push(buffer);
  }
  return Buffer.concat(chunks);
}

async function rotateIfNeeded(nextBytes: number): Promise<void> {
  try {
    const current = await stat(RECEIPT_PATH);
    if (current.size + nextBytes <= MAX_LOG_BYTES) return;
    await rename(RECEIPT_PATH, `${RECEIPT_PATH}.1`);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
  }
}

let writeChain = Promise.resolve();
function persistReceipt(receipt: AlertReceipt): Promise<void> {
  const line = `${JSON.stringify(receipt)}\n`;
  writeChain = writeChain.catch(() => undefined).then(async () => {
    await mkdir(path.dirname(RECEIPT_PATH), { recursive: true, mode: 0o700 });
    await rotateIfNeeded(Buffer.byteLength(line));
    await appendFile(RECEIPT_PATH, line, { encoding: 'utf8', mode: 0o600 });
  });
  return writeChain;
}

async function forward(receipt: AlertReceipt): Promise<void> {
  if (!FORWARD_URL) return;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FORWARD_TIMEOUT_MS);
  try {
    const response = await fetch(FORWARD_URL, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(buildAlertForwardPayload(receipt)),
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    console.log(`[alert-receiver] forward succeeded: HTTP ${response.status}`);
  } finally {
    clearTimeout(timeout);
  }
}

// Delivery records are fsynced into the mounted alert volume before HTTP 202.
// The JSONL receipt archive remains useful for local inspection, but it is not
// the queue: queued work survives process restarts and dead letters stay visible.
let deliveryQueue: DurableAlertQueue | null = null;
let deliveryQueueReady = false;

const emptyQueueMetrics: AlertDeliveryQueueMetrics = {
  pendingCount: 0,
  oldestQueueAgeSeconds: 0,
  deadLetterCount: 0,
  lastSuccessAt: null,
  lastError: null,
};

export function deriveDeliveryHealth(
  forwardConfigured: boolean,
  queueReady: boolean,
  metrics: AlertDeliveryQueueMetrics,
): { degraded: boolean; detail: string } {
  if (!forwardConfigured) {
    return { degraded: true, detail: 'archive-only mode: ALERT_FORWARD_URL is not configured' };
  }
  if (!queueReady) return { degraded: true, detail: 'durable forwarding queue is not ready' };
  if (metrics.deadLetterCount > 0) {
    return { degraded: true, detail: `${metrics.deadLetterCount} alert(s) are dead-lettered` };
  }
  if (metrics.lastError) {
    return { degraded: true, detail: 'a forwarding attempt failed; retry is pending' };
  }
  if (metrics.oldestQueueAgeSeconds > 5 * 60) {
    return { degraded: true, detail: `oldest alert has waited ${metrics.oldestQueueAgeSeconds}s for delivery` };
  }
  return {
    degraded: false,
    detail: metrics.pendingCount > 0
      ? `${metrics.pendingCount} alert(s) queued for forwarding`
      : 'forwarding destination and durable queue are ready',
  };
}

function deliveryHealth() {
  const metrics = deliveryQueue?.metrics() ?? emptyQueueMetrics;
  return {
    ...deriveDeliveryHealth(Boolean(FORWARD_URL), deliveryQueueReady, metrics),
    metrics,
  };
}

function json(res: ServerResponse, statusCode: number, payload: unknown): void {
  res.writeHead(statusCode, {
    'content-type': 'application/json',
    'cache-control': 'no-store',
    'x-content-type-options': 'nosniff',
  });
  res.end(JSON.stringify(payload));
}

const server = http.createServer(async (req, res) => {
  if (req.method === 'GET' && (req.url === '/healthz' || req.url === '/readyz')) {
    const health = deliveryHealth();
    // Compose uses /healthz as a liveness probe. Keep a running receiver
    // healthy there while /readyz reports degraded forwarding availability.
    const readinessFailed = req.url === '/readyz' && health.degraded;
    json(res, readinessFailed ? 503 : 200, {
      status: health.degraded ? 'degraded' : 'ok',
      detail: health.detail,
      pending_count: health.metrics.pendingCount,
      queue_oldest_age_seconds: health.metrics.oldestQueueAgeSeconds,
      dead_letter_count: health.metrics.deadLetterCount,
      last_success_at: health.metrics.lastSuccessAt,
      last_error: health.metrics.lastError,
    });
    return;
  }
  if (req.method !== 'POST' || req.url !== '/alerts') {
    json(res, 404, { error: 'not found' });
    return;
  }
  let payload: unknown;
  try {
    const body = await readBody(req);
    payload = JSON.parse(body.toString('utf8')) as unknown;
  } catch (error) {
    const tooLarge = (error as Error).message === 'request body too large';
    json(res, tooLarge ? 413 : 400, { error: tooLarge ? 'request too large' : 'invalid alert payload' });
    return;
  }

  const receipt = summarizeAlertPayload(payload);
  if (FORWARD_URL) {
    if (!deliveryQueueReady || !deliveryQueue) {
      res.setHeader('retry-after', '5');
      json(res, 503, { error: 'durable alert forwarding queue is not ready' });
      return;
    }
    try {
      await deliveryQueue.enqueue(receipt);
    } catch (error) {
      console.error('[alert-receiver] could not persist forwarding queue item:', (error as Error).message);
      res.setHeader('retry-after', '5');
      json(res, 503, {
        error: error instanceof AlertQueueFullError
          ? 'alert forwarding queue is full'
          : 'could not persist alert for forwarding',
      });
      return;
    }
    // The durable queue is authoritative for configured forwarding. Preserve
    // the human-readable archive when possible, without discarding queued work
    // if archive rotation or append fails.
    try {
      await persistReceipt(receipt);
    } catch (error) {
      console.error('[alert-receiver] receipt archive write failed:', (error as Error).message);
    }
  } else {
    try {
      await persistReceipt(receipt);
    } catch (error) {
      console.error('[alert-receiver] could not persist archive-only alert:', (error as Error).message);
      res.setHeader('retry-after', '5');
      json(res, 503, { error: 'could not persist alert receipt' });
      return;
    }
  }
  json(res, 202, { accepted: true });
});

function shutdown(signal: string): void {
  deliveryQueue?.stop();
  server.close((error) => {
    if (error) {
      console.error(`[alert-receiver] ${signal} shutdown failed:`, error.message);
      process.exitCode = 1;
    }
  });
}

const isMain = process.argv[1] !== undefined
  && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isMain) {
  const start = async () => {
    if (FORWARD_URL) {
      deliveryQueue = new DurableAlertQueue({
        directory: FORWARD_QUEUE_DIR,
        maxItems: FORWARD_QUEUE_MAX_ITEMS,
        maxAttempts: MAX_FORWARD_ATTEMPTS,
        backoffBaseMs: FORWARD_BACKOFF_BASE_MS,
        backoffCapMs: FORWARD_BACKOFF_CAP_MS,
        send: forward,
      });
      await deliveryQueue.initialize();
      deliveryQueueReady = true;
      deliveryQueue.start();
    }
    server.listen(PORT, '0.0.0.0', () => {
      console.log(`[alert-receiver] listening on internal port ${PORT}`);
    });
  };
  void start().catch((error: unknown) => {
    console.error('[alert-receiver] startup failed:', error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
  process.once('SIGTERM', () => shutdown('SIGTERM'));
  process.once('SIGINT', () => shutdown('SIGINT'));
}
