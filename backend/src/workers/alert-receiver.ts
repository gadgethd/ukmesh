import http, { type IncomingMessage, type ServerResponse } from 'node:http';
import { appendFile, mkdir, rename, stat } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const PORT = boundedInteger(process.env['ALERT_RECEIVER_PORT'], 8080, 1, 65_535);
const MAX_BODY_BYTES = boundedInteger(process.env['ALERT_RECEIVER_MAX_BODY_BYTES'], 262_144, 1_024, 1_048_576);
const MAX_LOG_BYTES = boundedInteger(process.env['ALERT_RECEIVER_MAX_LOG_BYTES'], 1_048_576, 65_536, 100 * 1_048_576);
const RECEIPT_PATH = process.env['ALERT_RECEIVER_PATH'] ?? '/var/lib/meshcore-alerts/alerts.jsonl';
const FORWARD_URL = validForwardUrl(process.env['ALERT_FORWARD_URL']);
const FORWARD_TIMEOUT_MS = boundedInteger(process.env['ALERT_FORWARD_TIMEOUT_MS'], 10_000, 1_000, 60_000);

export type AlertReceipt = {
  received_at: string;
  source: 'alertmanager' | 'synthetic' | 'unknown';
  status: 'firing' | 'resolved' | 'recovery' | 'unknown';
  alert_names: string[];
  firing: number;
  resolved: number;
};

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
  writeChain = writeChain.then(async () => {
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

// ---- BUG-014: durable delivery state ----
// Forwarding used to fire-and-forget: failures were logged asynchronously and
// /healthz stayed green while alerts silently never reached an operator.
// Now every receipt is retried with bounded exponential backoff, dead-lettered
// after MAX_FORWARD_ATTEMPTS, and readiness reflects delivery health.
const FORWARD_DEAD_LETTER_PATH = `${RECEIPT_PATH}.dead`;
const MAX_FORWARD_ATTEMPTS = Number(process.env['ALERT_FORWARD_MAX_ATTEMPTS'] ?? 5);
const FORWARD_BACKOFF_BASE_MS = Number(process.env['ALERT_FORWARD_BACKOFF_BASE_MS'] ?? 1_000);
const FORWARD_BACKOFF_CAP_MS = Number(process.env['ALERT_FORWARD_BACKOFF_CAP_MS'] ?? 60_000);

type DeliveryState = {
  attempts: number;
  lastAttempt: number;
  pending: boolean;
};

const delivery = new Map<string, DeliveryState>();
let forwardChain = Promise.resolve();
let lastForwardSuccessAt = FORWARD_URL ? 0 : Date.now();

async function deadLetter(receipt: AlertReceipt, reason: string): Promise<void> {
  try {
    await appendFile(FORWARD_DEAD_LETTER_PATH, `${JSON.stringify({ ...receipt, deadLetterReason: reason })}\n`, {
      encoding: 'utf8',
      mode: 0o600,
    });
  } catch (error) {
    console.error('[alert-receiver] failed to write dead-letter:', (error as Error).message);
  }
}

/** Enqueue a receipt for durable forwarding with bounded retries. */
function enqueueForward(receipt: AlertReceipt): void {
  const key = `${receipt.alert_names.join(',')}:${receipt.received_at}`;
  if (delivery.has(key) && delivery.get(key)!.pending) return; // already in flight
  delivery.set(key, { attempts: 0, lastAttempt: 0, pending: true });

  forwardChain = forwardChain.then(async () => {
    while (true) {
      const state = delivery.get(key)!;
      if (state.attempts >= MAX_FORWARD_ATTEMPTS) {
        state.pending = false;
        await deadLetter(receipt, `max attempts (${MAX_FORWARD_ATTEMPTS})`);
        console.error(`[alert-receiver] dead-lettered after ${MAX_FORWARD_ATTEMPTS} attempts: ${key}`);
        return;
      }
      if (state.lastAttempt > 0) {
        const backoff = Math.min(FORWARD_BACKOFF_CAP_MS, FORWARD_BACKOFF_BASE_MS * 2 ** (state.attempts - 1));
        await new Promise((resolve) => setTimeout(resolve, backoff));
      }
      state.lastAttempt = Date.now();
      try {
        await forward(receipt);
        state.pending = false;
        lastForwardSuccessAt = Date.now();
        return;
      } catch (error) {
        state.attempts += 1;
        console.error(`[alert-receiver] forward attempt ${state.attempts}/${MAX_FORWARD_ATTEMPTS} failed:`, (error as Error).message);
      }
    }
  });
}

function deliveryHealth(): { degraded: boolean; detail: string } {
  if (!FORWARD_URL) {
    return { degraded: true, detail: 'archive-only mode: ALERT_FORWARD_URL is not configured' };
  }
  if (lastForwardSuccessAt === 0) {
    return { degraded: true, detail: 'no successful forward since startup' };
  }
  const pending = [...delivery.values()].filter((state) => state.pending);
  const stuck = pending.filter((state) => Date.now() - state.lastAttempt > 5 * 60_000);
  if (stuck.length > 0) {
    return { degraded: true, detail: `${stuck.length} alert(s) undelivered for over 5 minutes` };
  }
  return { degraded: false, detail: `delivering to ${FORWARD_URL}` };
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
    // /healthz always 200 so the compose healthcheck (wget -qO-) never
    // restarts the container for degraded delivery; /readyz returns 503 so
    // orchestrators/operators can see real readiness.
    const degraded = req.url === '/readyz' && health.degraded;
    json(res, degraded ? 503 : 200, { status: health.degraded ? 'degraded' : 'ok', detail: health.detail });
    return;
  }
  if (req.method !== 'POST' || req.url !== '/alerts') {
    json(res, 404, { error: 'not found' });
    return;
  }
  try {
    const body = await readBody(req);
    const payload = JSON.parse(body.toString('utf8')) as unknown;
    const receipt = summarizeAlertPayload(payload);
    await persistReceipt(receipt);
    enqueueForward(receipt);
    json(res, 202, { accepted: true });
  } catch (error) {
    const tooLarge = (error as Error).message === 'request body too large';
    json(res, tooLarge ? 413 : 400, { error: tooLarge ? 'request too large' : 'invalid alert payload' });
  }
});

function shutdown(signal: string): void {
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
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`[alert-receiver] listening on internal port ${PORT}`);
  });
  process.once('SIGTERM', () => shutdown('SIGTERM'));
  process.once('SIGINT', () => shutdown('SIGINT'));
}
