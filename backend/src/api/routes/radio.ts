import { Router } from 'express';
import { parseBoundedInteger, parseBoundedString } from '../utils/input.js';
import {
  fetchBoundedJson,
  UpstreamCircuit,
  UpstreamRequestError,
} from '../../http/upstreamJson.js';

const router = Router();
export function parseRadioBotUrl(raw: string | undefined): URL | null {
  const value = String(raw ?? '').trim();
  if (!value) return null;
  const url = new URL(value);
  if (!['http:', 'https:'].includes(url.protocol)) {
    throw new Error('RADIO_BOT_URL must use http or https');
  }
  return url;
}
const RADIO_BOT_URL = parseRadioBotUrl(process.env['RADIO_BOT_URL']);
const CONNECT_TIMEOUT_MS = Math.min(
  10_000,
  Math.max(100, Number(process.env['RADIO_BOT_CONNECT_TIMEOUT_MS'] ?? 2_000) || 2_000),
);
const TOTAL_TIMEOUT_MS = Math.min(
  30_000,
  Math.max(CONNECT_TIMEOUT_MS, Number(process.env['RADIO_BOT_TOTAL_TIMEOUT_MS'] ?? 8_000) || 8_000),
);
const RESPONSE_MAX_BYTES = Math.min(
  4 * 1024 * 1024,
  Math.max(1_024, Number(process.env['RADIO_BOT_RESPONSE_MAX_BYTES'] ?? 512 * 1024) || 512 * 1024),
);
const circuit = new UpstreamCircuit(3, 30_000);

async function proxyRadioJson(
  path: string,
  init: RequestInit = {},
): Promise<unknown> {
  if (!RADIO_BOT_URL) throw new Error('RADIO_BOT_NOT_CONFIGURED');
  return fetchBoundedJson(new URL(path, RADIO_BOT_URL), init, {
    connectTimeoutMs: CONNECT_TIMEOUT_MS,
    totalTimeoutMs: TOTAL_TIMEOUT_MS,
    maxResponseBytes: RESPONSE_MAX_BYTES,
    circuit,
  });
}

function respondUpstreamError(res: import('express').Response, error: unknown): void {
  if ((error as Error).message === 'RADIO_BOT_NOT_CONFIGURED') {
    res.status(404).json({ error: 'radio bot is not configured' });
    return;
  }
  const circuitOpen = error instanceof UpstreamRequestError
    && error.code === 'CIRCUIT_OPEN';
  res.status(circuitOpen ? 503 : 502).json({
    error: circuitOpen ? 'radio bot temporarily unavailable' : 'radio bot request failed',
  });
}

router.get('/radio-history', async (req, res) => {
  const target = parseBoundedString(req.query['target'], {
    name: 'target',
    required: true,
    maxLength: 128,
  })!;
  const limit = parseBoundedInteger(req.query['limit'], {
    name: 'limit',
    defaultValue: 168,
    min: 1,
    max: 500,
  });

  try {
    res.json(await proxyRadioJson('/history', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ target, limit }),
    }));
  } catch (error) {
    respondUpstreamError(res, error);
  }
});

router.get('/radio-stats', async (_req, res) => {
  try {
    res.json(await proxyRadioJson('/state'));
  } catch (error) {
    respondUpstreamError(res, error);
  }
});

export default router;
