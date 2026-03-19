import fs from 'node:fs';
import { createInterface } from 'node:readline';
import { upsertMqttNodeLogin } from '../db/ownerAuth.js';
import { query } from '../db/index.js';

const LOG_PATH = process.env['MOSQUITTO_LOG_PATH'] ?? '/mosquitto/log/mosquitto.log';
const POLL_INTERVAL_MS = 5_000;
const HISTORICAL_SCAN_BYTES = 2_000_000; // last 2 MB on startup

// Matches: as meshcore_NODEIDPREFIX_N or meshcore_client_NODEIDPREFIX_N (... u'USERNAME')
const CONNECT_RE = /as meshcore_(?:client_)?([0-9A-F]+)_\d+ \([^)]*u'([^']+)'\)/i;

async function resolveNodeId(prefix: string): Promise<string | null> {
  if (prefix.length < 4) return null;
  const res = await query<{ node_id: string }>(
    `SELECT node_id FROM nodes WHERE node_id ILIKE $1 LIMIT 2`,
    [`${prefix}%`],
  );
  // Only proceed if unambiguous match
  return res.rows.length === 1 ? (res.rows[0]?.node_id ?? null) : null;
}

async function processLine(line: string): Promise<void> {
  if (!line.includes('New client connected')) return;
  const m = CONNECT_RE.exec(line);
  if (!m) return;
  const [, nodePrefix, mqttUsername] = m;
  if (!nodePrefix || !mqttUsername || mqttUsername === 'backend') return;
  try {
    const nodeId = await resolveNodeId(nodePrefix);
    if (nodeId) await upsertMqttNodeLogin(mqttUsername, nodeId);
  } catch (err) {
    console.error('[conn-monitor] processLine error:', (err as Error).message);
  }
}

async function scanRange(start: number, end: number): Promise<void> {
  return new Promise((resolve) => {
    const stream = fs.createReadStream(LOG_PATH, { start, end });
    const rl = createInterface({ input: stream, crlfDelay: Infinity });
    const lines: string[] = [];
    rl.on('line', (line) => lines.push(line));
    rl.on('close', async () => {
      for (const line of lines) await processLine(line);
      resolve();
    });
  });
}

export function startMqttConnectionMonitor(): void {
  if (!fs.existsSync(LOG_PATH)) {
    console.warn('[conn-monitor] log not found at', LOG_PATH, '— retrying in 30s');
    setTimeout(startMqttConnectionMonitor, 30_000);
    return;
  }

  let position = 0;

  async function init(): Promise<void> {
    const { size } = fs.statSync(LOG_PATH);
    const start = Math.max(0, size - HISTORICAL_SCAN_BYTES);
    if (start < size) {
      console.log('[conn-monitor] scanning historical log entries...');
      await scanRange(start, size - 1);
    }
    position = size;
    console.log('[conn-monitor] ready, polling every', POLL_INTERVAL_MS / 1000, 's');
  }

  async function poll(): Promise<void> {
    try {
      const { size } = fs.statSync(LOG_PATH);
      if (size < position) position = 0; // log rotated
      if (size > position) {
        await scanRange(position, size - 1);
        position = size;
      }
    } catch {
      // log temporarily unavailable
    }
  }

  init().catch((err: Error) => console.error('[conn-monitor] init error:', err.message));
  setInterval(() => poll().catch((err: Error) => console.error('[conn-monitor] poll error:', err.message)), POLL_INTERVAL_MS);
  console.log('[conn-monitor] started, monitoring', LOG_PATH);
}
