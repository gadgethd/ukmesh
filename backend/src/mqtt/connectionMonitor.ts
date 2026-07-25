import fs from 'node:fs';
import { createInterface } from 'node:readline';
import {
  parseDeniedOwnerPublish,
  parseMeshcoreClientNodePrefix,
  parseMosquittoConnection,
} from './brokerLog.js';

const LOG_PATH = process.env['MOSQUITTO_LOG_PATH'] ?? '/mosquitto/log/mosquitto.log';
const POLL_INTERVAL_MS = Number(process.env['MQTT_CONN_MONITOR_POLL_MS'] ?? 2_000);
const HISTORICAL_SCAN_BYTES = 50_000_000;
const MAX_TRACKED_CLIENTS = Number(process.env['MQTT_CONN_MONITOR_MAX_CLIENTS'] ?? 4_096);
const CLIENT_TTL_MS = Number(process.env['MQTT_CONN_MONITOR_CLIENT_TTL_MS'] ?? 15 * 60_000);

type ClientObservation = {
  mqttUsername: string;
  seenAt: number;
};

const clientToUsername = new Map<string, ClientObservation>();

function pruneClientObservations(now = Date.now()): void {
  for (const [clientId, observation] of clientToUsername) {
    if (now - observation.seenAt > CLIENT_TTL_MS) clientToUsername.delete(clientId);
  }
  while (clientToUsername.size > MAX_TRACKED_CLIENTS) {
    const oldest = clientToUsername.keys().next().value as string | undefined;
    if (!oldest) break;
    clientToUsername.delete(oldest);
  }
}

export type BrokerSecurityObservation =
  | { kind: 'connection'; mqttUsername: string; clientId: string; claimedNodePrefix: string | null }
  | { kind: 'denied-publish'; mqttUsername: string | null; clientId: string; claimedNodeId: string };

/**
 * Broker logs are observations, never authorization evidence. This helper
 * deliberately has no database or ACL dependency.
 */
export function observeBrokerLogLine(line: string, now = Date.now()): BrokerSecurityObservation | null {
  const connection = parseMosquittoConnection(line);
  if (connection) {
    clientToUsername.delete(connection.clientId);
    clientToUsername.set(connection.clientId, {
      mqttUsername: connection.mqttUsername,
      seenAt: now,
    });
    pruneClientObservations(now);
    return {
      kind: 'connection',
      mqttUsername: connection.mqttUsername,
      clientId: connection.clientId,
      claimedNodePrefix: parseMeshcoreClientNodePrefix(connection.clientId),
    };
  }

  const denied = parseDeniedOwnerPublish(line);
  if (!denied) return null;
  const observation = clientToUsername.get(denied.clientId);
  if (observation && now - observation.seenAt > CLIENT_TTL_MS) {
    clientToUsername.delete(denied.clientId);
  }
  return {
    kind: 'denied-publish',
    mqttUsername: clientToUsername.get(denied.clientId)?.mqttUsername ?? null,
    clientId: denied.clientId,
    claimedNodeId: denied.nodeId,
  };
}

async function scanRange(start: number, end: number): Promise<void> {
  let connections = 0;
  let deniedPublishes = 0;
  await new Promise<void>((resolve, reject) => {
    const stream = fs.createReadStream(LOG_PATH, { start, end });
    const rl = createInterface({ input: stream, crlfDelay: Infinity });
    rl.on('line', (line) => {
      const observation = observeBrokerLogLine(line);
      if (observation?.kind === 'connection') connections++;
      if (observation?.kind === 'denied-publish') deniedPublishes++;
    });
    rl.on('close', resolve);
    rl.on('error', reject);
  });
  if (connections > 0 || deniedPublishes > 0) {
    console.log('[conn-monitor] security observations:', { connections, deniedPublishes });
  }
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
    position = size;
    if (start < size) await scanRange(start, size - 1);
    console.log('[conn-monitor] audit-only mode; broker logs cannot grant ownership or ACLs');
  }

  async function poll(): Promise<void> {
    try {
      const { size } = fs.statSync(LOG_PATH);
      if (size < position) position = 0;
      if (size > position) {
        await scanRange(position, size - 1);
        position = size;
      }
      pruneClientObservations();
    } catch {
      // Log temporarily unavailable.
    }
  }

  void init().catch((err: Error) => console.error('[conn-monitor] init error:', err.message));
  setInterval(() => {
    void poll().catch((err: Error) => console.error('[conn-monitor] poll error:', err.message));
  }, POLL_INTERVAL_MS).unref();
}
