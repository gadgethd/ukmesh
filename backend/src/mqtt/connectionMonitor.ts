import fs from 'node:fs';
import { recordUntrustedMqttNodeObservation } from '../db/ownerAuth.js';
import { mqttDeniedPublishesTotal } from '../metrics.js';
import {
  parseDeniedPublish,
  parseMeshcoreClientNodePrefix,
  parseMosquittoConnection,
  parseOwnerNodeTopic,
} from './brokerLog.js';

const LOG_PATH = process.env['MOSQUITTO_LOG_PATH'] ?? '/mosquitto/log/mosquitto.log';
const POLL_INTERVAL_MS = Number(process.env['MQTT_CONN_MONITOR_POLL_MS'] ?? 2_000);
const HISTORICAL_SCAN_BYTES = 50_000_000;
const MAX_DENIAL_DETAILS_PER_SCAN = 64;
const MAX_TRACKED_CLIENTS = Number(process.env['MQTT_CONN_MONITOR_MAX_CLIENTS'] ?? 4_096);
const CLIENT_TTL_MS = Number(process.env['MQTT_CONN_MONITOR_CLIENT_TTL_MS'] ?? 15 * 60_000);
const MAX_AUDIT_OBSERVATIONS_PER_SCAN = Math.min(
  65_536,
  Math.max(1, Number(process.env['MQTT_AUDIT_MAX_OBSERVATIONS_PER_SCAN'] ?? 4_096) || 4_096),
);

type ClientObservation = {
  mqttUsername: string;
  seenAt: number;
};

const clientToUsername = new Map<string, ClientObservation>();
let monitorTimer: NodeJS.Timeout | null = null;
let retryTimer: NodeJS.Timeout | null = null;
let monitorInFlight: Promise<void> | null = null;
let monitorStopping = false;

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
  | { kind: 'denied-publish'; mqttUsername: string | null; clientId: string; topic: string; claimedNodeId: string | null };

type ScanPhase = 'history' | 'live';

function deniedTopicKind(topic: string): string {
  const suffix = topic.split('/').at(-1);
  return suffix && ['raw', 'packets', 'status', 'neighbors', 'neighbours'].includes(suffix) ? suffix : 'other';
}

/**
 * Broker logs are observations, never authorization evidence. No code in this
 * module writes owner_account_nodes or broker ACLs.
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

  const denied = parseDeniedPublish(line);
  if (!denied) return null;
  const observation = clientToUsername.get(denied.clientId);
  if (observation && now - observation.seenAt > CLIENT_TTL_MS) {
    clientToUsername.delete(denied.clientId);
  }
  return {
    kind: 'denied-publish',
    mqttUsername: clientToUsername.get(denied.clientId)?.mqttUsername ?? null,
    clientId: denied.clientId,
    topic: denied.topic,
    claimedNodeId: parseOwnerNodeTopic(denied.topic),
  };
}

export async function scanBrokerLogRange(
  logPath: string,
  start: number,
  end: number,
  phase: ScanPhase,
  recordObservation = recordUntrustedMqttNodeObservation,
): Promise<number> {
  const denied = new Map<string, { mqttUsername: string; claimedNodeId: string }>();
  const details = new Map<string, { clientId: string; topic: string; mqttUsername: string | null; count: number }>();
  let connectionCount = 0;
  let deniedPublishCount = 0;
  let omittedDeniedDetails = 0;
  let position = start;
  let pending: Buffer = Buffer.alloc(0);
  const stream = fs.createReadStream(logPath, { start, end });
  for await (const chunk of stream) {
    const bytes = pending.length ? Buffer.concat([pending, chunk as Buffer]) : chunk as Buffer;
    let lineStart = 0;
    let newline: number;
    while ((newline = bytes.indexOf(10, lineStart)) !== -1) {
      const line = bytes.subarray(lineStart, newline).toString('utf8').replace(/\r$/, '');
      position += newline - lineStart + 1;
      lineStart = newline + 1;
      const observation = observeBrokerLogLine(line);
      if (observation?.kind === 'connection') {
        connectionCount += 1;
      } else if (observation?.kind === 'denied-publish') {
        deniedPublishCount += 1;
        mqttDeniedPublishesTotal.inc({ kind: deniedTopicKind(observation.topic), phase });
        const detailKey = JSON.stringify([observation.clientId, observation.topic, observation.mqttUsername]);
        const detail = details.get(detailKey);
        if (detail) {
          detail.count += 1;
        } else if (details.size < MAX_DENIAL_DETAILS_PER_SCAN) {
          details.set(detailKey, {
            clientId: observation.clientId,
            topic: observation.topic,
            mqttUsername: observation.mqttUsername,
            count: 1,
          });
        } else {
          omittedDeniedDetails += 1;
        }
        if (observation.mqttUsername && observation.claimedNodeId && denied.size < MAX_AUDIT_OBSERVATIONS_PER_SCAN) {
          const key = `${observation.mqttUsername}\u0000${observation.claimedNodeId}`;
          denied.set(key, {
            mqttUsername: observation.mqttUsername,
            claimedNodeId: observation.claimedNodeId,
          });
        }
      }
    }
    pending = bytes.subarray(lineStart);
  }

  for (const detail of details.values()) {
    // JSON keeps untrusted client/topic text on one log line. Bound field sizes
    // as well as row count; the broker log remains the source for longer values.
    console.warn('[conn-monitor] denied publish', JSON.stringify({
      phase,
      ...detail,
      clientId: detail.clientId.slice(0, 512),
      topic: detail.topic.slice(0, 512),
      mqttUsername: detail.mqttUsername?.slice(0, 512) ?? null,
      truncated: detail.clientId.length > 512 || detail.topic.length > 512 || (detail.mqttUsername?.length ?? 0) > 512,
    }));
  }
  if (omittedDeniedDetails > 0) {
    console.warn('[conn-monitor] denied publish details omitted', { phase, count: omittedDeniedDetails });
  }

  for (const observation of denied.values()) {
    try {
      await recordObservation(observation.mqttUsername, observation.claimedNodeId);
    } catch (error) {
      // One failed audit row must not replay the entire broker-log range.
      console.error('[conn-monitor] audit observation write failed:', (error as Error).message);
    }
  }
  if (connectionCount > 0 || deniedPublishCount > 0) {
    console.log('[conn-monitor] audit-only broker observations', {
      phase,
      connections: connectionCount,
      deniedPublishes: deniedPublishCount,
      retainedDeniedObservations: denied.size,
      droppedDeniedObservations: Math.max(0, deniedPublishCount - denied.size),
      omittedDeniedDetails,
    });
  }
  // Do not advance past a partial final line. The next poll re-reads it once
  // the broker appends its newline, including split UTF-8 client/topic text.
  return position;
}

export function startMqttConnectionMonitor(): void {
  if (monitorStopping || monitorTimer || retryTimer || monitorInFlight) return;
  if (!fs.existsSync(LOG_PATH)) {
    console.warn('[conn-monitor] log not found at', LOG_PATH, '— retrying in 30s');
    retryTimer = setTimeout(() => {
      retryTimer = null;
      startMqttConnectionMonitor();
    }, 30_000);
    retryTimer.unref();
    return;
  }

  let position = 0;
  let pollInFlight = false;
  async function init(): Promise<void> {
    const { size } = fs.statSync(LOG_PATH);
    const start = Math.max(0, size - HISTORICAL_SCAN_BYTES);
    if (start < size) position = await scanBrokerLogRange(LOG_PATH, start, size - 1, 'history');
    console.log('[conn-monitor] ready in audit-only mode; logs cannot grant ownership');
  }

  async function poll(): Promise<void> {
    try {
      const { size } = fs.statSync(LOG_PATH);
      if (size < position) position = 0;
      if (size > position) {
        position = await scanBrokerLogRange(LOG_PATH, position, size - 1, 'live');
      }
      pruneClientObservations();
    } catch {
      // Log may be unavailable briefly during rotation.
    }
  }

  const initialising = init().then(() => {
    if (monitorStopping) return;
    monitorTimer = setInterval(() => {
      if (pollInFlight) return;
      pollInFlight = true;
      const polling = poll()
        .catch((error: Error) => console.error('[conn-monitor] poll error:', error.message))
        .finally(() => {
          pollInFlight = false;
          if (monitorInFlight === polling) monitorInFlight = null;
        });
      monitorInFlight = polling;
    }, POLL_INTERVAL_MS);
    monitorTimer.unref();
  }).catch((error: Error) => {
    if (monitorStopping) return;
    console.error('[conn-monitor] init error:', error.message);
    retryTimer = setTimeout(() => {
      retryTimer = null;
      startMqttConnectionMonitor();
    }, 30_000);
    retryTimer.unref();
  }).finally(() => {
    if (monitorInFlight === initialising) monitorInFlight = null;
  });
  monitorInFlight = initialising;
}

export async function stopMqttConnectionMonitor(): Promise<void> {
  monitorStopping = true;
  if (monitorTimer) {
    clearInterval(monitorTimer);
    monitorTimer = null;
  }
  if (retryTimer) {
    clearTimeout(retryTimer);
    retryTimer = null;
  }
  await monitorInFlight;
  monitorInFlight = null;
}
