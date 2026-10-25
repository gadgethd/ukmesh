import fs from 'node:fs';
import { createInterface } from 'node:readline';
import { mqttConnectionMonitorErrorsTotal, mqttConnectionMonitorHealthy } from '../metrics.js';
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
const MAX_AUDIT_OBSERVATIONS_PER_SCAN = Math.min(
  65_536,
  Math.max(1, Number(process.env['MQTT_AUDIT_MAX_OBSERVATIONS_PER_SCAN'] ?? 4_096) || 4_096),
);

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

export type ConnectionMonitorStatus = {
  state: 'starting' | 'healthy' | 'error' | 'stopped';
  lastSuccessfulPollAt: string | null;
  lastError: string | null;
};

export function createMqttConnectionMonitor({
  logPath = LOG_PATH,
  files = fs,
  recordObservation = async (username: string, nodeId: string) => {
    const { recordUntrustedMqttNodeObservation } = await import('../db/ownerAuth.js');
    await recordUntrustedMqttNodeObservation(username, nodeId);
  },
  pollIntervalMs = POLL_INTERVAL_MS,
  retryIntervalMs = 30_000,
} : {
  logPath?: string;
  files?: Pick<typeof fs, 'statSync' | 'createReadStream'>;
  recordObservation?: (username: string, nodeId: string) => Promise<void>;
  pollIntervalMs?: number;
  retryIntervalMs?: number;
} = {}) {
  let monitorTimer: NodeJS.Timeout | null = null;
  let retryTimer: NodeJS.Timeout | null = null;
  let monitorInFlight: Promise<void> | null = null;
  let monitorStopping = false;
  let position = 0;
  let lastSuccessfulPollAt: string | null = null;
  let readError: string | null = null;
  let auditError: string | null = null;

  function status(): ConnectionMonitorStatus {
    const lastError = readError ?? auditError;
    return {
      state: monitorStopping ? 'stopped' : lastError ? 'error' : lastSuccessfulPollAt ? 'healthy' : 'starting',
      lastSuccessfulPollAt,
      lastError,
    };
  }

  function publishHealth(): void {
    mqttConnectionMonitorHealthy.set(status().state === 'healthy' ? 1 : 0);
  }

  function fail(phase: 'init' | 'poll' | 'audit', error: unknown): void {
    const code = (error as NodeJS.ErrnoException | null)?.code ?? 'UNKNOWN';
    const detail = `${phase}:${code}`;
    if (phase === 'audit') auditError = detail;
    else readError = detail;
    mqttConnectionMonitorErrorsTotal.inc({ phase });
    publishHealth();
    console.error('[conn-monitor] failure', { phase, code, logPath });
  }

  async function scanRange(start: number, end: number): Promise<void> {
    const denied = new Map<string, { mqttUsername: string; claimedNodeId: string }>();
    let connectionCount = 0;
    let deniedPublishCount = 0;
    await new Promise<void>((resolve, reject) => {
      const stream = files.createReadStream(logPath, { start, end });
      const reader = createInterface({ input: stream, crlfDelay: Infinity });
      reader.on('line', (line) => {
        const observation = observeBrokerLogLine(line);
        if (observation?.kind === 'connection') {
          connectionCount += 1;
        } else if (observation?.kind === 'denied-publish') {
          deniedPublishCount += 1;
          if (observation.mqttUsername && denied.size < MAX_AUDIT_OBSERVATIONS_PER_SCAN) {
            const key = `${observation.mqttUsername}\u0000${observation.claimedNodeId}`;
            denied.set(key, {
              mqttUsername: observation.mqttUsername,
              claimedNodeId: observation.claimedNodeId,
            });
          }
        }
      });
      reader.on('close', resolve);
      reader.on('error', reject);
      stream.on('error', reject);
    });

    let auditFailed = false;
    for (const observation of denied.values()) {
      try {
        await recordObservation(observation.mqttUsername, observation.claimedNodeId);
      } catch (error) {
        // One failed audit row must not replay the entire broker-log range.
        auditFailed = true;
        fail('audit', error);
      }
    }
    if (denied.size > 0 && !auditFailed) auditError = null;
    if (connectionCount > 0 || deniedPublishCount > 0) {
      console.log('[conn-monitor] audit-only broker observations', {
        connections: connectionCount,
        deniedPublishes: deniedPublishCount,
        retainedDeniedObservations: denied.size,
        droppedDeniedObservations: Math.max(0, deniedPublishCount - denied.size),
      });
    }
  }

  async function pollOnce(initial = false): Promise<void> {
    try {
      const { size } = files.statSync(logPath);
      if (initial) position = Math.max(0, size - HISTORICAL_SCAN_BYTES);
      if (size < position) position = 0;
      if (size > position) {
        await scanRange(position, size - 1);
        position = size;
      }
      pruneClientObservations();
      readError = null;
      lastSuccessfulPollAt = new Date().toISOString();
    } catch (error) {
      // Rotation, permission failures and missing mounts all degrade readiness.
      // Retain the cursor so a recovered read retries the failed range.
      fail(initial ? 'init' : 'poll', error);
    }
    publishHealth();
  }

  function start(): Promise<void> {
    if (monitorStopping || monitorTimer || retryTimer || monitorInFlight) return Promise.resolve();
    const initialising = pollOnce(true).then(() => {
      if (monitorStopping) return;
      if (readError) {
        retryTimer = setTimeout(() => {
          retryTimer = null;
          void start();
        }, retryIntervalMs);
        retryTimer.unref();
        return;
      }
      if (!auditError) console.log('[conn-monitor] ready in audit-only mode; logs cannot grant ownership');
      monitorTimer = setInterval(() => {
        if (monitorInFlight) return;
        const polling = pollOnce().finally(() => {
          if (monitorInFlight === polling) monitorInFlight = null;
        });
        monitorInFlight = polling;
      }, pollIntervalMs);
      monitorTimer.unref();
    }).finally(() => {
      if (monitorInFlight === initialising) monitorInFlight = null;
    });
    monitorInFlight = initialising;
    return initialising;
  }

  async function stop(): Promise<void> {
    monitorStopping = true;
    if (monitorTimer) clearInterval(monitorTimer);
    if (retryTimer) clearTimeout(retryTimer);
    monitorTimer = retryTimer = null;
    publishHealth();
    await monitorInFlight;
    monitorInFlight = null;
  }

  return { start, stop, status, pollOnce };
}

const monitor = createMqttConnectionMonitor();
export const getMqttConnectionMonitorStatus = monitor.status;
export function startMqttConnectionMonitor(): void { void monitor.start(); }
export const stopMqttConnectionMonitor = monitor.stop;
