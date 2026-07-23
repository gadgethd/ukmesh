import fs from 'node:fs';
import { createInterface } from 'node:readline';
import { addOwnerNodeForUsername, upsertMqttNodeLogin } from '../db/ownerAuth.js';
import { invalidateOwnerNodeIdCache } from '../owner/ownerAccess.js';
import { query } from '../db/index.js';
import {
  parseDeniedOwnerPublish,
  parseMeshcoreClientNodePrefix,
  parseMosquittoConnection,
} from './brokerLog.js';

const LOG_PATH = process.env['MOSQUITTO_LOG_PATH'] ?? '/mosquitto/log/mosquitto.log';
// Lower poll interval = newly published nodes are linked (and appear in the
// owner dashboard) sooner after their first packet. Reading only appended bytes
// keeps each tick cheap. Configurable so it can be tuned without a rebuild.
const POLL_INTERVAL_MS = Number(process.env['MQTT_CONN_MONITOR_POLL_MS'] ?? 2_000);
const HISTORICAL_SCAN_BYTES = 50_000_000; // last 50 MB on startup

// Client IDs are needed to associate a denied PUBLISH topic with its authenticated
// username. The topic supplies the exact public key; no prefix guessing is needed.
const clientToUsername = new Map<string, string>();

// Username + prefix pairs that couldn't be resolved yet (node not in nodes table).
// Retried on every poll tick so new nodes link automatically once they publish their first packet.
const pendingLinks = new Map<string, { nodePrefix: string; mqttUsername: string }>();

async function resolveNodeId(prefix: string): Promise<string | null> {
  if (prefix.length < 4) return null;
  const res = await query<{ node_id: string }>(
    `SELECT node_id FROM nodes WHERE node_id ILIKE $1 LIMIT 2`,
    [`${prefix}%`],
  );
  // Only proceed if unambiguous match
  return res.rows.length === 1 ? (res.rows[0]?.node_id ?? null) : null;
}

async function recordOwnerNode(mqttUsername: string, nodeId: string): Promise<void> {
  // Persist both the observed login and the dashboard mapping. Writing the owner
  // mapping here lets an existing dashboard session discover newly published
  // nodes on its next refresh, without requiring another login.
  await addOwnerNodeForUsername(mqttUsername, nodeId);
  await upsertMqttNodeLogin(mqttUsername, nodeId);
  // A newly linked node must show up in the owner dashboard promptly, so drop
  // any cached (pre-link) resolution for this username.
  invalidateOwnerNodeIdCache(mqttUsername);
}

async function resolveAndMaybeLink(mqttUsername: string, nodePrefix: string): Promise<boolean> {
  const nodeId = await resolveNodeId(nodePrefix);
  if (!nodeId) return false;
  await recordOwnerNode(mqttUsername, nodeId);
  return true;
}

async function retryPendingLinks(): Promise<void> {
  if (pendingLinks.size === 0) return;
  for (const [key, pending] of pendingLinks.entries()) {
    try {
      const resolved = await resolveAndMaybeLink(pending.mqttUsername, pending.nodePrefix);
      if (resolved) {
        pendingLinks.delete(key);
        console.log('[conn-monitor] resolved pending link:', pending.mqttUsername, pending.nodePrefix);
      }
    } catch (err) {
      console.error('[conn-monitor] retry pending error:', (err as Error).message);
    }
  }
}

// Collect unique (username, nodePrefix) pairs from a range, then resolve + upsert in batch.
// This avoids building a massive promise chain when scanning large historical log sections.
async function scanRange(start: number, end: number): Promise<void> {
  const seenPrefixes = new Map<string, { nodePrefix: string; mqttUsername: string }>();
  const exactTopicLinks = new Map<string, { nodeId: string; mqttUsername: string }>();

  await new Promise<void>((resolve, reject) => {
    const stream = fs.createReadStream(LOG_PATH, { start, end });
    const rl = createInterface({ input: stream, crlfDelay: Infinity });
    rl.on('line', (line) => {
      const connection = parseMosquittoConnection(line);
      if (connection) {
        clientToUsername.set(connection.clientId, connection.mqttUsername);
        const nodePrefix = parseMeshcoreClientNodePrefix(connection.clientId);
        if (nodePrefix && connection.mqttUsername !== 'backend') {
          seenPrefixes.set(`${connection.mqttUsername}:${nodePrefix}`, {
            nodePrefix,
            mqttUsername: connection.mqttUsername,
          });
        }
        return;
      }

      const denied = parseDeniedOwnerPublish(line);
      if (!denied) return;
      const mqttUsername = clientToUsername.get(denied.clientId);
      if (!mqttUsername || mqttUsername === 'backend') return;
      exactTopicLinks.set(`${mqttUsername}:${denied.nodeId}`, { nodeId: denied.nodeId, mqttUsername });
    });
    rl.on('close', resolve);
    rl.on('error', reject);
  });

  // A denied first publish is the authoritative keyless-signup path: Mosquitto
  // gives us the authenticated client ID and the exact public key in its topic.
  for (const { nodeId, mqttUsername } of exactTopicLinks.values()) {
    try {
      await recordOwnerNode(mqttUsername, nodeId);
      console.log('[conn-monitor] learned owner node from MQTT topic:', mqttUsername, nodeId);
    } catch (err) {
      console.error('[conn-monitor] exact topic link error:', (err as Error).message);
    }
  }

  // Keep the mctomqtt client-ID-prefix path as a fallback for accounts that were
  // provisioned with an explicit ACL and therefore never produce a denial.
  for (const { nodePrefix, mqttUsername } of seenPrefixes.values()) {
    try {
      const resolved = await resolveAndMaybeLink(mqttUsername, nodePrefix);
      if (!resolved) {
        pendingLinks.set(`${mqttUsername}:${nodePrefix}`, { nodePrefix, mqttUsername });
      }
    } catch (err) {
      console.error('[conn-monitor] processLine error:', (err as Error).message);
    }
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
    // Set position before scanning so concurrent poll() calls skip this range
    position = size;
    if (start < size) {
      console.log('[conn-monitor] scanning historical log entries...');
      await scanRange(start, size - 1);
    }
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
    await retryPendingLinks();
  }

  init().catch((err: Error) => console.error('[conn-monitor] init error:', err.message));
  setInterval(() => poll().catch((err: Error) => console.error('[conn-monitor] poll error:', err.message)), POLL_INTERVAL_MS);
  console.log('[conn-monitor] started, monitoring', LOG_PATH);
}
