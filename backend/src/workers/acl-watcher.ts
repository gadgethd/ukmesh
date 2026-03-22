/**
 * acl-watcher — monitors Mosquitto container logs and automatically adds
 * new node IDs to the ACL when a known user's node is denied on publish.
 *
 * Only operates on users that already have an ACL block (no auto-provisioning
 * of unknown users). Safe to restart — replays recent log history on startup
 * to repopulate the clientId→username map.
 */
import Docker from 'dockerode';
import {
  getNodeIdsForUser,
  userExistsInAcl,
  nodeExistsInAcl,
  updateUserAclBlock,
  reloadMosquitto,
} from '../mqtt/aclManager.js';

const MOSQUITTO_CONTAINER_LABEL = process.env['MOSQUITTO_CONTAINER_NAME'] ?? 'mosquitto';

// "New client connected from X as <clientId> (p4, c1, k60, u'<username>')."
const CONNECT_RE = /New client connected from .+ as (\S+) \(.*u'([^']+)'\)/;

// "Denied PUBLISH from <clientId> (d0, q0, r0, m0, '<topic>', ..."
const DENIED_RE = /Denied PUBLISH from (\S+) \([^']*'([^']+)'/;

// topic: meshcore/{IATA}/{64-char nodeId}/{packets|status}
const TOPIC_NODE_RE = /^(?:meshcore|ukmesh)\/[A-Z0-9]+\/([A-F0-9]{64})\//i;

// clientId → MQTT username, populated from connection log events
const clientToUser = new Map<string, string>();

// Pending additions keyed by username, flushed after a short debounce
const pendingByUser = new Map<string, Set<string>>();
let flushTimer: ReturnType<typeof setTimeout> | null = null;

function scheduleDeniedNode(username: string, nodeId: string): void {
  const upper = nodeId.toUpperCase();

  if (nodeExistsInAcl(upper)) return; // already present

  if (!userExistsInAcl(username)) {
    console.log(`[acl-watcher] ignoring denied node for unknown user '${username}'`);
    return;
  }

  console.log(`[acl-watcher] queuing new node ${upper} for user '${username}'`);
  const set = pendingByUser.get(username) ?? new Set();
  set.add(upper);
  pendingByUser.set(username, set);

  if (flushTimer) return;
  flushTimer = setTimeout(() => {
    flushTimer = null;
    void flush();
  }, 2000);
}

async function flush(): Promise<void> {
  if (pendingByUser.size === 0) return;

  const snap = new Map(pendingByUser);
  pendingByUser.clear();

  let anyUpdated = false;
  for (const [username, newIds] of snap) {
    const existing = getNodeIdsForUser(username);
    const toAdd = [...newIds].filter((id) => !existing.map((e) => e.toUpperCase()).includes(id));
    if (toAdd.length === 0) continue;

    updateUserAclBlock(username, [...existing, ...toAdd]);
    console.log(`[acl-watcher] added to '${username}': ${toAdd.join(', ')}`);
    anyUpdated = true;
  }

  if (anyUpdated) {
    await reloadMosquitto();
  }
}

function handleLogLine(line: string): void {
  const connMatch = CONNECT_RE.exec(line);
  if (connMatch) {
    clientToUser.set(connMatch[1]!, connMatch[2]!);
    return;
  }

  const deniedMatch = DENIED_RE.exec(line);
  if (!deniedMatch) return;

  const clientId = deniedMatch[1]!;
  const topic    = deniedMatch[2]!;

  const username = clientToUser.get(clientId);
  if (!username) return; // haven't seen this client connect yet

  const topicMatch = TOPIC_NODE_RE.exec(topic);
  if (!topicMatch) return;

  scheduleDeniedNode(username, topicMatch[1]!);
}

async function streamLogs(): Promise<void> {
  const socketPath = process.env['DOCKER_SOCKET'] ?? '/var/run/docker.sock';
  const docker = new Docker({ socketPath });

  let containers: Docker.ContainerInfo[];
  try {
    containers = await docker.listContainers();
  } catch (err) {
    console.error('[acl-watcher] failed to list containers:', (err as Error).message);
    setTimeout(() => { void streamLogs(); }, 10_000);
    return;
  }

  const mc = containers.find((c) => c.Names.some((n) => n.includes(MOSQUITTO_CONTAINER_LABEL)));
  if (!mc) {
    console.error('[acl-watcher] mosquitto container not found, retrying in 10s');
    setTimeout(() => { void streamLogs(); }, 10_000);
    return;
  }

  const container = docker.getContainer(mc.Id);

  let stream: NodeJS.ReadableStream;
  try {
    stream = await container.logs({
      follow: true,
      stdout: true,
      stderr: true,
      tail: 500, // replay recent history to pre-populate clientToUser map
    }) as NodeJS.ReadableStream;
  } catch (err) {
    console.error('[acl-watcher] failed to attach log stream:', (err as Error).message);
    setTimeout(() => { void streamLogs(); }, 10_000);
    return;
  }

  console.log('[acl-watcher] attached to mosquitto log stream');

  // Docker multiplexed log stream has an 8-byte header per frame
  let buffer = Buffer.alloc(0);
  stream.on('data', (chunk: Buffer) => {
    buffer = Buffer.concat([buffer, chunk]);
    while (buffer.length >= 8) {
      const frameSize = buffer.readUInt32BE(4);
      if (buffer.length < 8 + frameSize) break;
      const frame = buffer.subarray(8, 8 + frameSize).toString('utf8');
      buffer = buffer.subarray(8 + frameSize);
      for (const line of frame.split('\n')) {
        const trimmed = line.trim();
        if (trimmed) handleLogLine(trimmed);
      }
    }
  });

  stream.on('error', (err: Error) => {
    console.error('[acl-watcher] stream error:', err.message);
  });

  stream.on('end', () => {
    console.log('[acl-watcher] log stream ended, reconnecting in 5s');
    setTimeout(() => { void streamLogs(); }, 5_000);
  });
}

void streamLogs();
