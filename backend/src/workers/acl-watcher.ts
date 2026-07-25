/**
 * Deprecated audit-only broker log watcher.
 *
 * Denied publishes and client identifiers are attacker-controlled observations,
 * not enrollment proof. This process intentionally performs no ACL mutation.
 */
import Docker from 'dockerode';
import { observeBrokerLogLine } from '../mqtt/connectionMonitor.js';

const MOSQUITTO_CONTAINER_LABEL = process.env['MOSQUITTO_CONTAINER_NAME'] ?? 'mosquitto';

export function handleAclAuditLogLine(line: string): void {
  const observation = observeBrokerLogLine(line);
  if (observation?.kind === 'denied-publish') {
    console.warn('[acl-audit] denied publish observed; no authorization change made', {
      mqttUsername: observation.mqttUsername,
      clientId: observation.clientId,
      claimedNodeId: observation.claimedNodeId,
    });
  }
}

async function streamLogs(): Promise<void> {
  const socketPath = process.env['DOCKER_SOCKET'] ?? '/var/run/docker.sock';
  const docker = new Docker({ socketPath });
  let containers: Docker.ContainerInfo[];
  try {
    containers = await docker.listContainers();
  } catch (err) {
    console.error('[acl-audit] failed to list containers:', (err as Error).message);
    setTimeout(() => { void streamLogs(); }, 10_000);
    return;
  }

  const broker = containers.find((container) =>
    container.Names.some((name) => name.includes(MOSQUITTO_CONTAINER_LABEL)));
  if (!broker) {
    console.error('[acl-audit] mosquitto container not found, retrying in 10s');
    setTimeout(() => { void streamLogs(); }, 10_000);
    return;
  }

  let stream: NodeJS.ReadableStream;
  try {
    stream = await docker.getContainer(broker.Id).logs({
      follow: true,
      stdout: true,
      stderr: true,
      tail: 500,
    }) as NodeJS.ReadableStream;
  } catch (err) {
    console.error('[acl-audit] failed to attach log stream:', (err as Error).message);
    setTimeout(() => { void streamLogs(); }, 10_000);
    return;
  }

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
        if (trimmed) handleAclAuditLogLine(trimmed);
      }
    }
  });
  stream.on('error', (err: Error) => console.error('[acl-audit] stream error:', err.message));
  stream.on('end', () => setTimeout(() => { void streamLogs(); }, 5_000));
}

if (process.env['ACL_AUDIT_WATCHER_ENABLED'] === '1') {
  void streamLogs();
} else {
  console.warn('[acl-audit] disabled; verified owner grants are the only ACL source');
}
