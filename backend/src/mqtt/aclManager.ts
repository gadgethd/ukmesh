import fs from 'node:fs';
import path from 'node:path';
import Docker from 'dockerode';

const ACL_PATH = process.env['MOSQUITTO_ACL_PATH'] ?? '/mosquitto/config/acl';
const MOSQUITTO_CONTAINER_LABEL = process.env['MOSQUITTO_CONTAINER_NAME'] ?? 'mosquitto';

function buildUserBlock(mqttUsername: string, nodeIds: string[]): string {
  const lines = [`user ${mqttUsername}`];
  for (const nodeId of nodeIds) {
    lines.push(`topic write meshcore/+/${nodeId}/packets`);
    lines.push(`topic write meshcore/+/${nodeId}/status`);
  }
  return lines.join('\n');
}

export function getNodeIdsForUser(mqttUsername: string): string[] {
  let content: string;
  try {
    content = fs.readFileSync(ACL_PATH, 'utf8');
  } catch {
    return [];
  }
  const lines = content.split('\n');
  const userLine = `user ${mqttUsername}`.toLowerCase();
  const userIdx = lines.findIndex((l) => l.trim().toLowerCase() === userLine);
  if (userIdx === -1) return [];

  const nodeIds: string[] = [];
  for (let i = userIdx + 1; i < lines.length; i++) {
    const trimmed = lines[i]?.trim() ?? '';
    if (trimmed === '' || trimmed.toLowerCase().startsWith('user ') || trimmed.startsWith('#')) break;
    const m = trimmed.match(/topic write .+\/([A-F0-9]{64})\//i);
    if (m) {
      const id = m[1]!.toUpperCase();
      if (!nodeIds.includes(id)) nodeIds.push(id);
    }
  }
  return nodeIds;
}

export function userExistsInAcl(mqttUsername: string): boolean {
  let content: string;
  try {
    content = fs.readFileSync(ACL_PATH, 'utf8');
  } catch {
    return false;
  }
  return new RegExp(`^user ${mqttUsername}$`, 'im').test(content);
}

export function nodeExistsInAcl(nodeId: string): boolean {
  let content: string;
  try {
    content = fs.readFileSync(ACL_PATH, 'utf8');
  } catch {
    return false;
  }
  return content.toUpperCase().includes(nodeId.toUpperCase());
}

export function updateUserAclBlock(mqttUsername: string, nodeIds: string[]): void {
  if (nodeIds.length === 0) return;

  let content: string;
  try {
    content = fs.readFileSync(ACL_PATH, 'utf8');
  } catch (err) {
    console.error('[acl-manager] Failed to read ACL file:', (err as Error).message);
    return;
  }

  const lines = content.split('\n');
  const userLine = `user ${mqttUsername}`;
  const userIdx = lines.findIndex((l) => l.trim().toLowerCase() === userLine.toLowerCase());

  let newLines: string[];
  if (userIdx === -1) {
    // Append new block at end, ensuring file ends with a blank line separator
    const trimmed = content.trimEnd();
    const newContent = `${trimmed}\n\n${buildUserBlock(mqttUsername, nodeIds)}\n`;
    newLines = newContent.split('\n');
  } else {
    // Find end of this user's block (next blank line or next `user ` line or EOF)
    let endIdx = userIdx + 1;
    while (endIdx < lines.length) {
      const trimmed = lines[endIdx]?.trim() ?? '';
      if (trimmed === '' || trimmed.toLowerCase().startsWith('user ') || trimmed.startsWith('#')) break;
      endIdx++;
    }
    const newBlock = buildUserBlock(mqttUsername, nodeIds).split('\n');
    newLines = [...lines.slice(0, userIdx), ...newBlock, ...lines.slice(endIdx)];
  }

  const newContent = newLines.join('\n');
  const tmpPath = `${ACL_PATH}.tmp`;
  try {
    fs.writeFileSync(tmpPath, newContent, 'utf8');
    fs.renameSync(tmpPath, ACL_PATH);
    console.log(`[acl-manager] Updated ACL block for ${mqttUsername} (${nodeIds.length} node(s))`);
  } catch (err) {
    console.error('[acl-manager] Failed to write ACL file:', (err as Error).message);
    try { fs.unlinkSync(tmpPath); } catch { /* ignore */ }
  }
}

export async function reloadMosquitto(): Promise<void> {
  const socketPath = process.env['DOCKER_SOCKET'] ?? '/var/run/docker.sock';
  if (!fs.existsSync(socketPath)) {
    console.warn('[acl-manager] Docker socket not found at', socketPath, '— skipping Mosquitto reload');
    return;
  }
  try {
    const docker = new Docker({ socketPath });
    const containers = await docker.listContainers();
    const mosquittoContainer = containers.find((c) =>
      c.Names.some((n) => n.includes(MOSQUITTO_CONTAINER_LABEL))
    );
    if (!mosquittoContainer) {
      console.warn('[acl-manager] Mosquitto container not found — skipping reload');
      return;
    }
    const container = docker.getContainer(mosquittoContainer.Id);
    await container.kill({ signal: 'SIGHUP' });
    console.log('[acl-manager] Sent SIGHUP to Mosquitto container');
  } catch (err) {
    console.error('[acl-manager] Failed to reload Mosquitto:', (err as Error).message);
  }
}
