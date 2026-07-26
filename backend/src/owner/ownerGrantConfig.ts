import { createHash } from 'node:crypto';

export type ConfiguredOwnerGrant = {
  mqttUsername: string;
  nodeId: string;
};

const USERNAME_RE = /^[A-Za-z0-9_.@-]{1,128}$/;
const NODE_ID_RE = /^[0-9A-F]{64}$/;

export function parseOwnerGrantConfig(raw: string): ConfiguredOwnerGrant[] {
  const grants: ConfiguredOwnerGrant[] = [];
  const seen = new Set<string>();
  for (const entry of raw.split(',')) {
    const trimmed = entry.trim();
    if (!trimmed) continue;
    const separator = trimmed.indexOf('=');
    if (separator < 1) throw new Error(`INVALID_OWNER_GRANT_CONFIG_ENTRY:${trimmed}`);
    const mqttUsername = trimmed.slice(0, separator).trim();
    if (!USERNAME_RE.test(mqttUsername)) throw new Error(`INVALID_OWNER_GRANT_USERNAME:${mqttUsername}`);
    const rawNodeIds = trimmed.slice(separator + 1).split('|');
    if (rawNodeIds.length < 1) throw new Error(`EMPTY_OWNER_GRANT:${mqttUsername}`);
    for (const rawNodeId of rawNodeIds) {
      const nodeId = rawNodeId.trim().toUpperCase();
      if (!NODE_ID_RE.test(nodeId)) throw new Error(`INVALID_OWNER_GRANT_NODE:${mqttUsername}`);
      const key = `${mqttUsername}\0${nodeId}`;
      if (seen.has(key)) continue;
      seen.add(key);
      grants.push({ mqttUsername, nodeId });
    }
  }
  return grants.sort((a, b) =>
    a.mqttUsername.localeCompare(b.mqttUsername) || a.nodeId.localeCompare(b.nodeId));
}

export function ownerGrantConfigGeneration(grants: ConfiguredOwnerGrant[]): string {
  return createHash('sha256').update(JSON.stringify(grants)).digest('hex');
}
