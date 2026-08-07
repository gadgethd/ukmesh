export type MosquittoConnection = {
  clientId: string;
  mqttUsername: string;
};

export type DeniedOwnerPublish = {
  clientId: string;
  nodeId: string;
};

// Mosquitto 2.x: "New client connected from X as <clientId> (..., u'<username>')."
const CONNECT_RE = /New client connected from .+ as (\S+) \([^)]*u'([^']+)'\)/;

// Mosquitto 2.x: "Denied PUBLISH from <clientId> (..., '<topic>', ...)"
const DENIED_PUBLISH_RE = /Denied PUBLISH from (\S+) \([^']*'([^']+)'/;

// mctomqtt client IDs include a stable public-key prefix.
const MESHCORE_CLIENT_RE = /^meshcore_(?:client_)?([0-9A-F]{4,64})_\d+$/i;

export function parseMosquittoConnection(line: string): MosquittoConnection | null {
  const match = CONNECT_RE.exec(line);
  const clientId = match?.[1]?.trim();
  const mqttUsername = match?.[2]?.trim();
  return clientId && mqttUsername ? { clientId, mqttUsername } : null;
}

export function parseMeshcoreClientNodePrefix(clientId: string): string | null {
  return MESHCORE_CLIENT_RE.exec(clientId)?.[1]?.toUpperCase() ?? null;
}

export function parseOwnerNodeTopic(topic: string): string | null {
  const parts = topic.split('/');
  if (parts.length !== 4) return null;

  const prefix = parts[0]?.trim().toLowerCase();
  if (prefix !== 'meshcore' && prefix !== 'ukmesh') return null;

  const iata = parts[1]?.trim().toUpperCase() ?? '';
  if (!/^[A-Z0-9]{2,8}$/.test(iata)) return null;

  const nodeId = parts[2]?.trim().toUpperCase() ?? '';
  if (!/^[0-9A-F]{64}$/.test(nodeId)) return null;

  const suffix = parts[3]?.trim().toLowerCase();
  return suffix === 'packets' || suffix === 'status' || suffix === 'neighbors' || suffix === 'neighbours' ? nodeId : null;
}

export function parseDeniedOwnerPublish(line: string): DeniedOwnerPublish | null {
  const match = DENIED_PUBLISH_RE.exec(line);
  const clientId = match?.[1]?.trim();
  const nodeId = match?.[2] ? parseOwnerNodeTopic(match[2]) : null;
  return clientId && nodeId ? { clientId, nodeId } : null;
}
