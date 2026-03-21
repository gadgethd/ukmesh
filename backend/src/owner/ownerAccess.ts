import mqtt from 'mqtt';
import { randomBytes } from 'node:crypto';
import { addOwnerNodeForUsername, getBestNodeForMqttUsername, getMappedOwnerNodeIds, getOwnerNodeIdsForUsername } from '../db/ownerAuth.js';
import { query } from '../db/index.js';

function parseOwnerMqttUsernameMap(): Map<string, string[]> {
  const raw = String(process.env['OWNER_MQTT_USERNAME_MAP'] ?? '').trim();
  const map = new Map<string, string[]>();
  if (!raw) return map;

  for (const entry of raw.split(',')) {
    const trimmed = entry.trim();
    if (!trimmed) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx <= 0) continue;
    const username = trimmed.slice(0, eqIdx).trim();
    const rawNodes = trimmed.slice(eqIdx + 1).trim();
    if (!username || !rawNodes) continue;
    const nodeIds = rawNodes
      .split('|')
      .map((nodeId) => nodeId.trim().toLowerCase())
      .filter((nodeId) => /^[0-9a-f]{64}$/.test(nodeId));
    if (nodeIds.length < 1) continue;
    map.set(username, Array.from(new Set(nodeIds)));
  }
  return map;
}

export async function resolveOwnerNodeIds(mqttUsername: string): Promise<string[]> {
  const databaseNodeIds = await getOwnerNodeIdsForUsername(mqttUsername);
  if (databaseNodeIds.length > 0) return databaseNodeIds;
  const legacyMap = parseOwnerMqttUsernameMap();
  return legacyMap.get(mqttUsername) ?? [];
}

export async function autoLinkOwnerNodeIds(mqttUsername: string): Promise<string[]> {
  const existing = await resolveOwnerNodeIds(mqttUsername);
  if (existing.length > 0) return existing;

  const nodeId = await getBestNodeForMqttUsername(mqttUsername);
  if (!nodeId) return [];

  await addOwnerNodeForUsername(mqttUsername, nodeId);
  return [nodeId];
}

export async function listMappedOwnerNodeIds(): Promise<string[]> {
  return getMappedOwnerNodeIds();
}

export function verifyMqttCredentials(mqttUsername: string, mqttPassword: string): Promise<boolean> {
  const brokerUrl = String(process.env['MQTT_BROKER_URL'] ?? 'ws://mosquitto:9001');
  const clientId = `owner-auth-${randomBytes(6).toString('hex')}`;
  const client = mqtt.connect(brokerUrl, {
    username: mqttUsername,
    password: mqttPassword,
    reconnectPeriod: 0,
    connectTimeout: 5_000,
    clean: true,
    clientId,
  });

  return new Promise((resolve) => {
    let done = false;
    const finish = (ok: boolean) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      client.removeAllListeners();
      client.end(true);
      resolve(ok);
    };
    const timer = setTimeout(() => finish(false), 6_000);
    client.once('connect', () => finish(true));
    client.once('error', () => finish(false));
    client.once('close', () => finish(false));
  });
}

export async function buildOwnerDashboard(nodeIds: string[]) {
  if (nodeIds.length < 1) {
    return {
      nodes: [],
      totals: {
        ownedNodes: 0,
        packets24h: 0,
        packets7d: 0,
        packetsReceived24h: 0,
      },
      roadmap: [
        'Per-node packet history for owner nodes',
        'Advert and heartbeat trend views',
        'RSSI and SNR trend views from observer reports',
        'Node placement planner (coming next)',
      ],
    };
  }

  const [ownedNodes, packetSummary, rxSummary] = await Promise.all([
    query<{
      node_id: string;
      name: string | null;
      network: string;
      last_seen: string | null;
      advert_count: number | null;
      lat: number | null;
      lon: number | null;
      iata: string | null;
    }>(
      `SELECT node_id, name, network, last_seen, advert_count, lat, lon, iata
       FROM nodes
       WHERE node_id = ANY($1::text[])
       ORDER BY last_seen DESC NULLS LAST`,
      [nodeIds],
    ),
    query<{ packets_24h: number; packets_7d: number }>(
      `SELECT
         COUNT(*) FILTER (WHERE time > NOW() - INTERVAL '24 hours')::int AS packets_24h,
         COUNT(*) FILTER (WHERE time > NOW() - INTERVAL '7 days')::int AS packets_7d
       FROM packets
       WHERE src_node_id = ANY($1::text[])`,
      [nodeIds],
    ),
    query<{ packets_24h: number }>(
      `SELECT
         COUNT(*) FILTER (WHERE time > NOW() - INTERVAL '24 hours')::int AS packets_24h
       FROM packets
       WHERE rx_node_id = ANY($1::text[])`,
      [nodeIds],
    ),
  ]);

  return {
    nodes: ownedNodes.rows.map((row) => ({
      ...row,
      last_seen: row.last_seen ? new Date(row.last_seen).toISOString() : null,
      advert_count: Number(row.advert_count ?? 0),
    })),
    totals: {
      ownedNodes: ownedNodes.rows.length,
      packets24h: Number(packetSummary.rows[0]?.packets_24h ?? 0),
      packets7d: Number(packetSummary.rows[0]?.packets_7d ?? 0),
      packetsReceived24h: Number(rxSummary.rows[0]?.packets_24h ?? 0),
    },
    roadmap: [
      'Per-node packet history for owner nodes',
      'Advert and heartbeat trend views',
      'RSSI and SNR trend views from observer reports',
      'Node placement planner (coming next)',
    ],
  };
}
