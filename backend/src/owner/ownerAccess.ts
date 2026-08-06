import mqtt from 'mqtt';
import { createHash, randomBytes } from 'node:crypto';
import { BoundedTtlMap } from '../cache/boundedTtlMap.js';
import { getOwnerNodeIdsForUsername } from '../db/ownerAuth.js';
import { query } from '../db/index.js';
import { getNodeIdsForUserInAcl, readAclFile } from '../mqtt/aclManager.js';
import { reconcileOwnerAuthorization } from './ownerAclReconciler.js';
import { groupOwnerNodes, type OwnerDashboardRow } from './ownerDashboard.js';
import { parseOwnerGrantConfig } from './ownerGrantConfig.js';

function normalizeNodeIds(nodeIds: string[]): string[] {
  return Array.from(new Set(
    nodeIds
      .map((nodeId) => nodeId.trim().toUpperCase())
      .filter((nodeId) => /^[0-9A-F]{64}$/.test(nodeId)),
  )).sort();
}

function configuredOwnerNodeIds(mqttUsername: string): string[] {
  const raw = String(process.env['OWNER_MQTT_USERNAME_MAP'] ?? '');
  const map = new Map<string, string[]>();
  for (const grant of parseOwnerGrantConfig(raw)) {
    map.set(grant.mqttUsername, [...(map.get(grant.mqttUsername) ?? []), grant.nodeId]);
  }
  return normalizeNodeIds(map.get(mqttUsername) ?? []);
}

const OWNER_ACCESS_CACHE_TTL_MS = Number(process.env['OWNER_ACCESS_CACHE_TTL_MS'] ?? 30_000);
const ownerNodeIdCache = new BoundedTtlMap<string, { ts: number; nodeIds: string[] }>({
  name: 'owner_nodes',
  maxEntries: 2048,
  maxWeight: 4 * 1024 * 1024,
  ttlMs: OWNER_ACCESS_CACHE_TTL_MS,
});
const ownerNodeIdInflight = new Map<string, Promise<string[]>>();
const OWNER_NODE_ID_INFLIGHT_MAX = 128;

export function invalidateOwnerNodeIdCache(mqttUsername: string): void {
  ownerNodeIdCache.delete(mqttUsername.trim());
}

export async function resolveOwnerNodeIds(mqttUsername: string): Promise<string[]> {
  const cacheKey = mqttUsername.trim();
  const cached = ownerNodeIdCache.get(cacheKey);
  if (cached && Date.now() - cached.ts < OWNER_ACCESS_CACHE_TTL_MS) {
    return [...cached.nodeIds];
  }
  const existing = ownerNodeIdInflight.get(cacheKey);
  if (existing) return [...await existing];
  if (ownerNodeIdInflight.size >= OWNER_NODE_ID_INFLIGHT_MAX) {
    throw new Error('OWNER_ACCESS_OVERLOADED');
  }

  const load = async (): Promise<string[]> => {
    const verified = normalizeNodeIds(await getOwnerNodeIdsForUsername(cacheKey));
    const mode = String(process.env['OWNER_AUTHORIZATION_MODE'] ?? 'shadow').trim().toLowerCase();
    if (mode === 'enforce') return verified;
    if (mode !== 'shadow') throw new Error(`INVALID_OWNER_AUTHORIZATION_MODE:${mode}`);
    if (verified.length > 0) return verified;

    // Transitional compatibility is deliberately read-only: the current ACL or
    // operator config may keep an existing owner working while inventory runs,
    // but broker logs can never create a new grant.
    const configured = configuredOwnerNodeIds(cacheKey);
    if (configured.length > 0) return configured;
    try {
      const legacy = normalizeNodeIds(getNodeIdsForUserInAcl(readAclFile(), cacheKey));
      if (legacy.length > 0) {
        console.warn('[owner-auth] shadow-mode legacy ACL authorization used', { mqttUsername: cacheKey });
      }
      return legacy;
    } catch {
      return [];
    }
  };
  const tracked = load()
    .then((nodeIds) => {
      ownerNodeIdCache.set(cacheKey, { ts: Date.now(), nodeIds });
      return nodeIds;
    })
    .finally(() => {
      if (ownerNodeIdInflight.get(cacheKey) === tracked) ownerNodeIdInflight.delete(cacheKey);
    });
  ownerNodeIdInflight.set(cacheKey, tracked);
  return [...await tracked];
}

export async function autoLinkOwnerNodeIds(mqttUsername: string): Promise<string[]> {
  await reconcileOwnerAuthorization();
  invalidateOwnerNodeIdCache(mqttUsername);
  return resolveOwnerNodeIds(mqttUsername);
}

// Verifying credentials means opening a full MQTT connection to the broker
// (5s connect timeout, 6s hard cap) — the dominant cost of a login. Cache
// successful verifications per username for a short window so repeat logins
// (and the 30-day cookie re-auth flows) skip the broker round-trip. The cache
// key is a hash of the exact username+password, so a changed/rotated password
// misses and re-verifies. Only positive results are cached.
const AUTH_CACHE_TTL_MS = Number(process.env['OWNER_AUTH_CACHE_TTL_MS'] ?? 5 * 60_000);
const authCache = new BoundedTtlMap<string, { credentialHash: string; ts: number }>({
  name: 'owner_auth',
  maxEntries: Number(process.env['OWNER_AUTH_CACHE_MAX_ENTRIES'] ?? 512),
  maxWeight: 2 * 1024 * 1024,
  ttlMs: AUTH_CACHE_TTL_MS,
});

function credentialHash(mqttUsername: string, mqttPassword: string): string {
  return createHash('sha256').update(`${mqttUsername}\u0000${mqttPassword}`).digest('hex');
}

export function verifyMqttCredentials(mqttUsername: string, mqttPassword: string): Promise<boolean> {
  const hash = credentialHash(mqttUsername, mqttPassword);
  const cached = authCache.get(mqttUsername);
  if (cached && cached.credentialHash === hash && Date.now() - cached.ts < AUTH_CACHE_TTL_MS) {
    return Promise.resolve(true);
  }
  return verifyMqttCredentialsViaBroker(mqttUsername, mqttPassword).then((ok) => {
    if (ok) {
      authCache.set(mqttUsername, { credentialHash: hash, ts: Date.now() });
    } else if (cached && cached.credentialHash === hash) {
      // Same credential that previously worked now rejected (revoked) — drop it.
      authCache.delete(mqttUsername);
    }
    return ok;
  });
}

function verifyMqttCredentialsViaBroker(mqttUsername: string, mqttPassword: string): Promise<boolean> {
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
  const ownedNodes = await query<OwnerDashboardRow>(
    `SELECT n.node_id AS canonical_id,
            n.name,
            n.network,
            n.last_seen::text,
            n.advert_count,
            n.lat,
            n.lon,
            n.iata,
            n.role,
            n.identity_source_ids AS members
       FROM node_identity_nodes n
      WHERE n.node_id IN (
        SELECT meshcore_canonical_node_id(source_node_id)
          FROM unnest($1::text[]) AS source(source_node_id)
      )
      ORDER BY n.last_seen DESC NULLS LAST`,
    [nodeIds],
  );

  return { nodes: groupOwnerNodes(ownedNodes.rows, nodeIds) };
}
