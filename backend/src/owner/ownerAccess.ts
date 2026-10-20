import mqtt from 'mqtt';
import { randomBytes } from 'node:crypto';
import { Redis } from 'ioredis';
import { BoundedTtlMap } from '../cache/boundedTtlMap.js';
import { getOwnerNodeIdsForUsername } from '../db/ownerAuth.js';
import { query } from '../db/index.js';
import { getNodeIdsForUserInAcl, readAclFile } from '../mqtt/aclManager.js';
import { getRedisConnectionOptions, getRedisUrl } from '../platform/config/redis.js';
import { reconcileOwnerAuthorization } from './ownerAclReconciler.js';
import { groupOwnerNodes, type OwnerDashboardRow } from './ownerDashboard.js';
import { parseOwnerGrantConfig } from './ownerGrantConfig.js';
import { createMqttCredentialVerifier } from './mqttCredentialVerifier.js';

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

export const verifyMqttCredentials = createMqttCredentialVerifier(verifyMqttCredentialsViaBroker);

// ---- Credential generation (BUG-010) ----
// Owner sessions record the credential generation that minted them. The
// operator revocation command bumps this Redis value after a password reset,
// and every authenticated owner request compares it with the cookie generation.
const OWNER_CRED_GEN_KEY = (username: string) => `owner:credential-gen:${username}`;
const OWNER_CRED_GEN_TTL_MS = 90 * 24 * 60 * 60 * 1000;

let credGenRedis: Redis | null = null;

function getCredGenRedis(): Redis {
  if (!credGenRedis) {
    credGenRedis = new Redis(getRedisUrl(), getRedisConnectionOptions());
    credGenRedis.on('error', (e: Error) => console.error('[redis/owner-credgen] error', e.message));
  }
  return credGenRedis;
}

/** Current credential generation for a username (0 when never minted). */
export async function getOwnerCredentialGeneration(mqttUsername: string): Promise<number> {
  try {
    const value = await getCredGenRedis().get(OWNER_CRED_GEN_KEY(mqttUsername));
    if (value === null) return 0;
    const parsed = Number(value);
    if (!Number.isSafeInteger(parsed) || parsed < 0) {
      throw new Error('INVALID_OWNER_CREDENTIAL_GENERATION');
    }
    return parsed;
  } catch (error) {
    console.error('[owner-credgen] read failed', error instanceof Error ? error.message : error);
    throw new Error('OWNER_CREDENTIAL_GENERATION_UNAVAILABLE', { cause: error });
  }
}

/**
 * Bump the credential generation, invalidating every session minted under an
 * older password. Errors propagate so an operator reset cannot report success
 * unless revocation was recorded.
 */
export async function bumpOwnerCredentialGeneration(mqttUsername: string): Promise<void> {
  const redis = getCredGenRedis();
  const key = OWNER_CRED_GEN_KEY(mqttUsername);
  await redis.incr(key);
  await redis.pexpire(key, OWNER_CRED_GEN_TTL_MS);
}

export async function closeOwnerCredentialGenerationClient(): Promise<void> {
  if (!credGenRedis) return;
  const client = credGenRedis;
  credGenRedis = null;
  await client.quit();
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
