import mqtt from 'mqtt';
import { randomBytes } from 'node:crypto';
import { Redis } from 'ioredis';
import { BoundedTtlMap } from '../cache/boundedTtlMap.js';
import { getOwnerNodeIdsForUsername } from '../db/ownerAuth.js';
import { query } from '../db/index.js';
import { getRedisConnectionOptions, getRedisUrl } from '../platform/config/redis.js';
import { reconcileOwnerAuthorization } from './ownerAclReconciler.js';
import { groupOwnerNodes, type OwnerDashboardRow } from './ownerDashboard.js';
import { createMqttCredentialVerifier } from './mqttCredentialVerifier.js';

function normalizeNodeIds(nodeIds: string[]): string[] {
  return Array.from(new Set(
    nodeIds
      .map((nodeId) => nodeId.trim().toUpperCase())
      .filter((nodeId) => /^[0-9A-F]{64}$/.test(nodeId)),
  )).sort();
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

// ---- seam for unit tests (default = production implementation) -----------
// The verified-grant reader is injectable so the authority rules can be proven
// without Postgres. Only this module can replace it; every production caller
// goes through the database reader.
type VerifiedOwnerGrantSource = (mqttUsername: string) => Promise<string[]>;
let verifiedOwnerGrantSource: VerifiedOwnerGrantSource = getOwnerNodeIdsForUsername;

export function __setOwnerGrantSourceForTests(source: VerifiedOwnerGrantSource | null): void {
  verifiedOwnerGrantSource = source ?? getOwnerNodeIdsForUsername;
  ownerNodeIdCache.clear();
  ownerNodeIdInflight.clear();
}

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
    const mode = String(process.env['OWNER_AUTHORIZATION_MODE'] ?? 'enforce').trim().toLowerCase();
    if (mode !== 'enforce' && mode !== 'shadow') {
      throw new Error(`INVALID_OWNER_AUTHORIZATION_MODE:${mode}`);
    }

    // Owner-portal authority is only ever a live, non-revoked, operator-verified
    // grant from the owner-auth database (getOwnerNodeIdsForUsername filters
    // verified_at/revoked_at/verification_method). OWNER_MQTT_USERNAME_MAP and
    // broker ACL stanzas are WRITE scope: they let a credential publish, they do
    // not establish who owns a node. The old shadow-mode fallback rebuilt
    // authority from those sources whenever the verified list was empty, which
    // turned broker write access into portal authority and survived revocation.
    // An empty verified list is therefore final in EVERY mode, shadow included.
    return normalizeNodeIds(await verifiedOwnerGrantSource(cacheKey));
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
// Owner sessions are stateless signed cookies with a 30-day TTL. To make
// password revocation effective immediately, every session records the
// credential generation that minted it. When the broker rejects a previously
// valid password (the only signal the backend has that a password changed),
// the generation is bumped in Redis and all older sessions stop validating.
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

/** The credential generation could not be read; callers must treat every session as unverified. */
export class OwnerCredentialGenerationUnavailableError extends Error {
  constructor(options: { cause?: unknown } = {}) {
    super('OWNER_CREDENTIAL_GENERATION_UNAVAILABLE', options);
    this.name = 'OwnerCredentialGenerationUnavailableError';
  }
}

/**
 * Current credential generation for a username (0 when never minted).
 *
 * Fails CLOSED: when Redis cannot answer, this throws instead of returning 0.
 * Returning 0 would compare equal to every session minted before the first
 * rotation, silently bypassing revocation for the whole outage window.
 */
export async function getOwnerCredentialGeneration(mqttUsername: string): Promise<number> {
  let value: string | null;
  try {
    value = await getCredGenRedis().get(OWNER_CRED_GEN_KEY(mqttUsername));
  } catch (error) {
    console.error(
      '[owner-credgen] read failed; refusing to validate owner sessions',
      error instanceof Error ? error.message : error,
    );
    throw new OwnerCredentialGenerationUnavailableError({ cause: error });
  }
  if (value === null) return 0;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    console.error('[owner-credgen] unreadable generation value; refusing to validate owner sessions');
    throw new OwnerCredentialGenerationUnavailableError();
  }
  return parsed;
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
