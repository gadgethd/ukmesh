import { Redis } from 'ioredis';
import { query } from '../db/index.js';
import type { NodeRow } from './renderer.js';

const TILE_SNAPSHOT_NODE_KEY_PREFIX = 'meshcore:tile_snapshot:node:';
const TILE_SNAPSHOT_IDS_ALL_KEY = 'meshcore:tile_snapshot:ids:all';
const TILE_SNAPSHOT_IDS_NETWORK_PREFIX = 'meshcore:tile_snapshot:ids:network:';
const TILE_SNAPSHOT_META_KEY = 'meshcore:tile_snapshot:meta';

type SnapshotNode = NodeRow & { network?: string | null };

let redisClient: Redis | null = null;

function redis(): Redis {
  if (!redisClient) {
    const redisUrl = process.env['REDIS_URL'] ?? 'redis://redis:6379';
    redisClient = new Redis(redisUrl);
    redisClient.on('error', (err) => console.error('[tile-snapshot] redis error', err.message));
  }
  return redisClient;
}

function nodeKey(nodeId: string): string {
  return `${TILE_SNAPSHOT_NODE_KEY_PREFIX}${nodeId}`;
}

function networkIdsKey(network: string): string {
  return `${TILE_SNAPSHOT_IDS_NETWORK_PREFIX}${network}`;
}

function toNullableString(value: unknown): string | null {
  if (value == null) return null;
  const text = String(value).trim();
  return text === '' ? null : text;
}

function toNullableNumber(value: unknown): number | null {
  if (value == null || value === '') return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function toBoolean(value: unknown, fallback = false): boolean {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (normalized === 'true' || normalized === '1' || normalized === 't') return true;
    if (normalized === 'false' || normalized === '0' || normalized === 'f') return false;
  }
  return fallback;
}

function normalizeSnapshotNode(value: Partial<SnapshotNode> & { node_id?: unknown; last_seen?: unknown }): SnapshotNode | null {
  const nodeId = toNullableString(value.node_id);
  const lastSeen = toNullableString(value.last_seen);
  if (!nodeId || !lastSeen) return null;
  return {
    node_id: nodeId,
    name: toNullableString(value.name),
    lat: toNullableNumber(value.lat),
    lon: toNullableNumber(value.lon),
    role: toNullableNumber(value.role),
    last_seen: lastSeen,
    is_online: toBoolean(value.is_online, true),
    network: toNullableString(value.network),
  };
}

function parseSnapshotNode(raw: string | null): SnapshotNode | null {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as Partial<SnapshotNode>;
    return normalizeSnapshotNode(parsed);
  } catch {
    return null;
  }
}

export async function getTileSnapshotNodes(network?: string): Promise<NodeRow[]> {
  const r = redis();
  const ids = await r.smembers(network ? networkIdsKey(network) : TILE_SNAPSHOT_IDS_ALL_KEY);
  if (ids.length < 1) return [];
  const rows = await r.mget(ids.map((id) => nodeKey(id)));
  return rows
    .map(parseSnapshotNode)
    .filter((node): node is SnapshotNode => Boolean(node))
    .sort((a, b) => Date.parse(b.last_seen) - Date.parse(a.last_seen));
}

export async function rebuildTileSnapshotFromDb(): Promise<number> {
  const res = await query<SnapshotNode>(
    `SELECT node_id, name, lat, lon, role, last_seen::text AS last_seen, is_online, network
     FROM nodes
     WHERE network IS DISTINCT FROM 'test'`,
  );
  const r = redis();
  const allIds = new Set<string>();
  const seenNetworks = new Set<string>();
  for (const row of res.rows) {
    const network = toNullableString(row.network);
    if (network) seenNetworks.add(network);
  }
  const pipe = r.pipeline();

  pipe.del(TILE_SNAPSHOT_IDS_ALL_KEY);
  for (const network of ['teesside', 'ukmesh', ...seenNetworks]) {
    pipe.del(networkIdsKey(network));
  }
  for (const row of res.rows) {
    const node = normalizeSnapshotNode(row);
    if (!node) continue;
    const serialized = JSON.stringify(node);
    pipe.set(nodeKey(node.node_id), serialized);
    pipe.sadd(TILE_SNAPSHOT_IDS_ALL_KEY, node.node_id);
    allIds.add(node.node_id);
    if (node.network) {
      pipe.sadd(networkIdsKey(node.network), node.node_id);
    }
  }
  pipe.hset(TILE_SNAPSHOT_META_KEY, {
    refreshed_at: new Date().toISOString(),
    node_count: String(allIds.size),
  });

  await pipe.exec();
  return allIds.size;
}

export async function upsertTileSnapshotNode(node: Record<string, unknown>): Promise<void> {
  const nodeId = toNullableString(node['node_id']);
  if (!nodeId) return;

  const r = redis();
  const existing = parseSnapshotNode(await r.get(nodeKey(nodeId)));
  const mergedInput: Partial<SnapshotNode> & { node_id: string; last_seen: string; is_online: boolean } = {
    node_id: nodeId,
    name: toNullableString(node['name']) ?? existing?.name ?? null,
    lat: toNullableNumber(node['lat']) ?? existing?.lat ?? null,
    lon: toNullableNumber(node['lon']) ?? existing?.lon ?? null,
    role: toNullableNumber(node['role']) ?? existing?.role ?? null,
    last_seen: toNullableString(node['last_seen']) ?? existing?.last_seen ?? new Date().toISOString(),
    is_online: node['is_online'] != null ? toBoolean(node['is_online'], true) : (existing?.is_online ?? true),
    network: toNullableString(node['network']) ?? existing?.network ?? null,
  };
  const merged = normalizeSnapshotNode(mergedInput);
  if (!merged) return;

  const pipe = r.pipeline();
  if (existing?.network && existing.network !== merged.network) {
    pipe.srem(networkIdsKey(existing.network), nodeId);
  }
  if (merged.network === 'test') {
    pipe.srem(TILE_SNAPSHOT_IDS_ALL_KEY, nodeId);
    if (existing?.network) pipe.srem(networkIdsKey(existing.network), nodeId);
    pipe.del(nodeKey(nodeId));
  } else {
    pipe.set(nodeKey(nodeId), JSON.stringify(merged));
    pipe.sadd(TILE_SNAPSHOT_IDS_ALL_KEY, nodeId);
    if (merged.network) pipe.sadd(networkIdsKey(merged.network), nodeId);
    pipe.hset(TILE_SNAPSHOT_META_KEY, 'updated_at', new Date().toISOString());
  }
  await pipe.exec();
}
