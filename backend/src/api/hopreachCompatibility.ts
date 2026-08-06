import { isIP } from 'node:net';
import { Router, type Request, type Response } from 'express';
import type { QueryResultRow } from 'pg';
import { BoundedTtlMap } from '../cache/boundedTtlMap.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

const MAX_PAGE_SIZE = 500;
const MAX_OFFSET = 100_000;
const MAX_BULK_PUBLIC_KEYS = 5_000;
const MAX_REACH_ROWS = 250_000;

type HopReachNodeRow = {
  public_key: string;
  name: string | null;
  lat: number | null;
  lon: number | null;
  last_heard: Date | string | null;
  first_seen: Date | string | null;
  advert_count: number | null;
};

type HopReachLinkRow = {
  source_id: string;
  pubkey: string;
  name: string | null;
  lat: number | null;
  lon: number | null;
  bottleneck: number;
  bidir: boolean;
};

type CacheStatus = 'HIT' | 'MISS' | 'COALESCED';

async function loadCached<K, V>(
  cache: BoundedTtlMap<K, V>,
  inFlight: Map<K, Promise<V>>,
  key: K,
  load: () => Promise<V>,
): Promise<{ value: V; status: CacheStatus }> {
  const cached = cache.get(key);
  if (cached !== undefined) return { value: cached, status: 'HIT' };
  const pending = inFlight.get(key);
  if (pending) return { value: await pending, status: 'COALESCED' };

  const promise = load()
    .then((value) => {
      cache.set(key, value);
      return value;
    })
    .finally(() => inFlight.delete(key));
  // The adapter is internal-only, but still cap coalescing state so a broken
  // calculator cannot grow it without bound through distinct requests.
  if (inFlight.size < 64) inFlight.set(key, promise);
  return { value: await promise, status: 'MISS' };
}

function combinedCacheStatus(...statuses: CacheStatus[]): CacheStatus {
  if (statuses.every((status) => status === 'HIT')) return 'HIT';
  if (statuses.some((status) => status === 'MISS')) return 'MISS';
  return 'COALESCED';
}

function normalizeIp(value: string | undefined): string {
  const first = String(value ?? '').split(',')[0]?.trim() ?? '';
  return first.startsWith('::ffff:') ? first.slice(7) : first;
}

function isPrivateIp(value: string): boolean {
  const ip = normalizeIp(value);
  if (ip === '127.0.0.1' || ip === '::1') return true;
  if (ip.startsWith('10.') || ip.startsWith('192.168.')) return true;
  if (/^172\.(1[6-9]|2\d|3[0-1])\./.test(ip)) return true;
  if (/^(fc|fd|fe80):/i.test(ip)) return true;
  return isIP(ip) === 0 && ip === 'localhost';
}

function requireInternalHopReach(req: Request, res: Response): boolean {
  // This router is intentionally mounted outside /api and is not proxied by
  // the public Nginx service. Reject forwarded traffic as a second boundary.
  const forwarded = [
    req.headers['cf-connecting-ip'],
    req.headers['x-forwarded-for'],
    req.headers['x-real-ip'],
  ].some((value) => String(value ?? '').trim() !== '');
  if (forwarded || !isPrivateIp(req.socket.remoteAddress ?? '')) {
    res.status(404).json({ error: 'not found' });
    return false;
  }
  return true;
}

function boundedInteger(
  raw: unknown,
  fallback: number,
  min: number,
  max: number,
): number | null {
  if (raw === undefined || raw === null || raw === '') return fallback;
  const value = typeof raw === 'number'
    ? raw
    : typeof raw === 'string' && /^\d+$/.test(raw)
      ? Number(raw)
      : NaN;
  if (!Number.isSafeInteger(value) || value < min || value > max) return null;
  return value;
}

function iso(value: Date | string | null): string | null {
  if (value === null) return null;
  const parsed = value instanceof Date ? value : new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function nodeDto(row: HopReachNodeRow) {
  return {
    public_key: row.public_key,
    name: row.name,
    role: 'repeater',
    lat: row.lat,
    lon: row.lon,
    last_heard: iso(row.last_heard),
    first_seen: iso(row.first_seen),
    advert_count: Number(row.advert_count ?? 0),
    relay_count_1h: null,
    relay_count_24h: null,
    hash_size: null,
    // UK Mesh has no sufficiently reliable region membership field at this
    // boundary, so HopReach's scope controls remain disabled instead of
    // inventing one.
    default_scope: null,
  };
}

function linkDto(row: HopReachLinkRow) {
  return {
    pubkey: row.pubkey,
    name: row.name ?? '',
    lat: row.lat,
    lon: row.lon,
    bottleneck: Number(row.bottleneck ?? 0),
    bidir: Boolean(row.bidir),
  };
}

function validPublicKeys(value: unknown): string[] | null {
  if (!Array.isArray(value) || value.length > MAX_BULK_PUBLIC_KEYS) return null;
  const unique = new Set<string>();
  for (const item of value) {
    if (typeof item !== 'string' || !/^[0-9a-f]{64}$/i.test(item)) return null;
    unique.add(item);
  }
  return [...unique];
}

const NODE_WHERE = `
  n.role = 2
  AND n.lat IS NOT NULL
  AND n.lon IS NOT NULL
  AND n.lat BETWEEN -90 AND 90
  AND n.lon BETWEEN -180 AND 180
  AND NOT (ABS(n.lat) < 1e-9 AND ABS(n.lon) < 1e-9)
  AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
  AND (
    n.network = 'ukmesh'
    OR EXISTS (
      SELECT 1 FROM node_identity_sightings sighting
      WHERE sighting.node_id = n.node_id AND sighting.network = 'ukmesh'
    )
  )`;

async function fetchLinks(query: QueryFn, publicKeys: string[], days: number) {
  if (publicKeys.length === 0) return [] as HopReachLinkRow[];
  const result = await query<HopReachLinkRow>(
    `WITH requested(requested_id, source_id) AS (
       SELECT id, meshcore_canonical_node_id(id)
       FROM unnest($1::text[]) AS ids(id)
     )
     SELECT
       requested.requested_id AS source_id,
       peer.node_id AS pubkey,
       peer.name,
       peer.lat,
       peer.lon,
       CASE
         WHEN nl.count_a_to_b > 0 AND nl.count_b_to_a > 0
           THEN LEAST(nl.count_a_to_b, nl.count_b_to_a)
         ELSE 0
       END::int AS bottleneck,
       (nl.count_a_to_b > 0 AND nl.count_b_to_a > 0) AS bidir
     FROM requested
     JOIN node_identity_links nl
       ON nl.node_a_id = requested.source_id OR nl.node_b_id = requested.source_id
     JOIN node_identity_nodes peer
       ON peer.node_id = CASE
         WHEN nl.node_a_id = requested.source_id THEN nl.node_b_id
         ELSE nl.node_a_id
       END
     WHERE nl.observed_count > 0
       AND nl.last_observed >= NOW() - ($2::int * INTERVAL '1 day')
       AND peer.lat IS NOT NULL
       AND peer.lon IS NOT NULL
       AND (peer.name IS NULL OR peer.name NOT LIKE '%🚫%')
     ORDER BY requested.source_id, peer.node_id
     LIMIT $3`,
    [publicKeys, days, MAX_REACH_ROWS],
  );
  return result.rows;
}

/**
 * Internal CoreScope-shaped adapter consumed only by the HopReach calculator.
 * It intentionally exposes observed node_links, never node_coverage or any
 * predicted viewshed geometry.
 */
export function createHopReachCompatibilityRoutes(query: QueryFn): Router {
  const router = Router();
  const nodeCountCache = new BoundedTtlMap<string, number>({
    name: 'hopreach-node-count', maxEntries: 1, maxWeight: 1_024, ttlMs: 30_000,
  });
  const nodePageCache = new BoundedTtlMap<string, HopReachNodeRow[]>({
    name: 'hopreach-node-pages', maxEntries: 256, maxWeight: 16 * 1024 * 1024, ttlMs: 30_000,
  });
  const reachCache = new BoundedTtlMap<string, HopReachLinkRow[]>({
    name: 'hopreach-observed-links', maxEntries: 64, maxWeight: 32 * 1024 * 1024, ttlMs: 60_000,
  });
  const nodeCountInFlight = new Map<string, Promise<number>>();
  const nodePageInFlight = new Map<string, Promise<HopReachNodeRow[]>>();
  const reachInFlight = new Map<string, Promise<HopReachLinkRow[]>>();
  router.use((req, res, next) => {
    if (requireInternalHopReach(req, res)) next();
  });

  router.get('/api/nodes', async (req, res) => {
    const limit = boundedInteger(req.query['limit'], MAX_PAGE_SIZE, 1, MAX_PAGE_SIZE);
    const offset = boundedInteger(req.query['offset'], 0, 0, MAX_OFFSET);
    if (limit === null || offset === null) {
      res.status(400).json({ error: 'invalid pagination' });
      return;
    }
    try {
      const [count, page] = await Promise.all([
        loadCached(nodeCountCache, nodeCountInFlight, 'all-repeaters', async () => {
          const result = await query<{ total: string }>(
            `SELECT COUNT(*)::text AS total FROM node_identity_nodes n WHERE ${NODE_WHERE}`,
          );
          return Number(result.rows[0]?.total ?? 0);
        }),
        loadCached(nodePageCache, nodePageInFlight, `${limit}:${offset}`, async () => {
          const result = await query<HopReachNodeRow>(
            `SELECT n.node_id AS public_key, n.name, n.lat, n.lon,
                    n.last_seen AS last_heard, n.created_at AS first_seen,
                    n.advert_count
               FROM node_identity_nodes n
              WHERE ${NODE_WHERE}
              ORDER BY n.node_id
              LIMIT $1 OFFSET $2`,
            [limit, offset],
          );
          return result.rows;
        }),
      ]);
      res.setHeader('Cache-Control', 'private, max-age=30');
      res.setHeader('X-HopReach-Cache', combinedCacheStatus(count.status, page.status));
      res.json({
        nodes: page.value.map(nodeDto),
        total: count.value,
      });
    } catch (error) {
      console.error('[hopreach-compat] nodes:', (error as Error).message);
      res.status(500).json({ error: 'internal compatibility query failed' });
    }
  });

  router.post('/api/reach/bulk', async (req, res) => {
    const publicKeys = validPublicKeys((req.body as Record<string, unknown> | undefined)?.['public_keys']);
    const days = boundedInteger((req.body as Record<string, unknown> | undefined)?.['days'], 14, 1, 90);
    if (publicKeys === null || days === null) {
      res.status(400).json({ error: 'public_keys and days are invalid or exceed bounds' });
      return;
    }
    try {
      const cacheKey = `${days}:${[...publicKeys].sort().join(',')}`;
      const rows = await loadCached(
        reachCache,
        reachInFlight,
        cacheKey,
        () => fetchLinks(query, publicKeys, days),
      );
      const linksByPublicKey: Record<string, ReturnType<typeof linkDto>[]> = Object.fromEntries(
        publicKeys.map((publicKey) => [publicKey, []]),
      );
      for (const row of rows.value) linksByPublicKey[row.source_id]?.push(linkDto(row));
      res.setHeader('Cache-Control', 'private, max-age=60');
      res.setHeader('X-HopReach-Cache', rows.status);
      res.json({ links_by_public_key: linksByPublicKey });
    } catch (error) {
      console.error('[hopreach-compat] bulk reach:', (error as Error).message);
      res.status(500).json({ error: 'internal compatibility query failed' });
    }
  });

  router.get('/api/nodes/:pubkey/reach', async (req, res) => {
    const publicKey = String(req.params['pubkey'] ?? '');
    const days = boundedInteger(req.query['days'], 14, 1, 90);
    if (!/^[0-9a-f]{64}$/i.test(publicKey) || days === null) {
      res.status(400).json({ error: 'invalid public key or days' });
      return;
    }
    try {
      const rows = await loadCached(
        reachCache,
        reachInFlight,
        `${days}:${publicKey}`,
        () => fetchLinks(query, [publicKey], days),
      );
      res.setHeader('Cache-Control', 'private, max-age=60');
      res.setHeader('X-HopReach-Cache', rows.status);
      res.json({ links: rows.value.map(linkDto) });
    } catch (error) {
      console.error('[hopreach-compat] node reach:', (error as Error).message);
      res.status(500).json({ error: 'internal compatibility query failed' });
    }
  });

  // Scope inference remains deliberately unavailable until UK Mesh has a
  // reliable source of region membership. These compatibility responses keep
  // an accidentally-enabled upstream client bounded and empty.
  router.get('/api/scope-stats', (_req, res) => res.json({ byRegion: [] }));
  router.get('/api/packets', (_req, res) => res.json({ packets: [], total: 0 }));
  router.get('/healthz', (_req, res) => res.json({ status: 'ok' }));
  return router;
}
