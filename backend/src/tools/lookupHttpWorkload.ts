import { createHash } from 'node:crypto';
import { constants } from 'node:fs';
import { open, writeFile } from 'node:fs/promises';
import express, { Router } from 'express';
import { withLookupTrial, readLookupSessionIdentity } from './lookupBenchmarkSession.js';
import type { LookupRuntime, LookupSourceLoader, LookupCacheModule, LookupSessionIdentity } from './lookupBenchmarkSession.js';
import type { LookupHttpCase } from './lookupHttpRequests.js';

type Caches = ReturnType<LookupCacheModule['createApiCaches']>;
type ObserverReceipt = { file: string; reuse: boolean };

async function lookupObserver(query: LookupRuntime['pool']['query'], session: LookupSessionIdentity,
  receipt?: ObserverReceipt) {
  let body: string;
  if (receipt?.reuse) {
    const file = await open(receipt.file, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
      const stat = await file.stat();
      if (!stat.isFile() || stat.size > 8192 || (stat.mode & 0o077) !== 0) {
        throw new Error('invalid private observer receipt');
      }
      body = await file.readFile('utf8');
    } finally { await file.close(); }
  } else {
    const observer = (await query(`SELECT p.rx_node_id FROM mesh.packets p
      WHERE p.visibility_ok IS TRUE AND p.is_private IS NOT TRUE AND p.rx_node_id IS NOT NULL
      AND p.network <> 'test' AND p.time > NOW() - INTERVAL '7 days'
      GROUP BY p.rx_node_id ORDER BY count(*) DESC, p.rx_node_id LIMIT 1`)).rows[0]?.rx_node_id;
    if (typeof observer !== 'string' || !observer) throw new Error('public observer required');
    body = JSON.stringify({ version: 1, database: session.database, user: session.current_user, observer });
    if (receipt) await writeFile(receipt.file, body, { flag: 'wx', mode: 0o600 });
  }
  const value = JSON.parse(body) as { version?: unknown; database?: unknown; user?: unknown; observer?: unknown } | null;
  if (value?.version !== 1 || value.database !== session.database || value.user !== session.current_user
    || typeof value.observer !== 'string' || !value.observer) {
    throw new Error('observer receipt differs from the authenticated lookup fixture');
  }
  return { observer: value.observer, observerWorkset: {
    sha256: createHash('sha256').update(body).digest('hex'), reused: receipt?.reuse === true,
  } };
}

/** Selected-source routes, fixture identities and public cases have one owner. */
export async function loadLookupHttpWorkload(load: LookupSourceLoader, runtime: LookupRuntime) {
  const { createNodeRepository } = await load('repositories/nodes.ts');
  const nodeReads = await load('repositories/nodeReads.ts');
  const packetReads = await load('repositories/packetReads.ts');
  const { registerNodeRoutes } = await load('api/routes/nodes.ts');
  const { registerMiscRoutes } = await load('api/routes/misc.ts');
  const { wrapAsyncHandlers, apiErrorMiddleware } = await load('api/errors.ts');
  const { networkFilters } = await load('platform/query/networkFilters.ts');
  const { readPublicResponseGeneration } = await load('privacy/responseGeneration.ts');
  const { createGlobalApiLimiter, NODES_LIMITER, PACKET_DETAIL_LIMITER } = await load('api/bootstrap/limiters.ts');
  const cacheModule = await load('api/bootstrap/caches.ts') as LookupCacheModule;
  const query = runtime.pool.query.bind(runtime.pool);
  return {
    async prepare(snapshotOverride?: string, observerReceipt?: ObserverReceipt) {
    const databaseSetup = { http: await readLookupSessionIdentity(runtime.query, 'http') };
    const counts = (await query(`SELECT (SELECT count(*)::text FROM core.nodes) nodes,
      (SELECT count(*)::text FROM mesh.packets) observations,
      (SELECT count(DISTINCT packet_hash)::text FROM mesh.packets) packets,
      (SELECT count(*)::text FROM core.node_links) links`)).rows[0];
    if (Number(counts.nodes) < 3 || Number(counts.observations) < 20) throw new Error('populated migrated fixture required');
    const snapshot = snapshotOverride ?? new Date().toISOString();
    const visible = await createNodeRepository(runtime.query).listPublicMapRows(
      ['node_id'], networkFilters('ukmesh'), snapshot, null, 1000);
    const identity = (await query(`SELECT n.node_id, n.name FROM core.node_identity_nodes n
      WHERE n.name IS NOT NULL AND n.name NOT LIKE '%🚫%' AND n.network <> 'test'
      AND n.role = 2 AND n.lat BETWEEN -90 AND 90 AND n.lon BETWEEN -180 AND 180
      AND EXISTS (SELECT 1 FROM core.node_identity_links l WHERE l.node_a_id=n.node_id OR l.node_b_id=n.node_id)
      AND n.node_id = ANY($1::text[])
      ORDER BY n.advert_count DESC, n.node_id LIMIT 1`, [visible.map((node: {node_id: string}) => node.node_id)])).rows[0];
    if (!identity) throw new Error('public linked identity required');
    const packet = (await query(`SELECT packet_hash FROM mesh.packets WHERE visibility_ok IS TRUE
      AND is_private IS NOT TRUE ORDER BY time DESC, packet_hash LIMIT 1`)).rows[0];
    if (!packet) throw new Error('public packet required');
    const { observer, observerWorkset } = await lookupObserver(query, databaseSetup.http, observerReceipt);
    const map = `/api/nodes/map?network=ukmesh&snapshot=${encodeURIComponent(snapshot)}&limit=1000`;
    const cases: LookupHttpCase[] = [
      { name: 'map-page', route: map, populated: true },
      { name: 'search-name', route: map + '&q=' + encodeURIComponent(identity.name), populated: true },
      { name: 'search-id', route: map + '&q=' + identity.node_id.slice(0, 12), populated: true },
      { name: 'search-short', route: map + '&q=a' },
      { name: 'search-missing', route: map + '&q=zz_lookup_missing_zz' },
      { name: 'map-observer', route: map + '&observer=' + observer },
      { name: 'node-links', route: `/api/nodes/${identity.node_id}/links?network=ukmesh`, populated: true },
      { name: 'node-history', route: `/api/nodes/${observer}/history?network=ukmesh&hours=672`, populated: true },
      { name: 'packet-detail', route: `/api/packets/${packet.packet_hash}?network=ukmesh` },
      { name: 'packet-missing', route: `/api/packets/${'F'.repeat(64)}?network=ukmesh`, expected: 404 },
    ];
    const region = (await query(`SELECT region FROM core.region_scope_members
      GROUP BY region ORDER BY count(*) DESC,region LIMIT 1`)).rows[0]?.region;
    const regionEnabled = ['1','true','yes','on'].includes(String(process.env['PUBLIC_FEATURE_REGION_SCOPING_ENABLED']??'').toLowerCase());
    if (region && regionEnabled) cases.push({ name: 'map-region', route: map + '&region=' + encodeURIComponent(region), populated: true });
    if (visible.length > 1000) {
      const cursor = Buffer.from(JSON.stringify({ version: 1, nodeId: visible[999].node_id })).toString('base64url');
      cases.push({ name: 'map-next-page', route: map + '&cursor=' + cursor, populated: true });
    }
      return { counts, snapshot, cases, regionEnabled, databaseSetup, observerWorkset };
    },
    withTrial<T>(work: (port: number, caches: Caches) => Promise<T>): Promise<T> {
      return withLookupTrial(cacheModule, async ({ caches, ownRoutes, listen }) => {
        const router = Router();
        const { nodeLinksCache, nodeLinksInflight, inferredNodesCache, inferredNodesInflight } = caches;
        const closeNodes = registerNodeRoutes(router, {
          ...nodeReads, nodeRepository: createNodeRepository(runtime.query), networkFilters,
          getPublicVisibilityGeneration: () => readPublicResponseGeneration(runtime.query),
          requireLocalOnly: () => false, nodeLinksCache, nodeLinksInflight, nodeLinksCacheTtlMs: cacheModule.NODE_LINKS_CACHE_TTL_MS,
          inferredNodesCache, inferredNodesInflight, inferredNodesCacheTtlMs: cacheModule.INFERRED_NODES_CACHE_TTL_MS,
          nodesLimiter: NODES_LIMITER,
        });
        ownRoutes(closeNodes);
        registerMiscRoutes(router, { query: runtime.query, ...packetReads,
          getPublicVisibilityGeneration: () => readPublicResponseGeneration(runtime.query), packetDetailLimiter: PACKET_DETAIL_LIMITER });
        wrapAsyncHandlers(router);
        const app = express(); app.use('/api', createGlobalApiLimiter(), router); app.use(apiErrorMiddleware);
        const port = await listen(app);
        return work(port, caches);
      });
    },
  };
}
