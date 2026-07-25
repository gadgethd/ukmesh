import type { Router } from 'express';
import type { QueryResultRow } from 'pg';
import { resolveRequestNetwork } from '../../http/requestScope.js';
import type { NetworkFilters } from '../utils/networkFilters.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(text: string, params?: unknown[]) => Promise<{ rows: T[] }>;

type Deps = {
  query: QueryFn;
  networkFilters: (network?: string, observer?: string) => NetworkFilters;
  limiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
};

export function registerActivityTimelineRoutes(router: Router, deps: Deps): void {
  router.get('/activity/timeline', deps.limiter, async (req, res) => {
    try {
      const requestedMinutes = Number(req.query['minutes'] ?? 360);
      const requestedBucketMinutes = Number(req.query['bucket'] ?? 15);
      const minutes = Number.isFinite(requestedMinutes) ? Math.min(1_440, Math.max(60, Math.round(requestedMinutes))) : 360;
      const bucketMinutes = Number.isFinite(requestedBucketMinutes) ? Math.min(60, Math.max(5, Math.round(requestedBucketMinutes))) : 15;
      const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers, 'ukmesh');
      const network = requestedNetwork === 'test' ? 'test' : 'ukmesh';
      const filters = deps.networkFilters(network);
      const windowParam = `$${filters.params.length + 1}`;
      const bucketParam = `$${filters.params.length + 2}`;
      const result = await deps.query<{
        bucket: string;
        packet_count: string;
        observer_count: string;
        active_node_ids: string[];
      }>(
        `WITH scoped AS MATERIALIZED (
           SELECT
             date_bin(${bucketParam}::interval, p.time, TIMESTAMPTZ '2000-01-01') AS bucket,
             p.packet_hash,
             p.rx_node_id,
             p.src_node_id
           FROM packets p
           WHERE p.time > NOW() - ${windowParam}::interval
             ${filters.packetsAlias('p')}
             AND (
               COALESCE(cardinality(p.path_hashes), 0) = 0
               OR p.path_hash_size_bytes BETWEEN 1 AND 3
             )
             AND NOT EXISTS (
               SELECT 1
               FROM unnest(COALESCE(p.path_hashes, ARRAY[]::text[])) AS malformed_path_hash
               WHERE malformed_path_hash IS NULL
                  OR length(malformed_path_hash) <> p.path_hash_size_bytes * 2
                  OR malformed_path_hash !~ '^[0-9A-Fa-f]+$'
             )
             AND NOT EXISTS (
               SELECT 1
               FROM nodes private_node
               WHERE private_node.name LIKE '%🚫%'
                 AND (
                   private_node.node_id IN (p.rx_node_id, p.src_node_id)
                   OR EXISTS (
                     SELECT 1
                     FROM unnest(COALESCE(p.path_hashes, ARRAY[]::text[])) AS path_hash
                     WHERE p.path_hash_size_bytes BETWEEN 1 AND 3
                       AND length(path_hash) = p.path_hash_size_bytes * 2
                       AND UPPER(private_node.node_id) LIKE UPPER(path_hash) || '%'
                   )
                 )
             )
         ),
         packet_buckets AS (
           SELECT bucket,
                  COUNT(DISTINCT packet_hash)::text AS packet_count,
                  COUNT(DISTINCT rx_node_id)::text AS observer_count
           FROM scoped
           GROUP BY bucket
         ),
         node_counts AS (
           SELECT bucket, node_id, COUNT(*) AS observations
           FROM (
             SELECT bucket, rx_node_id AS node_id FROM scoped WHERE rx_node_id IS NOT NULL
             UNION ALL
             SELECT bucket, src_node_id AS node_id FROM scoped WHERE src_node_id IS NOT NULL
           ) events
           GROUP BY bucket, node_id
         )
         SELECT
           pb.bucket::text,
           pb.packet_count,
           pb.observer_count,
           ARRAY(
             SELECT nc.node_id
             FROM node_counts nc
             WHERE nc.bucket = pb.bucket
             ORDER BY nc.observations DESC, nc.node_id
             LIMIT 250
           ) AS active_node_ids
         FROM packet_buckets pb
         ORDER BY pb.bucket`,
        [...filters.params, `${minutes} minutes`, `${bucketMinutes} minutes`],
      );
      res.setHeader('Cache-Control', 'public, max-age=30, stale-while-revalidate=60');
      res.json({
        generatedAt: new Date().toISOString(),
        windowMinutes: minutes,
        bucketMinutes,
        buckets: result.rows.map((row) => ({
          time: row.bucket,
          packetCount: Number(row.packet_count),
          observerCount: Number(row.observer_count),
          activeNodeIds: row.active_node_ids,
        })),
      });
    } catch (err) {
      console.error('[api] GET /activity/timeline', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
