import type { Router } from 'express';
import type { QueryResultRow } from 'pg';
import { resolvePublicNetworkScope } from '../../http/requestScope.js';
import type { NetworkFilters } from '../../platform/query/networkFilters.js';
import { parseBoundedInteger } from '../utils/input.js';
import { publicJson } from '../../http/publicJson.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(text: string, params?: unknown[]) => Promise<{ rows: T[] }>;

type Deps = {
  query: QueryFn;
  networkFilters: (network?: string, observer?: string) => NetworkFilters;
  limiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  getPublicVisibilityGeneration: () => Promise<number>;
};

export function registerActivityTimelineRoutes(router: Router, deps: Deps): void {
  router.get('/activity/timeline', deps.limiter, async (req, res) => {
    const minutes = parseBoundedInteger(req.query['minutes'], {
      name: 'minutes',
      defaultValue: 360,
      min: 60,
      max: 1_440,
    });
    const bucketMinutes = parseBoundedInteger(req.query['bucket'], {
      name: 'bucket',
      defaultValue: 15,
      min: 5,
      max: 60,
    });
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const filters = deps.networkFilters(network);
      const windowParam = `$${filters.params.length + 1}`;
      const bucketParam = `$${filters.params.length + 2}`;
      await publicJson(res, deps.getPublicVisibilityGeneration, async () => {
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
             FROM mesh.packets p
             WHERE p.time > NOW() - ${windowParam}::interval
               ${filters.packetsAlias('p')}
           ),
           distinct_packets AS (
             SELECT bucket, packet_hash
             FROM scoped
             WHERE packet_hash IS NOT NULL
             GROUP BY bucket, packet_hash
           ),
           packet_buckets AS (
             SELECT bucket, COUNT(*)::text AS packet_count
             FROM distinct_packets
             GROUP BY bucket
           ),
           raw_nodes AS MATERIALIZED (
             SELECT bucket, rx_node_id AS raw_node_id, TRUE AS is_observer,
                    COUNT(*) AS observations
             FROM scoped
             WHERE rx_node_id IS NOT NULL
             GROUP BY bucket, rx_node_id
             UNION ALL
             SELECT bucket, src_node_id AS raw_node_id, FALSE AS is_observer,
                    COUNT(*) AS observations
             FROM scoped
             WHERE src_node_id IS NOT NULL
             GROUP BY bucket, src_node_id
           ),
           canonical_nodes AS MATERIALIZED (
             SELECT bucket, meshcore_canonical_node_id(raw_node_id) AS node_id,
                    is_observer, observations
             FROM raw_nodes
           ),
           node_counts AS (
             SELECT bucket, node_id, SUM(observations) AS observations
             FROM canonical_nodes
             GROUP BY bucket, node_id
           ),
           observer_counts AS (
             SELECT bucket, COUNT(DISTINCT node_id)::text AS observer_count
             FROM canonical_nodes
             WHERE is_observer
             GROUP BY bucket
           )
           SELECT
             pb.bucket::text,
             pb.packet_count,
             COALESCE(oc.observer_count, '0') AS observer_count,
             ARRAY(
               SELECT nc.node_id
               FROM node_counts nc
               WHERE nc.bucket = pb.bucket
               ORDER BY nc.observations DESC, nc.node_id
               LIMIT 250
             ) AS active_node_ids
           FROM packet_buckets pb
           LEFT JOIN observer_counts oc USING (bucket)
           ORDER BY pb.bucket`,
          [...filters.params, `${minutes} minutes`, `${bucketMinutes} minutes`],
        );
        return {
          generatedAt: new Date().toISOString(),
          windowMinutes: minutes,
          bucketMinutes,
          buckets: result.rows.map((row) => ({
            time: row.bucket,
            packetCount: Number(row.packet_count),
            observerCount: Number(row.observer_count),
            activeNodeIds: row.active_node_ids,
          })),
        };
      });
    } catch (err) {
      console.error('[api] GET /activity/timeline', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
