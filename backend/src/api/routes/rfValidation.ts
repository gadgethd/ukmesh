import type { Router } from 'express';
import type { QueryResultRow } from 'pg';
import { resolveRequestNetwork } from '../../http/requestScope.js';
import type { NetworkFilters } from '../utils/networkFilters.js';
import { isPrivateNode } from '../utils/privateNode.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(text: string, params?: unknown[]) => Promise<{ rows: T[] }>;
type Deps = {
  query: QueryFn;
  networkFilters: (network?: string, observer?: string) => NetworkFilters;
  limiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
};

export function registerRfValidationRoutes(router: Router, deps: Deps): void {
  router.get('/rf-validation', deps.limiter, async (req, res) => {
    try {
      const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers, 'ukmesh');
      const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
      const requestedLimit = Number(req.query['limit'] ?? 100);
      const limit = Number.isFinite(requestedLimit) ? Math.min(250, Math.max(25, Math.round(requestedLimit))) : 100;
      const filters = deps.networkFilters(network);
      const limitParam = `$${filters.params.length + 1}`;
      const result = await deps.query<{
        node_a_id: string; node_b_id: string; name_a: string | null; name_b: string | null;
        observed_count: string; multibyte_observed_count: string; itm_path_loss_db: number | null;
        itm_viable: boolean | null; force_viable: boolean; last_observed: string; classification: string;
      }>(
        `SELECT
           nl.node_a_id, nl.node_b_id, a.name AS name_a, b.name AS name_b,
           nl.observed_count, nl.multibyte_observed_count, nl.itm_path_loss_db,
           nl.itm_viable, nl.force_viable, nl.last_observed::text,
           CASE
             WHEN nl.force_viable THEN 'operator_override'
             WHEN nl.itm_viable = false AND nl.multibyte_observed_count > 0 THEN 'observed_unexpected'
             WHEN nl.itm_viable = false AND nl.observed_count >= 10 THEN 'observed_unexpected'
             WHEN nl.itm_viable = true AND nl.observed_count <= 2 AND nl.last_observed < NOW() - INTERVAL '7 days' THEN 'weak_model_evidence'
             ELSE 'match'
           END AS classification
         FROM node_links nl
         JOIN nodes a ON a.node_id = nl.node_a_id
         JOIN nodes b ON b.node_id = nl.node_b_id
         WHERE nl.last_observed > NOW() - INTERVAL '30 days'
           AND (a.role IS NULL OR a.role = 2)
           AND (b.role IS NULL OR b.role = 2)
           ${filters.nodesAlias('a')}
           ${filters.nodesAlias('b')}
         ORDER BY
           CASE
             WHEN nl.itm_viable = false AND nl.multibyte_observed_count > 0 THEN 0
             WHEN nl.force_viable THEN 1
             WHEN nl.itm_viable = false AND nl.observed_count >= 10 THEN 2
             WHEN nl.itm_viable = true AND nl.observed_count <= 2 THEN 3
             ELSE 4
           END,
           nl.multibyte_observed_count DESC,
           nl.observed_count DESC
         LIMIT ${limitParam}`,
        [...filters.params, limit],
      );
      const links = result.rows.map((row) => ({
        source: row.node_a_id,
        target: row.node_b_id,
        sourceName: isPrivateNode(row.name_a) ? 'Private Node' : row.name_a,
        targetName: isPrivateNode(row.name_b) ? 'Private Node' : row.name_b,
        observations: Number(row.observed_count),
        strongObservations: Number(row.multibyte_observed_count),
        pathLossDb: row.itm_path_loss_db,
        modelViable: row.itm_viable,
        operatorOverride: row.force_viable,
        lastObserved: row.last_observed,
        classification: row.classification,
      }));
      const mismatches = links.filter((link) => link.classification !== 'match');
      res.setHeader('Cache-Control', 'public, max-age=300, stale-while-revalidate=600');
      res.json({
        generatedAt: new Date().toISOString(),
        windowDays: 30,
        methodology: 'Observed relay evidence compared with the stored ITM viability result; weak evidence is not treated as a proven model error.',
        summary: {
          evaluated: links.length,
          matches: links.length - mismatches.length,
          mismatches: mismatches.length,
          observedUnexpected: mismatches.filter((link) => link.classification === 'observed_unexpected').length,
          operatorOverrides: mismatches.filter((link) => link.classification === 'operator_override').length,
          weakModelEvidence: mismatches.filter((link) => link.classification === 'weak_model_evidence').length,
        },
        mismatches,
      });
    } catch (err) {
      console.error('[api] GET /rf-validation', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
