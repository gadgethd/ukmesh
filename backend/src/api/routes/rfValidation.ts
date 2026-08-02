import type { Router } from 'express';
import type { QueryResultRow } from 'pg';
import { resolvePublicNetworkScope } from '../../http/requestScope.js';
import type { NetworkFilters } from '../utils/networkFilters.js';
import { parseBoundedInteger } from '../utils/input.js';
import { rfValidationRows } from '../../repositories/networkAnalysis.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(text: string, params?: unknown[]) => Promise<{ rows: T[] }>;
type Deps = {
  query: QueryFn;
  networkFilters: (network?: string, observer?: string) => NetworkFilters;
  limiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
};

export function registerRfValidationRoutes(router: Router, deps: Deps): void {
  router.get('/rf-validation', deps.limiter, async (req, res) => {
    const limit = parseBoundedInteger(req.query['limit'], {
      name: 'limit',
      defaultValue: 100,
      min: 25,
      max: 250,
    });
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const filters = deps.networkFilters(network);
      const result = await rfValidationRows(deps.query, filters, limit);
      const links = result.rows.map((row) => ({
        source: row.node_a_id,
        target: row.node_b_id,
        sourceName: row.name_a,
        targetName: row.name_b,
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
