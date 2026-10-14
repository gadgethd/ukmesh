import type { Router } from 'express';
import type { QueryResultRow } from 'pg';
import { resolvePublicNetworkScope } from '../../http/requestScope.js';
import type { NetworkFilters } from '../utils/networkFilters.js';
import { parseBoundedInteger } from '../utils/input.js';
import { queryRfValidation, rfValidationRows } from '../../repositories/networkAnalysis.js';
export { RF_VALIDATION_DB_TIMEOUT_MS } from '../../repositories/networkAnalysis.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(text: string, params?: unknown[], signal?: AbortSignal) => Promise<{ rows: T[] }>;
type Deps = {
  // Kept compatible with the central route registration. RF reads use the
  // dedicated read-only pool below instead of this general-purpose query.
  query: QueryFn;
  networkFilters: (network?: string, observer?: string) => NetworkFilters;
  limiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
};

export const RF_VALIDATION_REQUEST_TIMEOUT_MS = 4_500;

export function registerRfValidationRoutes(router: Router, deps: Deps, rfQuery: QueryFn = queryRfValidation): void {
  router.get('/rf-validation', deps.limiter, async (req, res) => {
    const limit = parseBoundedInteger(req.query['limit'], {
      name: 'limit',
      defaultValue: 100,
      min: 25,
      max: 250,
    });
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(new Error('RF_VALIDATION_TIMEOUT')), RF_VALIDATION_REQUEST_TIMEOUT_MS);
    const onClose = () => {
      if (!res.writableEnded) controller.abort(new Error('RF_VALIDATION_DISCONNECTED'));
    };
    res.once('close', onClose);
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const filters = deps.networkFilters(network);
      const result = await rfValidationRows(rfQuery, filters, limit, controller.signal);
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
        methodology: 'Relay links observed in the last 30 days, ranked by model disagreement and evidence strength. Stored observation counts and ITM results are combined across recently active identity aliases; weak evidence is not a proven model error.',
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
      if (res.destroyed) return;
      console.error('[api] GET /rf-validation', (err as Error).message);
      res.setHeader('Cache-Control', 'no-store');
      if (controller.signal.aborted || (err as { code?: string }).code === '57014'
        || /timeout|timed out/i.test((err as Error).message)) {
        res.setHeader('Retry-After', '30');
        res.status(503).json({ error: 'RF validation is temporarily unavailable. Please retry shortly.' });
      } else {
        res.status(500).json({ error: 'Internal server error' });
      }
    } finally {
      clearTimeout(timeout);
      res.off('close', onClose);
    }
  });
}
